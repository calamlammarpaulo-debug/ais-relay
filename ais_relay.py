import asyncio
import datetime as dt
import json
import os
import time

import websockets
from arcgis.features import FeatureLayer
from arcgis.gis import GIS

# --- Settings (secrets come from GitHub Actions) ---
AIS_KEY = os.environ["AIS_KEY"]
LAYER_URL = os.environ["LAYER_URL"]  # must end in /FeatureServer/0
AGOL_USER = os.environ["AGOL_USER"]
AGOL_PASS = os.environ["AGOL_PASS"]

# [[lat, lon], [lat, lon]] corners; this box roughly covers the Philippines
BBOX = [[[4.0, 116.0], [22.0, 128.0]]]
LISTEN_SECONDS = 30
STALE_HOURS = 2


async def collect():
    """Listen to AISstream and keep the latest position per vessel."""
    latest = {}
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        # The subscription must be sent within 3 seconds of connecting
        await ws.send(json.dumps({
            "APIKey": AIS_KEY,
            "BoundingBoxes": BBOX,
            "FilterMessageTypes": ["PositionReport"],
        }))

        end = time.time() + LISTEN_SECONDS
        while time.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=end - time.time())
            except asyncio.TimeoutError:
                break

            m = json.loads(raw)

            # Skip anything that isn't a position report; stop on errors
            if "MetaData" not in m or "PositionReport" not in m.get("Message", {}):
                print("Unexpected message from AISstream:", m)
                if "error" in m:
                    break
                continue

            md = m["MetaData"]
            pr = m["Message"]["PositionReport"]
            mmsi = str(md["MMSI"])

            latest[mmsi] = {
                "geometry": {
                    "x": md["longitude"],
                    "y": md["latitude"],
                    "spatialReference": {"wkid": 4326},
                },
                "attributes": {
                    "MMSI": mmsi,
                    "ShipName": (md.get("ShipName") or "").strip(),
                    "SOG": pr.get("Sog"),
                    "COG": pr.get("Cog"),
                    "Heading": pr.get("TrueHeading"),
                    "NavStatus": pr.get("NavigationalStatus"),
                    "LastUpdate": int(time.time() * 1000),
                },
            }

    print(f"Collected {len(latest)} vessels")
    return latest


def upsert(latest):
    """Add new vessels, update existing ones, remove stale ones."""
    gis = GIS("https://www.arcgis.com", AGOL_USER, AGOL_PASS)
    lyr = FeatureLayer(LAYER_URL, gis)

    if latest:
        existing = lyr.query(where="1=1", out_fields="OBJECTID,MMSI", return_geometry=False)
        oid_by_mmsi = {f.attributes["MMSI"]: f.attributes["OBJECTID"] for f in existing.features}

        adds, updates = [], []
        for mmsi, feature in latest.items():
            if mmsi in oid_by_mmsi:
                feature["attributes"]["OBJECTID"] = oid_by_mmsi[mmsi]
                updates.append(feature)
            else:
                adds.append(feature)

        result = lyr.edit_features(adds=adds, updates=updates)
        print(f"Added {len(adds)}, updated {len(updates)}")

        # Print any per-feature failures so they show up in the log
        for kind in ("addResults", "updateResults"):
            for r in result.get(kind, []):
                if not r.get("success"):
                    print("Edit failed:", r)

    # Remove vessels not seen for STALE_HOURS (hosted layers store UTC)
    cutoff = (dt.datetime.utcnow() - dt.timedelta(hours=STALE_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    lyr.delete_features(where=f"LastUpdate < TIMESTAMP '{cutoff}'")


if __name__ == "__main__":
    upsert(asyncio.run(collect()))
