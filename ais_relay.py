import asyncio, json, time, datetime as dt
import websockets
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

AIS_KEY   = "027d88e7b72d86e556da33c72176a77e98683a39"
# [[lat, lon], [lat, lon]] corners; this box roughly covers the Philippines
BBOX      = [[[4.0, 116.0], [22.0, 128.0]]]
LAYER_URL = "https://services3.arcgis.com/RL4FFq0uAkGR2EsJ/arcgis/rest/services/AIS_stream/FeatureServer"
LISTEN_SECONDS = 60

async def collect():
    latest = {}
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        # subscription must be sent within 3 seconds of connecting
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
            md, pr = m["MetaData"], m["Message"]["PositionReport"]
            latest[str(md["MMSI"])] = {
                "geometry": {"x": md["longitude"], "y": md["latitude"],
                             "spatialReference": {"wkid": 4326}},
                "attributes": {
                    "MMSI": str(md["MMSI"]),
                    "ShipName": (md.get("ShipName") or "").strip(),
                    "SOG": pr.get("Sog"), "COG": pr.get("Cog"),
                    "Heading": pr.get("TrueHeading"),
                    "NavStatus": pr.get("NavigationalStatus"),
                    "LastUpdate": int(time.time() * 1000),
                },
            }
    return latest

def upsert(latest):
    gis = GIS("https://www.arcgis.com", "mcalamlam_RS", "geodata0617")  # or use the notebook's active GIS
    lyr = FeatureLayer(LAYER_URL, gis)
    existing = lyr.query(where="1=1", out_fields="OBJECTID,MMSI", return_geometry=False)
    oid_by_mmsi = {f.attributes["MMSI"]: f.attributes["OBJECTID"] for f in existing.features}

    adds, updates = [], []
    for mmsi, f in latest.items():
        if mmsi in oid_by_mmsi:
            f["attributes"]["OBJECTID"] = oid_by_mmsi[mmsi]
            updates.append(f)
        else:
            adds.append(f)
    if adds or updates:
        lyr.edit_features(adds=adds, updates=updates)

    # remove vessels not seen for 2 hours
    cutoff = (dt.datetime.utcnow() - dt.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    lyr.delete_features(where=f"LastUpdate < TIMESTAMP '{cutoff}'")

upsert(asyncio.run(collect()))
