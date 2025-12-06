import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from redis_client import redis_conn
import json
from model import RideRequest
from fastapi import APIRouter
from fastapi.middleware.cors import CORSMiddleware

router = APIRouter()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],           # GET, POST, OPTIONS, etc.
    allow_headers=["*"],         # allow all origins
)

driver_connections = {}
passenger_connections = {}


# ---- Helpers ----
def decode_val(v):
    return v.decode() if isinstance(v, bytes) else v

def decode_dict(d):
    return {decode_val(k): decode_val(v) for k, v in d.items()}

async def safe_send(ws, payload):
    """Send JSON on a websocket but guard against exceptions so one bad client
    does not kill the whole server."""
    try:
        await ws.send_json(payload)
    except Exception as exc:
        # Log and ignore
        print("⚠️ Failed to send to websocket:", exc)

# ------------------------
# DRIVER WEBSOCKET
# ------------------------
@app.websocket("/ws/driver/{driver_id}")
async def driver_ws(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    driver_connections[driver_id] = websocket

    redis_conn.sadd("available_drivers", driver_id)
    print(f"Driver {driver_id} connected")

    # Check ongoing ride
    ride_key = f"ride:driver:{driver_id}"
    ride_data = redis_conn.hgetall(ride_key)
    print("Raw ride data from Redis:", ride_data) 
    if ride_data:
        # Decode redis bytes safely
        ride = decode_dict(ride_data)

        # Extract fields
        passenger_id = ride.get("passenger_id")
        pickup_lat = ride.get("pickup_lat")
        pickup_lon = ride.get("pickup_lon")
        status = ride.get("status", "assigned")

        # Validate mandatory fields
        if passenger_id and pickup_lat is not None and pickup_lon is not None:
            try:
                await websocket.send_json({
                    "type": "ongoing_ride",
                    "passenger_id": passenger_id,
                    "pickup_lat": float(pickup_lat),
                    "pickup_lon": float(pickup_lon),
                    "status": status
                })
                print(f"Restored ride for driver {driver_id}")

            except ValueError:
                print(f"⚠️ Invalid lat/lon stored for driver {driver_id}: {pickup_lat}, {pickup_lon}")
                # Optional: cleanup bad redis data
                # redis_conn.delete(ride_key)

        else:
            print(f"⚠️ Missing ride fields for driver {driver_id}: {ride}")
            # Inform driver of partial/incomplete ride
            await safe_send(websocket, {"type": "ride_error", "message": "Incomplete ride data."})

    try:
        while True:
            data = await websocket.receive_json()
            # ----- CHECK FOR RIDE ACCEPT FIRST -----
            if data.get("type") == "accept_ride":
                request_id = data.get("request_id")
                print(f"Driver {driver_id} ACCEPTED ride {request_id}")
                await handle_driver_accept(driver_id, request_id)
                continue  # important
            if data.get("type") == "completed_ride":
                request_id = data.get("request_id")
                ride_key = f"ride_request:{request_id}"

                ride_data = redis_conn.hgetall(ride_key)
                if not ride_data:
                    await websocket.send_json({"type": "error", "message": "Ride not found"})
                    continue

                # Mark ride completed
                redis_conn.hset(ride_key, "status", "completed")
                print(f"Ride {request_id} completed by driver {driver_id}")

                passenger_id = ride_data.get("passenger_id")

                # Notify passenger
                if passenger_id in passenger_connections:
                    await passenger_connections[passenger_id].send_json({
                        "type": "ride_completed",
                        "request_id": request_id
                    })

                # Add driver back to available list
                redis_conn.sadd("available_drivers", driver_id)
                redis_conn.delete(ride_key)
                redis_conn.delete(f"ride:driver:{driver_id}")
                redis_conn.delete(f"ride:passenger:{passenger_id}")

                # Confirm to driver
                await websocket.send_json({
                    "type": "ride_completed_ack",
                    "request_id": request_id
                })

                continue
            lon = data.get("lon")
            lat = data.get("lat")

            if lon is None or lat is None:
                print("❌ Missing lat/lon:", data)
                continue

            try:
                lon = float(lon)
                lat = float(lat)
            except:
                print("❌ Invalid float:", data)
                continue

            # GEO position update
            redis_conn.geoadd("drivers_geo", [lon, lat, driver_id])

            # Update driver info
            redis_conn.hset(
                f"driver:{driver_id}",
                mapping={"lat": lat, "lon": lon, "status": data.get("status", "available")}
            )

    except WebSocketDisconnect:
        print(f"Driver {driver_id} disconnected")
        try:
            redis_conn.srem("available_drivers", driver_id)
        except Exception:
            pass
        driver_connections.pop(driver_id, None)


# ------------------------
# PASSENGER WEBSOCKET
# ------------------------
@app.websocket("/ws/passenger/{passenger_id}")
async def passenger_ws(websocket: WebSocket, passenger_id: str):
    await websocket.accept()
    passenger_connections[passenger_id] = websocket
    print(f"Passenger {passenger_id} connected")

    ride_key = f"ride:passenger:{passenger_id}"
    ride_data = redis_conn.hgetall(ride_key)

    if ride_data:
        ride = decode_dict(ride_data)
        driver_id = ride.get("driver_id")
        pickup_lat = ride.get("pickup_lat")
        pickup_lon = ride.get("pickup_lon")
        status = ride.get("status", "assigned")
                # Only send if sensible
        if driver_id and pickup_lat is not None and pickup_lon is not None:
            try:
                await websocket.send_json({
                    "type": "ongoing_ride",
                    "driver_id": driver_id,
                    "pickup_lat": float(pickup_lat),
                    "pickup_lon": float(pickup_lon),
                    "status": status
                })
            except ValueError:
                print("⚠️ Invalid passenger ride lat/lon:", ride)
    try:
        while True:
            msg = await websocket.receive_text()
            print(f"Passenger {passenger_id}: {msg}")

    except WebSocketDisconnect:
        print(f"Passenger {passenger_id} disconnected")
        passenger_connections.pop(passenger_id, None)


# ------------------------
# PASSENGER REQUESTS A RIDE
# ------------------------
@app.post("/request_ride")
async def request_ride(data: RideRequest):

    passenger_id = data.passenger_id
    lat, lon = data.lat, data.lon

    import uuid
    request_id = str(uuid.uuid4())

    redis_conn.hset(
        f"passenger:{passenger_id}",
        mapping={"lat": lat, "lon": lon, "timestamp": time.time()}
    )

    # Nearest drivers
    nearby = redis_conn.georadius("drivers_geo", lon, lat, 10, unit="km", withdist=True)

    decoded = []
    for raw_id, dist in nearby:
        d_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
        decoded.append((d_id, dist))
        print("Driver ->", d_id, "Distance ->", dist)

    if not decoded:
        return {"status": "no_drivers_available"}

    # Store ride request
    redis_conn.hset(
        f"ride_request:{request_id}",
        mapping={"passenger_id": passenger_id, "pickup_lat": lat, "pickup_lon": lon, "status": "pending"}
    )

    # Broadcast to drivers
    for driver_id, dist in decoded:
        if driver_id in driver_connections:
            await driver_connections[driver_id].send_json({
                "type": "ride_request",
                "request_id": request_id,
                "passenger_id": passenger_id,
                "pickup_lat": lat,
                "pickup_lon": lon
            })

    return {"status": "request_sent", "request_id": request_id}


# ------------------------
# DRIVER ACCEPTS REQUEST
# ------------------------
async def handle_driver_accept(driver_id, request_id):
    ride_key = f"ride_request:{request_id}"
    ride_data = redis_conn.hgetall(ride_key)
    print("Raw ride_request data:", ride_data)

    if not ride_data:
        ws = driver_connections.get(driver_id)
        if ws:
            await safe_send(ws, {"type": "ride_taken"})
        return

    ride = decode_dict(ride_data)

    # Attempt atomic assign
    lua = """
    local k = KEYS[1]
    local expected = ARGV[1]
    local driver = ARGV[2]
    local cur = redis.call('HGET', k, 'status')
    if not cur then return -1 end
    if cur ~= expected then return 0 end
    redis.call('HSET', k, 'status', 'assigned')
    redis.call('HSET', k, 'driver_id', driver)
    return 1
    """
    res = redis_conn.eval(lua, 1, ride_key, "pending", driver_id)

    if res == -1:
        ws = driver_connections.get(driver_id)
        if ws:
            await safe_send(ws, {"type": "ride_taken"})
        return

    if res == 0:
        ws = driver_connections.get(driver_id)
        if ws:
            await safe_send(ws, {"type": "ride_taken"})
        return

    # res == 1 -> success, proceed
    passenger_id = ride.get("passenger_id")
    try:
        pickup_lat = float(ride.get("pickup_lat"))
        pickup_lon = float(ride.get("pickup_lon"))
    except Exception:
        print("⚠️ Invalid pickup coords in ride_request:", ride)
        ws = driver_connections.get(driver_id)
        if ws:
            await safe_send(ws, {"type": "ride_error", "message": "Invalid pickup coordinates."})
        return

    # Save driver-specific ongoing ride (for reconnect)
    redis_conn.hset(f"ride:driver:{driver_id}", mapping={
        "request_id": request_id,
        "passenger_id": passenger_id,
        "pickup_lat": pickup_lat,
        "pickup_lon": pickup_lon,
        "status": "assigned"
    })
    redis_conn.expire(f"ride:driver:{driver_id}", 3600)

    # Save passenger side pointer
    redis_conn.hset(f"ride:passenger:{passenger_id}", mapping={
        "request_id": request_id,
        "driver_id": driver_id,
        "pickup_lat": pickup_lat,
        "pickup_lon": pickup_lon,
        "status": "assigned"
    })
    redis_conn.expire(f"ride:passenger:{passenger_id}", 3600)

    # Optionally delete the ride_request key (since driver assigned)
    # redis_conn.delete(ride_key)

    # Notify passenger
    pws = passenger_connections.get(passenger_id)
    if pws:
        await safe_send(pws, {
            "type": "driver_assigned",
            "driver_id": driver_id,
            "pickup_lat": pickup_lat,
            "pickup_lon": pickup_lon
        })

    # Confirm driver
    dws = driver_connections.get(driver_id)
    if dws:
        await safe_send(dws, {
            "type": "ride_confirmed",
            "passenger_id": passenger_id,
            "pickup_lat": pickup_lat,
            "pickup_lon": pickup_lon
        })

    # Remove driver from available set
    try:
        redis_conn.srem("available_drivers", driver_id)
    except Exception:
        pass

    # Notify other drivers
    for d_id, ws in list(driver_connections.items()):
        if d_id != driver_id:
            await safe_send(ws, {"type": "ride_taken", "request_id": request_id})

    print(f"Ride {request_id} assigned to driver {driver_id}")



# ------------------------
# REDIS SUBSCRIBER
# ------------------------
@app.on_event("startup")
async def redis_subscribe():
    import threading

    def listen():
        pub = redis_conn.pubsub()
        pub.subscribe("ride_channel")
        print("Subscribed to Redis...")
        for msg in pub.listen():
            if msg["type"] == "message":
                print("QUEUE →", msg["data"])

    threading.Thread(target=listen, daemon=True).start()



# List all important Redis keys
@router.get("/admin/keys")
def list_keys():
    keys = redis_conn.keys("*")
    decoded = [k.decode() for k in keys]
    return {"keys": decoded}

# Get value of any key
@router.get("/admin/key/{key_name}")
def get_key(key_name: str):
    try:
        key = key_name
        if key.encode() not in redis_conn.keys("*"):
            return {"error": "Key not found"}

        type_ = redis_conn.type(key).decode()

        if type_ == "string":
            return {"type": "string", "value": redis_conn.get(key).decode()}

        if type_ == "hash":
            return {
                "type": "hash",
                "value": {k.decode(): v.decode() for k, v in redis_conn.hgetall(key).items()}
            }

        if type_ == "set":
            return {
                "type": "set",
                "value": [v.decode() for v in redis_conn.smembers(key)]
            }

        if type_ == "zset":
            z = redis_conn.zrange(key, 0, -1, withscores=True)
            return {"type": "zset", "value": [(i.decode(), s) for i, s in z]}

        return {"type": type_, "value": "Not supported yet"}

    except Exception as e:
        return {"error": str(e)}

app.include_router(router, prefix="/admin")