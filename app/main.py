import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from redis_client import redis_conn
import json
from model import RideRequest
app = FastAPI()

# Store active WebSocket connections
driver_connections = {}
passenger_connections = {}


# ------------------------
# DRIVER WEBSOCKET
# ------------------------
@app.websocket("/ws/driver/{driver_id}")
async def driver_ws(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    driver_connections[driver_id] = websocket

    # Mark the driver as available
    redis_conn.sadd("available_drivers", driver_id)

    print(f"Driver {driver_id} connected")

    # --- Check if driver has an ongoing ride ---
    ride_key = f"ride:driver:{driver_id}"
    ride_data = redis_conn.hgetall(ride_key)

    if ride_data:
        # Send ongoing ride details to driver
        await websocket.send_json({
            "type": "ongoing_ride",
            "passenger_id": ride_data.get("passenger_id"),
            "pickup_lat": float(ride_data.get("pickup_lat")),
            "pickup_lon": float(ride_data.get("pickup_lon")),
            "status": ride_data.get("status", "assigned")
        })

        print(f"Restored ride for driver {driver_id}")

    try:
        while True:
            data = await websocket.receive_json()
            # expecting: {"lat": 12.9, "lon": 77.6}

            lon = data.get("lon")
            lat = data.get("lat")

            print("RAW DATA RECEIVED:", data)
            print("LON:", lon, "LAT:", lat)

            # Validate values
            if lon is None or lat is None:
                print("❌ ERROR: Missing lat/lon in message:", data)
                continue

            try:
                lon = float(lon)
                lat = float(lat)
            except Exception as e:
                print("❌ ERROR converting lat/lon to float:", e)
                continue

            print("GEOADD SENDING:", driver_id, lon, lat)

            # UPDATE DRIVER GEO POSITION IN REDIS
            redis_conn.geoadd(
                "drivers_geo",
                [lon, lat, driver_id]
            )

            # Update driver status + location
            redis_conn.hset(
                f"driver:{driver_id}",
                mapping={
                    "lat": lat,
                    "lon": lon,
                    "status": data.get("status", "available")
                }
            )

    except WebSocketDisconnect:
        print(f"Driver {driver_id} disconnected")
        redis_conn.srem("available_drivers", driver_id)
        driver_connections.pop(driver_id, None)


# ------------------------
# PASSENGER WEBSOCKET
# ------------------------
@app.websocket("/ws/passenger/{passenger_id}")
async def passenger_ws(websocket: WebSocket, passenger_id: str):
    await websocket.accept()
    passenger_connections[passenger_id] = websocket
    print(f"Passenger {passenger_id} connected")

    # 🔥 NEW: Check if passenger already has assigned ride in Redis
    ride_key = f"ride:passenger:{passenger_id}"
    ride_data = redis_conn.hgetall(ride_key)

    if ride_data:
        # Convert bytes → str
        ride = {k: v for k, v in ride_data.items()}

        # Immediately send ongoing ride info
        await websocket.send_json({
            "type": "ongoing_ride",
            "driver_id": ride["driver_id"],
            "pickup_lat": float(ride["pickup_lat"]),
            "pickup_lon": float(ride["pickup_lon"]),
            "status": ride.get("status", "assigned")
        })

        print(f"Passenger {passenger_id} restored ride with driver {ride['driver_id']}")

    # Normal WS receive loop
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Passenger {passenger_id}: {data}")

    except WebSocketDisconnect:
        print(f"Passenger {passenger_id} disconnected")
        del passenger_connections[passenger_id]



# ------------------------
# PASSENGER REQUESTS A RIDE
# ------------------------
@app.post("/request_ride")
async def request_ride(data: RideRequest):

    passenger_id = data.passenger_id
    lat = data.lat
    lon = data.lon

    # 1. Store passenger location in Redis
    redis_conn.hset(
        f"passenger:{passenger_id}",
        mapping={
            "lat": lat,
            "lon": lon,
            "timestamp": time.time()
        }
    )

    # 2. Find the nearest available driver using GEO
    nearby_drivers = redis_conn.georadius(
        "drivers_geo",
        lon,
        lat,
        10,          # search radius in km
        unit="km",
        withdist=True
    )

    print("🛰 Nearby drivers with distances:")

    # Decode & print all
    decoded_drivers = []
    for raw_driver_id, dist in nearby_drivers:
        d_id = raw_driver_id.decode() if isinstance(raw_driver_id, bytes) else raw_driver_id
        decoded_drivers.append((d_id, dist))
        print(f"   Driver: {d_id}, Distance: {dist} km")

    if not decoded_drivers:
        print("❌ No nearby drivers available")
        return {"status": "no_drivers_available"}

    # ✅ SORT THE DRIVERS BY DISTANCE
    decoded_drivers.sort(key=lambda x: x[1])  # sort by distance ASC

    # Select closest
    driver_id, distance = decoded_drivers[0]
    print(f"🏎 Closest driver (sorted): {driver_id}, distance={distance} km")

    # Check availability
    if not redis_conn.sismember("available_drivers", driver_id):
        print("❌ Driver not available anymore:", driver_id)
        return {"status": "no_drivers_available"}

    # Remove from available pool
    redis_conn.srem("available_drivers", driver_id)

    # 3. Create a ride object (store in Redis)
    ride_payload = {
        "passenger_id": passenger_id,
        "driver_id": driver_id,
        "pickup_lat": lat,
        "pickup_lon": lon,
        "status": "assigned",
        "timestamp": time.time()
    }

    redis_conn.hmset(f"ride:passenger:{passenger_id}", ride_payload)
    redis_conn.hmset(f"ride:driver:{driver_id}", ride_payload)

    # 4. Notify the driver via WebSocket
    if driver_id in driver_connections:
        await driver_connections[driver_id].send_json({
            "type": "new_ride",
            "passenger_id": passenger_id,
            "pickup": {"lat": lat, "lon": lon}
        })

    # 5. Notify the passenger via WebSocket
    if passenger_id in passenger_connections:
        await passenger_connections[passenger_id].send_json({
            "type": "driver_assigned",
            "driver_id": driver_id,
            "pickup_confirmed": True
        })

    return {
        "status": "driver_assigned",
        "driver_id": driver_id,
        "pickup_lat": lat,
        "pickup_lon": lon
    }


# ------------------------
# REDIS SUBSCRIBER (Background Task)
# ------------------------
@app.on_event("startup")
async def redis_subscribe():
    import threading

    def listen():
        pubsub = redis_conn.pubsub()
        pubsub.subscribe("ride_channel")
        print("Subscribed to Redis queue...")

        for message in pubsub.listen():
            if message["type"] == "message":
                print("QUEUE EVENT:", message["data"])

    thread = threading.Thread(target=listen)
    thread.start()
