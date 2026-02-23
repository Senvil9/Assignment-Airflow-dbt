import os
import requests
import psycopg2
from datetime import datetime

API_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = [
    {"name": "Kathmandu", "latitude": 27.7172, "longitude": 85.3240},
    {"name": "Pokhara", "latitude": 28.2669, "longitude": 83.9685},
    {"name": "Biratnagar", "latitude": 26.4525, "longitude": 87.2718},
    {"name": "Birgunj", "latitude": 27.0171, "longitude": 84.8808}
]

def extract_and_load_weather():
    """Fetch weather for all cities"""
    all_records = []
    params_base = {
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Kathmandu",
        "past_days": 7
    }

    # Extract phase
    for city in CITIES:
        params =  params_base.copy()
        params["latitude"] = city["latitude"]
        params["longitude"] = city["longitude"]

        r = requests.get(API_URL, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        days = payload['daily']['time']
        tmax = payload['daily']['temperature_2m_max']
        tmin = payload['daily']['temperature_2m_min']
        psum = payload['daily']['precipitation_sum']

        for i in range(len(days)):
            all_records.append({
                'ingested_at': datetime.now(),
                'city': city['name'],
                'latitude': city['latitude'],
                'longitude': city['longitude'],
                'day': days[i],
                'temp_max': tmax[i],
                'temp_min': tmin[i],
                'precipitation_sum': psum[i]    
            })
          
    # load phase
    conn =  psycopg2.connect (
        host = os.getenv("WAREHOUSE_HOST"),
        port = os.getenv("WAREHOUSE_PORT"),
        dbname = os.getenv("WAREHOUSE_DB"),
        user= os.getenv("WAREHOUSE_USER"),
        password = os.getenv("WAREHOUSE_PASSWORD")
    )

    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            for rec in all_records:
                cur.execute(
                    """
                    INSERT INTO bronze.weather_raw
                    (ingested_at, city_name, latitude, longitude, day, temp_max, temp_min, precipitation_sum)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (rec['ingested_at'], rec['city'], rec['latitude'], rec['longitude'],
                     rec['day'], rec['temp_max'], rec['temp_min'], rec['precipitation_sum'])
                )
        print(f"Successfully inserted {len(all_records)} records across {len(CITIES)} cities.")
        return len(all_records)
    except Exception as e:
        print(f"Load failed: {e}")
        return 0
    finally:
        conn.close()
