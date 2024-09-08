from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from time import sleep
# import sys


import random
import datetime

# Get environment variables
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
# BUCKET_NAME = "your-new-bucket-name"


def check_bucket_exists(bucket_name):
    # Initialize InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    try:
        # Access the Buckets API
        buckets_api = client.buckets_api()
        
        # Retrieve all buckets
        buckets = buckets_api.find_buckets().buckets
        
        # Check if the specified bucket exists
        for bucket in buckets:
            if bucket.name == bucket_name:
                print(f"Bucket '{bucket_name}' exists.")
                return True
        
        print(f"Bucket '{bucket_name}' does not exist.")
        return False
    except Exception as e:
        print(f"Error checking bucket: {e}")
        return False
    finally:
        # Close the client connection
        client.close()


def create_bucket(bucket_name):
    # Initialize InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    try:
        # Access the Buckets API
        buckets_api = client.buckets_api()

        # Check if bucket already exists
        existing_buckets = buckets_api.find_buckets().buckets
        for bucket in existing_buckets:
            if bucket.name == bucket_name:
                print(f"Bucket '{bucket_name}' already exists.")
                return bucket.id

        # Create the new bucket
        new_bucket = buckets_api.create_bucket(bucket_name=bucket_name, org=INFLUXDB_ORG)
        print(f"Bucket '{bucket_name}' created successfully.")
        return new_bucket.id
    except Exception as e:
        print(f"Error creating bucket: {e}")
    finally:
        client.close()

def insert_sensor_data(bucket_name, sensor_data):
    # Initialize InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    try:
        # Initialize the write API
        write_api = client.write_api(write_options=SYNCHRONOUS)

        # Iterate over each sensor data and write it to InfluxDB
        for data in sensor_data:
            point = Point("sensor_measurement") \
                .tag("location", data["location"]) \
                .tag("device_id", data["device_id"]) \
                .tag("make", data["make"]) \
                .tag("software_version", data["software_version"]) \
                .tag("os_version", data["os_version"]) \
                .field("temperature", data["temperature"]) \
                .field("humidity", data["humidity"])

            # Write the point to the bucket
            write_api.write(bucket=bucket_name, org=INFLUXDB_ORG, record=point)

        print(f"Data successfully written to '{bucket_name}'.")
    except Exception as e:
        print(f"Error writing sensor data: {e}")
    finally:
        client.close()

def generate_simulated_temperature_humidity_data():
    # Define the range for temperature (in Celsius) and humidity (in percentage)
    temperature_min = 12  # Minimum temperature in Shimla in September
    temperature_max = 20  # Maximum temperature in Shimla in September
    humidity_min = 60     # Minimum humidity in Shimla in September
    humidity_max = 90     # Maximum humidity in Shimla in September

    # Initialize an empty list to store the simulated data
    simulated_data = []

    # Generate data for each day of September (30 days)
    for day in range(1, 31):
        # Generate a random temperature and humidity for the day
        temperature = round(random.uniform(temperature_min, temperature_max), 2)
        humidity = round(random.uniform(humidity_min, humidity_max), 2)

        # Create a date object for the current day of September
        date = datetime.date(2023, 9, day)

        # Store the date, temperature, and humidity in a dictionary
        data = {
            "date": date,
            "temperature": temperature,
            "humidity": humidity
        }

    return data


def simulated_sensor():
    # out = generate_simulated_temperature_humidity_data().get('temperature')
    # print(out)
    # sys.exit()
    bucket_name = INFLUX_BUCKET
    if not check_bucket_exists(bucket_name):
        print(f"Bucket or DB {bucket_name} does not exits !")
        bucket_id = create_bucket(bucket_name)
        if bucket_id:
            print(f"Bucket or DB {bucket_name} created!")
        else:   
            print(f"Bucket or DB {bucket_name} could not be created!")

    # Example sensor data
    sensor_data = [
        {
            "location": "Bangalore-Empyrean0",
            "device_id": "akshay_laptop_simulator",
            "make": "PythonCodeDummy",
            "software_version": "v1.2.0",
            "os_version": "OS10.5",
            "temperature": generate_simulated_temperature_humidity_data().get('temperature'),
            "humidity": generate_simulated_temperature_humidity_data().get('humidity')
        },
        {
            "location": "Bangalore-Empyrean1",
            "device_id": "akshay_laptop_simulator",
            "make": "PythonCodeDummy",
            "software_version": "v1.2.1",
            "os_version": "OS10.6",
            "temperature": generate_simulated_temperature_humidity_data().get('temperature'),
            "humidity": generate_simulated_temperature_humidity_data().get('humidity')
        }
    ]

    # Insert the sensor data into the bucket
    insert_sensor_data(INFLUX_BUCKET, sensor_data)



if __name__ == "__main__":
    upload_interval = 10 * 60 # minutes * seconds
    while True:
        generate_simulated_temperature_humidity_data()
        sleep(upload_interval)
        

   
    
