import paho.mqtt.client as mqtt
import base64
import json
import os

# Load environment variables
broker_host = os.getenv('NL_BROKER')
broker_port = int(os.getenv('NL_BROKER_PORT'))
mqtt_username = os.getenv('NL_USER')
mqtt_password = os.getenv('NL_PASS')
mqtt_topic = "application/07e86557-e000-41d9-a2a2-1b615a816a0e/device/#"

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(mqtt_topic)

# Callback when a PUBLISH message is received from the server
def on_message(client, userdata, msg):
    print(f"Message received on topic {msg.topic}")
    payload = json.loads(msg.payload.decode())
    print(f"Payload: {json.dumps(payload, indent=4)}")

# Create an MQTT client instance
client = mqtt.Client()

# Set username and password
client.username_pw_set(mqtt_username, mqtt_password)

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(broker_host, broker_port, 60)

# Start the loop to process received messages
client.loop_forever()
