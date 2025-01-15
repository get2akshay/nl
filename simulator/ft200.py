import paho.mqtt.client as mqtt
import base64
import json
import os



def extract_and_decode(payload_dict):
    # Extract base64 encoded data
    base64_data = payload_dict.get("data")
    
    # Convert base64 data to bytes
    data_bytes = base64.b64decode(base64_data)
    
    # Convert bytes to hex string
    hex_data = data_bytes.hex()
    
    # Decode the hex data
    return decode_hex_data(hex_data)

def decode_hex_data(hex_data):
    header = hex_data[:2]
    length = hex_data[2:4]
    command = hex_data[4:6]
    data_field = hex_data[6:-2]
    checksum = hex_data[-2:]
    
    if header != "fa":
        return "Invalid header"
    
    command = int(command, 16)
    data_field_bytes = bytes.fromhex(data_field)
    
    if command == 0x01:
        return decode_serial_port_data(data_field_bytes)
    elif command == 0x02:
        return decode_rs485_data(data_field_bytes)
    elif command == 0x03:
        return decode_io_pin_reporting(data_field_bytes)
    elif command == 0x04:
        return decode_io_timing_collection(data_field_bytes)
    elif command == 0x05:
        return decode_io_control(data_field_bytes)
    elif command == 0x06:
        return decode_startup(data_field_bytes)
    elif command == 0x07:
        return decode_heartbeat(data_field_bytes)
    elif command == 0x08:
        return decode_shutdown(data_field_bytes)
    elif command == 0x09:
        return decode_rssi_snr_requests(data_field_bytes)
    else:
        return "Unknown command"

def decode_serial_port_data(data):
    return {"Serial Port Data": data.hex()}

def decode_rs485_data(data):
    return {"RS485 Data": data.hex()}

def decode_io_pin_reporting(data):
    return {"IO Pin Reporting": data.hex()}

def decode_io_timing_collection(data):
    return {"IO Timing Collection": data.hex()}

def decode_io_control(data):
    return {"IO Control": data.hex()}

def decode_startup(data):
    battery_power = data[0]
    reporting_interval = int.from_bytes(data[1:3], byteorder='big')
    return {"Startup": {"Battery Power": battery_power, "Reporting Interval": reporting_interval}}

def decode_heartbeat(data):
    battery_power = data[0]
    reporting_interval = int.from_bytes(data[1:3], byteorder='big')
    return {"Heartbeat": {"Battery Power": battery_power, "Reporting Interval": reporting_interval}}

def decode_shutdown(data):
    battery_power = data[0]
    reporting_interval = int.from_bytes(data[1:3], byteorder='big')
    return {"Shutdown": {"Battery Power": battery_power, "Reporting Interval": reporting_interval}}

def decode_rssi_snr_requests(data):
    return {"RSSI and SNR Requests": data.hex()}

def parse_payload(payload_dict):
    # Convert the payload string to a dictionary
    #payload_dict = json.loads(payload)
    
    # Extract the required fields
    data = payload_dict.get("data")
    dev_eui = payload_dict.get("deviceInfo", {}).get("devEui")
    location = payload_dict.get("rxInfo", [{}])[0].get("location")
    
    # Create a dictionary with the extracted values
    result = {
        "data": data,
        "devEui": dev_eui,
        "location": location
    }
    
    return result

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
    #print(f"Payload: {json.dumps(payload, indent=4)}")
    # Call the function and print the result
    parsed_data = parse_payload(payload)
    print(json.dumps(parsed_data, indent=4))
    # Extract and decode the data
    decoded_data = extract_and_decode(parsed_data)
    print(json.dumps(decoded_data, indent=4))
    

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




