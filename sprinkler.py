import json
import paho.mqtt.client as mqtt
import time
from datetime import datetime
import os
import board
import busio
import digitalio
from adafruit_mcp230xx.mcp23017 import MCP23017

def on_connect (client, userdata, flags, rc):
    global zones

    """ Callback called when connection/reconnection is detected """
    print ("Connect %s result is: %s" % (host, rc))

    # With Paho, always subscribe at on_connect (if you want to
    # subscribe) to ensure you resubscribe if connection is
    # lost.
    # client.subscribe("some/topic")

    if rc == 0:
        client.connected_flag = True

        for key in zones:
            print("Subscribing to sprinkler/" + key)
            client.subscribe("sprinkler/" + key)

        print ("connected OK")
        return

    print ("Failed to connect to %s, error was, rc=%s" % rc)
    # handle error here
    sys.exit (-1)


def on_message(client, userdata, msg):
    global change

    """ Callback called for every PUBLISH received """
    print ("%s => %s" % (msg.topic, str(msg.payload)))

    change = {
        'zone': msg.topic.split("/")[1],
        'state': True if str(msg.payload.decode("utf-8")) == "ON" else False
    }

def on_subscribe(client, userdata, mid, granted_qos):
    """ Callback called for every topic subscription """
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def set_zone_state(zone):
    global zones

    print ("Zone " + zone + " is currently " + ("ON" if zones[zone]['old_state'] else "OFF") + " should be " + ("ON" if zones[zone]['new_state'] else "OFF"))

    for i in range(1,6):
        # Inverting the value since that's how the relay board works
        try:
            zones[zone]['pin'].value = not zones[zone]['new_state']
            zones[zone]['old_state'] = zones[zone]['new_state']
            break
        except OSError:
            print ("Caught OSError, try #" + str(i) + ", retrying")

            if i == 6:
                raise

    ret = client.publish ("sprinkler/" + zone + "/state", "ON" if zones[zone]['new_state'] else "OFF")
    client.loop()
    print ("Publish operation finished with ret=%s" % ret)

host          = os.environ["MQTT_HOST"]
port          = os.environ["MQTT_PORT"]
clean_session = True
client_id     = os.environ["MQTT_CLIENT_ID"]
user_name     = os.environ["MQTT_USER"]
password      = os.environ["MQTT_PASSWORD"]

i2c = busio.I2C(board.SCL, board.SDA)
mcp = MCP23017(i2c, address=0x21)

change = {}

zones = {
    'main': {
        'new_state': False,
        'old_state': False,
        'pin_number': 0
    },
    'zone1': {
        'new_state': False,
        'old_state': False,
        'pin_number': 2
    },
    'zone2': {
        'new_state': False,
        'old_state': False,
        'pin_number': 3
    },
    'zone3': {
        'new_state': False,
        'old_state': False,
        'pin_number': 4
    },
    'zone4': {
        'new_state': False,
        'old_state': False,
        'pin_number': 5
    },
    'zone5': {
        'new_state': False,
        'old_state': False,
        'pin_number': 6
    },
    'zone6': {
        'new_state': False,
        'old_state': False,
        'pin_number': 7
    }
}

# Define clientId, host, user and password
client = mqtt.Client (client_id = client_id, clean_session = clean_session)
client.username_pw_set (user_name, password)

client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe

# connect using standard unsecure MQTT with keepalive to 60
client.connect (host, port, keepalive = 60)
client.connected_flag = False
while not client.connected_flag:           #wait in loop
    print ("MQTT Client is not connected")
    client.loop()
    time.sleep (1)

# Initialize pins as outputs and update initial status
for key in zones:
    zones[key]['pin'] = mcp.get_pin(zones[key]['pin_number'])
    zones[key]['pin'].direction = digitalio.Direction.OUTPUT

    set_zone_state(key)

while True:
    if len(change) >= 1:
        # Process the change
        actions = [change]

        # Set the main state to the change that came in, which will turn it off/on
        actions.append({
            'zone': 'main',
            'state': change['state']
        })

        # Loop through the zones to turn off any that are on
        for zone in zones:
            if zones[zone]['old_state'] and zone != 'main':
                print ("Acting on " + zone + " since it is ON")
                actions.append({
                    'zone': zone,
                    'state': False
                })

        change = {}

        for action in actions:
            zones[action['zone']]['new_state'] = action['state']
            set_zone_state(action['zone'])


    ret = client.publish ("sprinkler/last_update", datetime.now())
    print ("Publish operation finished with ret=%s" % ret)

    client.loop()
    time.sleep(1)

# close connection
client.disconnect ()
