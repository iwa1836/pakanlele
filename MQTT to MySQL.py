#!/usr/bin/python -u

import mysql.connector as mariadb
import paho.mqtt.client as mqtt
import ssl

mariadb_connection = mariadb.connect(host='sql6.freemysqlhosting.net', user='sql6703027', password='jVjpPldIsb', database='sql6703027')
cursor = mariadb_connection.cursor()

# MQTT Settings 
MQTT_Broker = "broker.hivemq.com"
MQTT_Port = 1883
Keep_Alive_Interval = 60
MQTT_Topic = "pakanlele/data"

# Subscribe
def on_connect(client, userdata, flags, rc):
  mqttc.subscribe(MQTT_Topic, 0)

def on_message(mosq, obj, msg):
  print(msg.payload.decode("utf-8"))
  sql = "INSERT INTO sensor_data ( %s ) VALUES ( %f )" % ("turbidity_1", float(msg.payload.decode("utf-8")))

  # Save Data into DB Table
  try:
      cursor.execute(sql)
  except mariadb.Error as error:
      print("Error: {}".format(error))
  mariadb_connection.commit()

def on_subscribe(mosq, obj, mid, granted_qos):
  pass

mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect
#mqttc.tls_set(ca_certs="ca.crt", tls_version=ssl.PROTOCOL_TLSv1_2)
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop & close db-connection
mqttc.loop_forever()
mariadb_connection.close()