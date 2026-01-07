#Importing Libraries
import requests
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pandas as pd
from sqlalchemy import create_engine, text

#Fetching data from API
response = requests.get("https://financialmodelingprep.com/stable/search-name?query=apple&apikey=WKVgrPC3nHyltW8geY4ssaogMVDiWqLM")


if response.status_code == 200:
    value = response.json()    
    key = "Apple"
    print("Fetched Data successfully")
else:
    print(f"Error : {response.status_code}")
    value = None


#Kakfka Producer Setup
producer = KafkaProducer(
    bootstrap_servers = '127.0.0.1:9092', 
    key_serializer = str.encode,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

#Kafka Consumer Setup
consumer = KafkaConsumer(
    "stocks",
    bootstrap_servers = '127.0.0.1:9092',
    key_deserializer = lambda k: k.decode('utf-8'),
    value_deserializer = lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset = 'earliest', enable_auto_commit = False
)

#Producer send 
producer.send("stocks", key=key, value=value)
producer.flush()
print(f"Sent: {key} → {value}")

#Consumer receive
for message in consumer:
    print(f"Received - Key: {message.key}, Value: {message.value}")
    break  


print("Consumer finished")


#Creating DataFrame
df = pd.DataFrame(value)

print(df)

df.to_csv('stocks_info.csv', index= False)

#Connect to SQL Server
try:
    connection_stocks_info = create_engine("mssql+pyodbc://@RIYA\\SQLEXPRESS/stocks_info"
    "?driver=ODBC+DRiver+17+for+SQL+Server"
    "&trusted_connection=yes")
    
  
    with connection_stocks_info.connect()as conn:
        conn.commit()
    
    print("Connected to Database")
    

except Exception as ex:
    print("Connection failed",ex)

# Insert Data in SQL Server
df.to_sql('stocks',con = connection_stocks_info, schema= 'dbo', if_exists= "append",index= False)


