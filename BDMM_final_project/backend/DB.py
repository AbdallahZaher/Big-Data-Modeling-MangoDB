from pymongo import MongoClient

host="rhea.isegi.unl.pt"
port="28004"
user="GROUP_4"
password="MjIzOTc0NzI0MTAyNzAxMzg0MjcwODk0MzIwNTA4NzA2MzYxOTAw"
protocol="mongodb"

client = MongoClient(f"{protocol}://{user}:{password}@{host}:{port}")
db = client.contracts
eu = db.eu
