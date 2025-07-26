
from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://p95532980:Tk8m1SW6jnz1nB2V@cluster0.v7pgf3n.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)