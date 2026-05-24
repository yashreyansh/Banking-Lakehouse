import os
from dotenv import load_dotenv
load_dotenv()   # reads .env file

SERVER   = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

print(SERVER,DATABASE, USERNAME)