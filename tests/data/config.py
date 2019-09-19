# encoding: utf-8
import os

from dotenv import load_dotenv

if os.path.exists(".env"):
    load_dotenv(".env")

# For testing the postgrest module
POSTGREST_URL = "https://marple-api.herokuapp.com"
POSTGREST_TABLES = [
    "dataset",
]
POSTGREST_ROLE = os.environ.get('DB_ROLE')
POSTGREST_JWT_TOKEN = os.environ.get('JWT_TOKEN')

assert POSTGREST_ROLE is not None
assert POSTGREST_JWT_TOKEN is not None

AWS_ACCESS_ID = os.environ.get('AWS_ACCESS_ID')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')

TAB_DATA_URL = os.environ['TAB_DATA_URL']
TAB_DATA_DB_ROLE = os.environ['TAB_DATA_DB_ROLE']
TAB_DATA_JWT_TOKEN = os.environ['TAB_DATA_JWT_TOKEN']
