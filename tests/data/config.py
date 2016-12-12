# encoding: utf-8
import os

# For testing the postgrest module
POSTGREST_URL = "https://marple-api.herokuapp.com"
POSTGREST_TABLES = [
    "dataset",
]
POSTGREST_ROLE = os.environ.get('DB_USER')
POSTGREST_JWT_TOKEN = os.environ.get('JWT_TOKEN')

AWS_ACCESS_ID = os.environ.get('AWS_ACCESS_ID')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')


