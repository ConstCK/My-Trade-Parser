import os

from dotenv import load_dotenv
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
BASE_URL = 'https://spimex.com'
MAIN_URL = 'https://spimex.com/markets/oil_products/trades/results/'
SELECTED_CLASS = 'accordeon-inner__item-title link xls'

# Маршрут для связи с БД
ASYNC_DB_URL = f'postgresql+asyncpg://{DB_USER}:{
    DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
SYNC_DB_URL = f'postgresql+psycopg2://{DB_USER}:{
    DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
