from db.db import async_engine, Base
from models.models import *


# Создание всех таблиц в БД
async def create_tables():
    async with async_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        print('All tables are created successfully...')


# Удаление всех таблиц из БД
async def delete_tables():
    async with async_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        print('All tables are deleted from DB...')
