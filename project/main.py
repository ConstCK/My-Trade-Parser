import asyncio
import time

import httpx

from config import MAIN_URL
from crud.crud import TradingExchange
from services.initial_tasks import create_tables
from services.parser import Parser

my_parser = Parser(MAIN_URL)
db_service = TradingExchange()


async def main():
    # Создание всех таблиц в БД, если они не существуют
    await create_tables()
    # Получение всех маршрутов для скачивания xls файлов
    urls = await my_parser.get_xls_urls(2024)
    # Получение всех данных асинхронно
    start = time.time()
    tasks = [asyncio.create_task(my_parser.get_xls_data(url)) for url in urls]
    await asyncio.gather(*tasks)
    print(f'Async data fetching time is {time.time() - start} seconds')
    # Получение всех данных синхронно
    start = time.time()
    for url in urls:
        await my_parser.get_xls_data(url)
    print(f'Sync data fetching time is {time.time() - start} seconds')
    # Извлечение всех данных для добавления в БД
    result = await my_parser.get_all_data()
    # Добавление данных в БД используя асинхронную сессию
    await db_service.async_create_or_update_data(result)
    # Добавление данных в БД используя синхронную сессию
    await db_service.sync_create_or_update_data(result)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as error:
        print(
            f'There are some app problems...exiting program...Error description: {error}')
