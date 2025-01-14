import asyncio

from config import MAIN_URL
from crud.crud import TradingExchange
from services.initial_tasks import create_tables
from services.parser import Parser


my_parser = Parser(MAIN_URL)
db_service = TradingExchange()


async def main():
    await create_tables()
    await my_parser.get_xls_urls(2024)
    await my_parser.get_xls_data()
    result = await my_parser.get_all_data()
    await db_service.sync_create_or_update_data(result)
    await db_service.async_create_or_update_data(result)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as error:
        print(
            f'There are some app problems...exiting program...Error description: {error}')
