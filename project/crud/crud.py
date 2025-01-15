import datetime
from sqlalchemy.dialects.postgresql import insert as insert_with_conflict

from db.db import async_session, session
from models.models import TradingResult
from services.services import time_it


class TradingExchange:
    """
    Класс для CRUD операций с БД
    """

    def __init__(self) -> None:
        self.async_session = async_session()
        self.sync_session = session()

    # Метод для асинхронного добавления данных в БД с обновлением существующих записей!
    @time_it
    async def async_create_or_update_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания/обновления данных в таблице с результатами торгов"""

        print('Starting asynchronous data transferring from local storage to DB...')
        async with self.async_session as s:
            for i in data:
                stmt = insert_with_conflict(TradingResult).values(
                    **i).on_conflict_do_update(index_elements=['exchange_product_id', 'date'],
                                               set_=dict(updated_on=datetime.datetime.now()))
                await s.execute(stmt)
                await s.commit()
        print('All data is successfully added to DB...')

    # Метод для синхронного добавления данных в БД с обновлением существующих записей!
    @time_it
    async def sync_create_or_update_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания/обновления данных в таблице с результатами торгов"""
        print('Starting synchronous data transferring from local storage to DB...')
        with self.sync_session as s:
            for i in data:
                stmt = insert_with_conflict(TradingResult).values(
                    **i).on_conflict_do_update(index_elements=['exchange_product_id', 'date'],
                                               set_=dict(updated_on=datetime.datetime.now()))
                s.execute(stmt)
                s.commit()
        print('All data is successfully added to DB...')
