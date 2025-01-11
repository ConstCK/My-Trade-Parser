import datetime
from sqlalchemy import select, update, insert
from sqlalchemy.dialects.postgresql import insert as insert_with_conflict


from db.db import async_session
from models.models import TradingResult
from services.initial_tasks import create_tables, delete_tables


class TradingExchange:
    def __init__(self) -> None:
        self.session = async_session()

    async def add_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания таблицы с данными с торговой биржи с нуля"""
        await delete_tables()
        await create_tables()
        print('Starting data transfering from local storage to DB...')
        async with self.session as s:
            for i in data:
                stmt = insert(TradingResult).values(
                    exchange_product_id=i['exchange_product_id'],
                    exchange_product_name=i['exchange_product_name'],
                    oil_id=i['oil_id'],
                    delivery_basis_id=i['delivery_basis_id'],
                    delivery_basis_name=i['delivery_basis_name'],
                    delivery_type_id=i['delivery_type_id'],
                    volume=i['volume'],
                    total=int(i['total']),
                    count=i['count'],
                    date=i['date'],
                )
                try:
                    await s.execute(stmt)
                    await s.commit()
                except Exception as error:
                    print('Error adding data to DB...', error)
                    continue

        print('All data is successfully added to DB...')


# Альтернативный вариант добавления данных в БД

    async def add_all_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания таблицы с данными с торговой биржи с нуля"""
        await delete_tables()
        await create_tables()

        print('Starting data transfering from local storage to DB...')
        async with self.session as s:
            entries = [TradingResult(**i) for i in data]
            try:
                s.add_all(entries)
                await s.commit()
            except Exception as error:
                print('Error adding data to DB...', error)

        print('All data is successfully added to DB...')

# Альтернативный вариант добавления данных в БД с обновлением существующих записей
    async def add_or_update_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания/обновления данных в таблице с результатами торгов"""

        print('Starting data transfering from local storage to DB...')
        async with self.session as s:

            for i in data:
                stmt = select(TradingResult).where(
                    (TradingResult.exchange_product_id ==
                     i['exchange_product_id'])
                    & (TradingResult.date == i['date'])
                )
                obj = await s.execute(stmt)
                result = obj.scalar()

                if not result:
                    s.add(TradingResult(**i))
                    await s.commit()
                else:
                    stmt = update(TradingResult).where(
                        TradingResult.exchange_product_id == i['exchange_product_id']).values()
                    await s.execute(stmt)
                    await s.commit()

        print('All data is successfully added to DB...')

# Еще один альтернативный вариант добавления данных в БД с обновлением существующих записей!
    async def create_or_update_data(self, data: list[dict[str, str | int]]) -> None:
        """Метод для создания/обновления данных в таблице с результатами торгов"""

        print('Starting data transfering from local storage to DB...')
        async with self.session as s:
            for i in data:
                stmt = insert_with_conflict(TradingResult).values(
                    **i).on_conflict_do_update(index_elements=['exchange_product_id', 'date'],
                                               set_=dict(updated_on=datetime.datetime.now()))
                await s.execute(stmt)
                await s.commit()
        print('All data is successfully added to DB...')
