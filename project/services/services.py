import datetime
import io
import pandas

from db.db import async_engine


class DataService:
    def __init__(self) -> None:
        self.buffer: list[dict[str, str | int]] = list()
        self.engine = async_engine

    @staticmethod
    def clean_table(objects: pandas.DataFrame, date: datetime.date) -> pandas.DataFrame:
        """Метод для приведения таблицы к запрашиваемой ТЗ структуре"""
        # Переименование столбцов DataFrame
        objects.columns = [x for x in range(15)]
        # Удаление ненужных столбцов и пустых строк DataFrame
        objects = objects[[1, 2, 3, 4, 5, 14]][objects[14] != '-'].dropna()
        # Удаление ненужных строк
        objects = objects.drop(objects[objects[1].str.len() != 11].index)

        # приведение данных некоторых столбцов к нужному типу
        objects = objects.astype(
            {4: 'int64', 5: 'float64', 14: 'int64'}, errors='ignore')

        # Добавление необходимых столбцов
        objects[15] = date
        objects[16] = objects[1].str[:4]
        objects[17] = objects[1].str[4:7]
        objects[18] = objects[1].str[-1]

        # Финальное переименование столбцов DataFrame
        objects.columns = [
            'exchange_product_id',
            'exchange_product_name',
            'delivery_basis_name',
            'volume',
            'total',
            'count',
            'date',
            'oil_id',
            'delivery_basis_id',
            'delivery_type_id',
        ]
        return objects

    async def get_buffer_data(self):
        """Получение данных из внутреннего хранилища"""
        return self.buffer

    @staticmethod
    def get_data_from_xls(content: bytes) -> pandas.DataFrame:
        """Получение данных из удаленного excel файла с записью в DataFrame"""
        with io.BytesIO(content) as f:
            print('Reading from excel file...')
            data = pandas.read_excel(f)
        result = pandas.DataFrame(data)
        return result

    def buffer_data(self, objects: pandas.DataFrame) -> None:
        """Получение данных из DataFrame с записью во внутреннее хранилище"""
        try:
            # Запись данных из DataFrame в список словарей внутреннего хранилища
            buffer_data = objects.to_dict('records')
            self.buffer.extend(buffer_data)
        except Exception:
            print('Data buffering error...')
