import datetime

import httpx
from bs4 import BeautifulSoup

from config import SELECTED_CLASS, BASE_URL
from services.services import DataService, time_it


class Parser:
    def __init__(self, url: str) -> None:
        self.url = url
        self.xls_urls: list[tuple[str, str]] = list()
        self.service = DataService()

    async def get_xls_urls(self, limit: int) -> list[tuple[str, str]]:
        """Получение всех маршрутов (с указанного года) с Excel файлами с данными для БД
        и сохранение их локально"""
        page = 0
        data_year = datetime.datetime.now().year
        print('Receiving data files urls...')
        while limit <= data_year:
            page += 1
            async with httpx.AsyncClient() as client:
                r = await client.get(f'{self.url}?page=page-{page}')
                soup = BeautifulSoup(r.content, 'html.parser')

                raw_data = soup.find_all('a',
                                         class_=SELECTED_CLASS)[:10]
                data = [x['href'] for x in raw_data]

                for i in data:
                    data_year = int(i[32:36])
                    if data_year < limit:
                        break
                    self.xls_urls.append((i[32:40], i))

        return self.xls_urls

    async def get_xls_data(self, xls_url) -> None:
        """Получение данных с удаленного excel файла с сохранением в локальный буфер"""
        date, url = xls_url

        current_date = datetime.date.fromisoformat(
            f'{date[:4]}-{date[4:6]}-{date[6:]}')
        current_url = f'{BASE_URL}{url}'

        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(current_url)
                objects = self.service.get_data_from_xls(r.content)
                print(f'Adding data to local storage from {current_url}... ')
                objects = self.service.clean_table(objects, current_date)
                self.service.buffer_data(objects)
            except Exception as err:
                print('Data receiving, refactoring or buffering error...', err)

    async def get_all_data(self) -> list[dict[str, str | int]]:
        return await self.service.get_buffer_data()
