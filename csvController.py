import csv
import os
import shutil

from typing import List

from clock import get_day


async def save_data(path, datas):
    with open(path, "a", newline='\n') as file:
        writer = csv.writer(file)
        writer.writerows(datas)


async def make_file_name(name: str, date: str) -> str:
    return name + '_' + date + '.csv'


class CsvController:
    def __init__(self,
                 directory: str,
                 device_name: str,
                 header: List[str],
                 external_directory: str):
        self.writer = CsvWriter(header)
        self.directory = directory
        self.device_name = device_name

        self.external_directory = external_directory
        self.last_date = get_day()

    async def init_dirs(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    async def add_data(self, datas):
        await self.init_dirs()
        date = get_day()
        path = await self.get_file_path(date)

        await self.writer.save(path, datas)
        await self.trigger(date)

    async def get_file_path(self, date: str) -> str:
        file_name = await make_file_name(self.device_name, date)
        return os.path.join(self.directory, file_name)

    async def trigger(self, date):
        is_day_changed = self.last_date != date
        if is_day_changed:
            await self.move_file(self.last_date)
            self.last_date = date

    async def move_file(self, date: str):
        file_name = await make_file_name(self.device_name, date)
        src_path = os.path.join(self.directory, file_name)
        dest_path = os.path.join(self.external_directory, file_name)

        if os.path.isfile(src_path) and os.path.isdir(self.external_directory):
            shutil.move(src_path, dest_path)


class CsvWriter:
    def __init__(self, header: List[str]):
        self.header = header

    async def file_init(self, path: str):
        if not os.path.isfile(path):
            with open(path, "w", newline='\n') as file:
                writer = csv.writer(file)
                writer.writerow(self.header)

    async def save(self, path, datas):
        try:
            await self.file_init(path)

            transpose = [list(x) for x in zip(*datas)]
            await save_data(path, transpose)
        except Exception as e:
            print('예외가 발생하였습니다. 전체 로그를 개발자에게 전달 부탁드립니다.')
            print(e)