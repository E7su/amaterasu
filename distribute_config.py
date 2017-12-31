#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import contextlib
import math
import time
from multiprocessing import cpu_count, Pool

from aiohttp import ClientSession

from urls_list import urls


@contextlib.contextmanager
def timeit():
    """Замеряет время"""
    ts = time.time()

    yield  # подставляется внутренность with

    ts = time.time() - ts
    print('Time spent: {} sec'.format(ts))


def make_chunks(iterable, count):
    """Разбивает iterable на части по <=count элементов
       Пример: make_chunks([1,2,3,4,5], 2) -> [[1,2], [3,4], [5]]
    """
    return [iterable[i:i + count] for i in range(0, len(iterable), count)]


async def fetch(url, data, session):
    """Организует запрос
    """
    async with session.post(url, data=data) as response:  # контекст ответа url
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await response.read()


async def bound_fetch(sem, url, data, session):
    """ Геттер функция с семафором
    """
    async with sem:
        await fetch(url, data, session)


async def run(urls, data):
    """Создаёт клиент-сессию
    """
    # семафор ограничивает количество тасков (потоков),
    # которые могут обрабатываться одновременно
    sem = asyncio.Semaphore(1000)

    # создание сессии клиента позволяет не открывать сессию для каждого запроса
    async with ClientSession() as session:
        create_task = lambda url: asyncio.ensure_future(bound_fetch(sem, url, data, session))
        # собирает объекты в одном месте и ждёт их завершения
        responses = asyncio.gather(*map(create_task, urls))
        await responses


def proc(args):
    """Для каждого процесса запускает его собственные потоки
    """
    urls, data = args

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls, data))
    loop.run_until_complete(future)


def get_string_from_conf(conf_file):
    """Считывает файл в строку
    """
    file_conf = open(conf_file)
    conf_str = my_file.read()
    file_conf.close()
    return conf_str


def start():
    spark_conf = get_string_from_conf('spark-defaults.conf')
    # дата передается во все процессы ->
    # их потоки ->
    # отправляется в посте ->
    # ловится на сервере по ключу
    data = {'data': spark_conf}

    RESERVED_CORES_COUNT = 2
    process_count = cpu_count() - RESERVED_CORES_COUNT
    process_count = process_count if process_count >= 1 else 1

    # делим список урлов поровну для каждого процесса (чанк),
    # добавляем данные, которые уйдут в процесс
    args = [(chunk, data) for chunk in
            make_chunks(urls, math.ceil(len(urls) / process_count))]

    pool = Pool(process_count)  # создаем пул процессов по количеству ядер

    with timeit():
        pool.map_async(proc, args)
        pool.close()
        pool.join()


if __name__ == '__main__':
    start()
