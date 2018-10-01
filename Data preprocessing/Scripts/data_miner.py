#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ==============================================================
# author - Dikower (Din Dmitriy)
# Here is the class for data mining and training model functions
# Special thanks to Elena Nikitina for the code on BeatifulSoup
# ==============================================================

import os
import time
import math

import asyncio
import aiohttp
import socket
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

import logging
logging.basicConfig(format="[%(asctime)s][%(levelname)s]-%(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class DataMiner:
    def __init__(self, api_key: str, csv_file_path: str, metro_data_path: str, classes: list, language: str,
                 mine_coors=True):
        # Variables for requests
        self.api_key = api_key
        self.format = "json"
        self.language = language
        self.classes = classes
        self.search_url = "https://search-maps.yandex.ru/v1/"
        self.geocode_url = "http://geocode-maps.yandex.ru/1.x/"
        self.spn = "0.015,0.015"
        self.results = 500

        # Variables for async running
        self.event_loop = asyncio.get_event_loop()
        self.data_base = pd.read_csv(csv_file_path, sep=';', encoding="utf8")
        self.variable_data_base = None
        self.mine_coors = mine_coors
        zero_vector = np.zeros(self.data_base.shape[0])
        for _class in self.classes:
            self.data_base[_class] = pd.Series(zero_vector)
            self.data_base[f"min_distance_for_{_class}"] = pd.Series(zero_vector)
            self.data_base[f"mean_distance_for_{_class}"] = pd.Series(zero_vector)

        self.metro_data_path = metro_data_path
        try:
            os.mkdir("backups")
        except FileExistsError:
            pass

        logging.basicConfig(format="[%(asctime)s][%(levelname)s] - %(message)s",
                            level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    # The function which starts coroutines filling data_base with coordinates
    async def get_coors(self, session):
        # using index because of async returning => non sorted results
        futures = [await self.find_object(session, index, address)
                   for index, address in zip(self.data_base.index, self.data_base["address"])]
        coors = list(range(self.data_base.shape[0]))
        for future in futures:
            index, dot = future
            coors[index] = ",".join(dot)
        self.data_base["coors"] = pd.Series(coors)

    # The function which starts coroutines adding classes columns
    async def search_by_columns(self, session):  # makes n parallel responses for n flats
        self.logger.info("Starting search by columns")
        for _class in self.classes:
            # using index because of async returning => non sorted results
            futures = [asyncio.ensure_future(self.search_objects_class(session, index, coordinates, _class))
                       for index, coordinates in zip(self.data_base.index, self.data_base["coors"])]

            new_class = np.zeros(self.data_base.shape[0])
            new_distances = np.zeros(self.data_base.shape[0])
            new_mean_distance = np.zeros(self.data_base.shape[0])
            start = time.process_time()
            times = 0
            futures = await asyncio.gather(*futures)
            for future in futures:
                _, index, value, min_distance, mean_distance = future
                new_class[index] = value
                new_distances[index] = min_distance
                new_mean_distance[index] = mean_distance
                times += 1
                if times % 10 == 0:
                    print(f"{(times/ self.data_base.shape[0]) * 100}%")

            self.data_base[_class] = pd.Series(new_class)
            self.data_base[f"min_distance_for_{_class}"] = pd.Series(new_distances)
            self.data_base[f"mean_distance_for_{_class}"] = pd.Series(new_mean_distance)

            self.data_base.to_csv(f"backups/backup_{time.time()}.csv", sep=";", encoding="utf8", index=False)
            print(f"The Mining of {_class} class finished after {round(time.process_time() - start, 2)}")

    # The function which starts coroutines adding filled rows
    async def search_by_rows(self, session):  # makes n parallel requests for n classes
        self.logger.info("Starting search by rows")
        for index, flat in self.data_base.iterrows():
            # using index because of async returning => non sorted results
            futures = [asyncio.ensure_future(self.search_objects_class(session, index, flat["coors"], _class))
                       for _class in self.classes]

            futures = await asyncio.gather(*futures)
            for future in futures:
                _class, index, value, min_distance, mean_distance = future
                self.data_base.at[index, _class] = value
                self.data_base.at[index, f"min_distance_for_{_class}"] = min_distance
                self.data_base.at[index, f"mean_distance_for_{_class}"] = mean_distance

            if (index + 1) % 100 == 0:
                self.data_base.to_csv(f"backups/backup_{time.time()}.csv", sep=";", encoding="utf8", index=False)
                self.logger.info(f"{round(index/self.data_base.shape[0] * 100, 2)}%")

    # The request coroutine getting coordinates from address using yandex geocode
    async def find_object(self, session, index, address):
        toponym_coodrinates = ""
        async with session.get(self.geocode_url, params={"geocode": address, "format": self.format}) as response:
            json_response = await response.json()
            try:
                toponym = json_response["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]
                toponym_coodrinates = toponym["Point"]["pos"]
            except:
                self.logger.info(f"ERROR in find_object! Geocoder response: {json_response}")
        # print(toponym_coodrinates)
        return index, toponym_coodrinates.split()

    # The request coroutine getting amount of class objects near the dot using yandex geosearch
    async def search_objects_class(self, session, index, coordinates, objects_class):
        search_params = {
            "apikey": self.api_key,
            "text": objects_class,
            "lang": self.language,
            "ll": coordinates,
            "type": "biz",
            "spn": self.spn,
            "rspn": 1,
            "results": self.results
        }

        async with session.get(self.search_url, params=search_params) as response:
            json_response = await response.json()
            # self.logger.info(json_response)
            try:
                value = len(json_response["features"])
                objects = {}
                for _object in json_response["features"]:
                    name = _object["properties"]["CompanyMetaData"]["name"]
                    if objects_class == "метро":
                        if name in self.variable_data_base.columns:
                            self.data_base["prices_near_metro"] = self.variable_data_base[name]
                    object_coordinates = _object['geometry']['coordinates']
                    distance = await self.distance(coordinates.split(","), object_coordinates)
                    objects[name] = distance

                min_distance = 0
                mean_distance = 0
                if objects != {}:
                    min_distance = round(objects[min(objects.keys(), key=lambda x: objects[x])])
                    mean_distance = round(np.array(list(objects.values())).mean())
            except:
                self.logger.info(f"ERROR in search! Organisation search response: {json_response}")
                index, value, min_distance, mean_distance = 0, 0, 0, 0

            return objects_class, index, value, min_distance, mean_distance

    def add_metro_data(self):
        self.variable_data_base = pd.read_csv(self.metro_data_path, encoding="utf8", sep=";")
        self.data_base["prices_near_metro"] = pd.Series(np.zeros(self.data_base.shape[0]))

    @staticmethod
    async def parse_flat_page(session, url):
        evaluations = {
            "Конструктив и состояние": 0,
            "Положительное соседство": 0,
            "Отрицательное соседство": 0,
            "Квартиры и планировки": 0,
            "Инфраструктура": 0,
            "Безопасность": 0,
            "Транспорт": 0,
            "Экология": 0
        }
        async with session.get(url) as response:
            bs = BeautifulSoup(await response.text(), "lxml")
            spans = bs.find_all("span")
            for span in spans:
                if "class" in span.attrs:
                    for sp in span["class"]:
                        evaluation_class = span.parent.parent.parent.text
                        if "star_full" in sp:
                            evaluations[evaluation_class] = evaluations.get(evaluation_class, 0) + 1
                        elif "star_half" in sp:
                            evaluations[evaluation_class] = evaluations.get(evaluation_class, 0) + 0.5

            return evaluations

    # The function calculating distance between two dots
    @staticmethod
    async def distance(a, b):
        degree_to_meters_factor = 111 * 1000  # 111 километров в метрах
        a_lon, a_lat = map(float, a)
        b_lon, b_lat = map(float, b)

        # Берем среднюю по широте точку и считаем коэффициент для нее.
        radians_lattitude = math.radians((a_lat + b_lat) / 2.)
        lat_lon_factor = math.cos(radians_lattitude)

        # Вычисляем смещения в метрах по вертикали и горизонтали.
        dx = abs(a_lon - b_lon) * degree_to_meters_factor * lat_lon_factor
        dy = abs(a_lat - b_lat) * degree_to_meters_factor

        # Вычисляем расстояние между точками.
        distance = math.sqrt(dx * dx + dy * dy)
        return distance

    # The function starting work
    async def mine(self, rows):
        # connector = aiohttp.TCPConnector(verify_ssl=False, family=socket.AF_INET)
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            if self.mine_coors:
                await self.get_coors(session)

            if rows:
                await self.search_by_rows(session)
            else:
                await self.search_by_columns(session)


# Buildings type to search (features for model)
classes = ["метро", "аптека", "парк", "кафе", "торговый центр", "школа", "детский сад", "улица",
           "магазин", "больница", "поликлиника", "остановка", "парковка", "спортзал", "кино"]

# Language of classes (find supporting languages on https://tech.yandex.ru/maps/geosearch/?from=mapsapi)
language = "ru_RU"

# Your token for yandex organisation search api
# This one allows you to make only 500 requests per a day, so you can fill only 33 rows
token = "3c4a592e-c4c0-4949-85d1-97291c87825c"  #

# File path (the class opens csv with sep=';'. The file columns: address; ... your columns for model)
path = "prices.csv"
metro_data_path = "processed_prices_near_metro"
# If there is column with coors switch to False
mine_coors = False
# Not recommended to switch to False
# Because it can send a lot of parallel requests, where its number is depended by number of flats
mine_by_rows = True

# Starts mining
dm = DataMiner(token, path, metro_data_path, classes, language, mine_coors)
dm.event_loop.run_until_complete(dm.mine(mine_by_rows))
dm.add_metro_data()
dm.event_loop.close()

# Saves mined data to csv
dm.data_base.to_csv("result.csv", sep=";", encoding="utf8", index=True)
