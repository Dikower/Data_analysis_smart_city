#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ==============================================================
# author - Dikower (Din Dmitriy)
# Here is the class for data mining and training model functions
# ==============================================================

import os
import time
import math

import asyncio
import aiohttp
import socket

import pandas as pd
import numpy as np


class DataMiner:
    def __init__(self, api_key: str, csv_file_path: str, classes: list, language: str, mine_coors=True):
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
        try:
            os.mkdir("backups")
        except FileExistsError:
            pass

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
    async def search_classes(self, session):
        print("search_classes")
        for _class in self.classes:
            # using index because of async returning => non sorted results
            if _class == "метро":
                self.variable_data_base = pd.read_csv("processed_prices_near_metro.csv", encoding="utf8", sep=";")
                self.data_base["prices_near_metro"] = pd.Series(np.zeros(self.data_base.shape[0]))
            futures = [await self.search_objects_class(session, index, coordinates, _class)
                       for index, coordinates in zip(self.data_base.index, self.data_base["coors"])]
            new_class = np.zeros(self.data_base.shape[0])
            new_distances = np.zeros(self.data_base.shape[0])
            for future in futures:
                index, value, min_distance = future
                new_class[index] = value
                new_distances[index] = value
            self.data_base[_class] = pd.Series(new_class)
            self.data_base[f"min_distance_for_{_class}"] = pd.Series(new_distances)
            self.data_base.to_csv(f"backups/backup_{time.time()}.csv", sep=";", encoding="utf8", index=False)

    # The request coroutine getting coordinates from address using yandex geocode
    async def find_object(self, session, index, address):
        async with session.get(self.geocode_url, params={"geocode": address, "format": self.format}) as response:
            json_response = await response.json()
        toponym = json_response["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]
        toponym_coodrinates = toponym["Point"]["pos"]
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
            # print(json_response)
            value = len(json_response["features"])
            objects = {}
            for object in json_response["features"]:
                name = object["properties"]["CompanyMetaData"]["name"]
                if objects_class == "метро":
                    if name in self.variable_data_base.columns:
                        self.data_base["prices_near_metro"] = self.variable_data_base[name]
                object_coordinates = object['geometry']['coordinates']
                distance = await self.distance(coordinates.split(","), object_coordinates)
                objects[name] = distance

            if objects != {}:
                min_distance = objects[min(objects.keys(), key=lambda x: objects[x])]
            else:
                min_distance = 0

            return index, value, min_distance

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
    async def mine(self):
        # connector = aiohttp.TCPConnector(verify_ssl=False, family=socket.AF_INET)
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            if self.mine_coors:
                _ = [await func(session) for func in [self.get_coors, self.search_classes]]
            else:
                await self.search_classes(session)


# Buildings type to search (features for model)
classes = ["метро", "аптека", "парк", "кафе", "торговый центр", "школа", "детский сад", "улица",
           "магазин", "больница", "поликлиника", "остановка", "парковка", "спортзал", "кино"]

# Language of classes (find supporting languages on https://tech.yandex.ru/maps/geosearch/?from=mapsapi)
language = "ru_RU"

# Your token for yandex geocoder api
token = "3c4a592e-c4c0-4949-85d1-97291c87825c"

# File path (the class opens csv with sep=';'. The file columns: address; ... your columns for model;)
path = "data.csv"

# If there is column with coors switch to False
mine_coors = True

# Starts mining
dm = DataMiner(token, path, classes, language, mine_coors)
dm.event_loop.run_until_complete(dm.mine())
dm.event_loop.close()

# Saves mined data to csv
dm.data_base.to_csv("result.csv", sep=";", encoding="utf8", index=False)
