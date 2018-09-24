#!/usr/bin/env python
# -*- coding: utf-8 -*-

#============================
# Testing aiohttp and asyncio
#============================

import aiohttp
import asyncio
import socket

address = "Ипатьевский, переулок, 4-10с1, Москва, Россия; 23"
params = {"geocode": address, "format": "json"}

async def main():  # verify_ssl=False,
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get("http://geocode-maps.yandex.ru/1.x/", params=params) as response:
            json_response = await response.json()
            print(json_response)
            toponym = json_response["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]
            toponym_address = toponym["metaDataProperty"]["GeocoderMetaData"]["text"]
            toponym_coodrinates = toponym["Point"]["pos"]
            print(toponym_coodrinates)
            return toponym_coodrinates

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
#
# async def fetch(session, url):
#     async with session.get(url) as response:
#         return await response.text()
#
# async def main():
#
#     async with aiohttp.ClientSession() as session:
#         html = await fetch(session, 'http://python.org')
#         print(html)
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
