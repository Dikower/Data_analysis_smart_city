from bs4 import BeautifulSoup
import requests
import sys

url = "https://www.cian.ru/sale/flat/193180664/"

resp = requests.get(url)
bs = BeautifulSoup(resp.text, "lxml")
spans = bs.find_all("span")
for span in spans:
    if "class" in span.attrs:
        for sp in span["class"]:
            if "star" in sp:
                # здесь можно проверять start_full и пр.
                print(span.parent.parent.parent.text, sp) # для примера - вывод названия параметра - "Транскорт", "Инфраструктура" и т.п.
                # break
