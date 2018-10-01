#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ==============================================================
# author - Dikower (Din Dmitriy)
# Here is the code for training the model
# ==============================================================

import catboost as cb
from sklearn.model_selection import train_test_split
import pandas as pd


def main():
    model = cb.CatBoostRegressor()
    # The data was mined by data_miner.py in Data prepocessing/Scripts
    data = pd.read_csv("data.csv", sep=";", encoding="utf8")
    data = data.drop(["coors"], axis=1)
    classes = {}
    # Some classes need to be mined from cian.ru
    for _class in ["Конструктив и состояние", "Положительное соседство", "Отрицательное соседство",
                   "Квартиры и планировки", "Инфраструктура", "Безопасность", "Транспорт", "Экология", "price_per_m"]:
        if _class in data.columns:
            classes[_class] = data[_class]
            data = data.drop([_class], axis=1)
    # Train models for each class
    for _class in classes:
        x_train, x_test_val, y_train, y_test_val = train_test_split(data, classes[_class], test_size=0.2, random_state=7)
        x_test, x_val, y_test, y_val = train_test_split(x_test_val, y_test_val, test_size=0.7)

        model.fit(x_train, y_train,
                  use_best_model=True,
                  eval_set=cb.Pool(x_val, y_val),
                  logging_level="Verbose",  # 'Silent', 'Verbose', 'Info', 'Debug'
                  early_stopping_rounds=1,
                  save_snapshot=True,
                  snapshot_file="backup.cbsnapshot",
                  snapshot_interval=300,
                  )
        print(model.score(x_test, y_test))
        model.save_model("trained_model", format="cbm")


if __name__ == '__main__':
    main()
