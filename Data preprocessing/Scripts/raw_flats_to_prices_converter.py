#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# =========================================================================================
# Data from:
# https://github.com/npetrenko/CIAN-parser
# =========================================================================================

import pandas as pd

df = pd.read_csv("../Data/raw_flats.csv", encoding="utf8")
df = df[df.roomarea5 <= 0]
new_df = pd.DataFrame()

for column in ["lat", "lon", "price", "overallarea"]:
    new_df[column] = df[column]

del df
new_df["price_per_m"] = new_df["price"] / new_df["overallarea"]
new_df["coors"] = new_df["lon"].astype(str)+','+new_df["lat"].astype(str)
new_df = new_df.drop(["overallarea", "price", "lon", "lat"], axis=1)
new_df.to_csv("../Data/prices.csv", index=False)
