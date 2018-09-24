import pandas as pd

df = pd.read_csv("unprocessed_prices_near_metro.csv", sep=";", encoding="utf8")
new_data = {}
for row in range(df.shape[0]):
    metros = df["metros"][row].split(", ")
    for metro in metros:
        new_data[metro] = [df["price"][row]]
new_data = pd.DataFrame(new_data)
new_data.to_csv("processed_prices_near_metro.csv")
