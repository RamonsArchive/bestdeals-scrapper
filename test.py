import pandas as pd

df = pd.read_csv("facebook_marketplace_listings.csv")
last_column = df.columns[-1]
print(df[last_column].iloc[0])