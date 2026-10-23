import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("data/raw/star_classification.csv")
print(df.head())
print(df["class"].value_counts())



# Select a random 25% while keeping class proportions
subset = df.groupby("class", group_keys=False).sample(frac=0.25, random_state=42).reset_index(drop=True)

print(subset["class"].value_counts())
