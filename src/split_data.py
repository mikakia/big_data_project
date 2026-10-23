import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("data/raw/star_classification.csv")
print("Total number of samples in the dataset:", len(df))
print(df.head())
print(df["class"].value_counts())



# Select a random 25% while keeping class proportions
subset = df.groupby("class", group_keys=False).sample(frac=0.25, random_state=42).reset_index(drop=True)

print(subset["class"].value_counts())
n= len(subset)
print("Total number of samples in the subset:",n)
# Split the subset into three different datasets: part1, part2, part3
master = subset.iloc[:len(subset)//3]
worker1 = subset.iloc[len(subset)//3:2*len(subset)//3]
worker2 = subset.iloc[2*len(subset)//3:]  


print("Master:",master["class"].value_counts())
print("Worker 1:",worker1["class"].value_counts())
print("Worker 2:",worker2["class"].value_counts())    

# Save the three parts to CSV files
master.to_csv("data/master/master.csv", index=False)
worker1.to_csv("data/worker1/worker1.csv", index=False)
worker2.to_csv("data/worker2/worker2.csv", index=False)
