import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import os
import glob
import json
from datetime import datetime

datapoints = []

path = "./results"
for filename in glob.glob(os.path.join(path, "*.json")):
    with open(os.path.join(os.getcwd(), filename), "r") as f:
        parsed = json.loads(f.read())
        datapoints += parsed
df = pd.DataFrame(datapoints)
df = df.sort_values(by=['run_id'], ascending=False)
dataset = df["dataset_file"].iloc[0]

# Convert "run_id" to datetime
df["run_id"] = df["run_id"].apply(lambda x: datetime.fromtimestamp(int(x)))

# Create a list of unique "ef" values from the DataFrame
ef_values = df["ef"].unique()

sns.set_theme()
plt.figure(figsize=(12, 8))  # Adjust the figure size as needed

# Loop through each "ef" value and create a line plot
for ef_value in ef_values:
    ef_df = df[df["ef"] == ef_value]
    sns.lineplot(
        data=ef_df,
        x="run_id",
        y="recall",
        label=f"ef={ef_value}",
    )

plt.title(f"{dataset} recall with continual updates")
plt.xlabel("Time")
plt.ylabel("Recall")
plt.legend(title="EF Values", loc="upper right")

date_format = mdates.DateFormatter("%H:%M")
plt.gca().xaxis.set_major_formatter(date_format)

plt.savefig("output.png", bbox_inches='tight')

plt.show()  # Display the plot

