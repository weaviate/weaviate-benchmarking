import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

import os, glob
import json


datapoints = []

path = "./results"
for filename in glob.glob(os.path.join(path, "*.json")):
    with open(os.path.join(os.getcwd(), filename), "r") as f:
        parsed = json.loads(f.read())
        datapoints += parsed
df = pd.DataFrame(datapoints)
df = df.sort_values(by=['run_id'], ascending=False)
dataset = df["dataset_file"].iloc[0]


#df = df[df["after_restart"] == "false"]

sns.set_theme()
plot = sns.relplot(
    height=7,
    aspect=1.2,
    data=df,
    markers=True,
    kind="line",
    x="recall",
    y="qps",
    hue="run_id",
    style="run_id"
    # style="cloud_provider",
    # style="shards",
    # size="size",
)

plot.set(title=f"{dataset} Query Performance")


plt.savefig("output.png", bbox_inches='tight')

print(df["recall"].idxmax)
