#!/usr/bin/env python3

import os
import glob
import json
import argparse
import seaborn as sns
import matplotlib.ticker as tkr
import matplotlib.pyplot as plt
import pandas as pd

efConstruction = 256
maxConnections = 32


def custom_filter(row):
    if row['limit'] == 100 and row['ef'] < 90:
        return False
    return True 


def get_datapoints(dataset:str, path: str):
    datapoints = []
    for filename in glob.glob(os.path.join(path, "*.json")):
        with open(os.path.join(os.getcwd(), filename), "r") as f:
            parsed = json.loads(f.read())
            datapoints += parsed[1:]
    df = pd.DataFrame(datapoints)
    return df[
        (df["dataset_file"] == dataset)     # filter for a specific dataset
        & (df['run'] == "hnsw")              # remove PQ/BQ/SQ results
        & (df["maxConnections"] == maxConnections)
        & (df["efConstruction"] == efConstruction)
        & (df.apply(custom_filter, axis=1))
    ]


def create_plot(results_df: pd.DataFrame):

    dataset = results_df["dataset_file"].iloc[0]

    sns.set_theme(
        style='whitegrid',
        font_scale=1.2,
        rc={
            # 'axes.grid': True,
            # 'savefig.transparent': True,
            # 'axes.facecolor': color,
            # 'figure.facecolor': color,
            # 'axes.edgecolor': color,
            # 'grid.color': color,
            # 'ytick.labelcolor': color,
            # 'xtick.labelcolor': color,
            }
    )
    plot = sns.relplot(
        linewidth=3,
        height=7,
        aspect=1.5,
        marker="o",
        dashes=False,
        data=results_df,
        kind="line",
        x="recall",
        y="qps",
        hue="limit",
        style="limit",
        palette=["b", "g"],
    )
    plot.set_axis_labels(
        x_var="Recall, [%]",
        y_var="QPS",
    )
    plot.figure.subplots_adjust(top=0.85)
    plot.figure.suptitle(
        f"Query Performance, {dataset}",
        weight="bold",
        
    )
    sns.move_legend(
        plot,
        "lower center",
        bbox_to_anchor=(.5, .84),
        ncol=3,
        title="Limit: ",
        frameon=False,
    )


    plot.axes[0][0].get_xaxis().set_major_formatter(tkr.FuncFormatter(lambda x, _: f'{x*100:.0f}'))
    plot.axes[0][0].get_yaxis().set_major_formatter(tkr.StrMethodFormatter('{x:,.0f}'))
    plt.savefig(f"{dataset.split('.')[0]}.png", bbox_inches='tight')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collate ann results into markdown tables.")
    parser.add_argument('-d', '--dataset', required=True, help="The dataset file to filter by")
    parser.add_argument('-r', '--results', default="./results", help="The directory containing benchmark results")
    args = parser.parse_args()

    create_plot(
        get_datapoints(args.dataset, os.path.expanduser(args.results)),
    )
