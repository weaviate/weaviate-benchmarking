#!/usr/bin/env python3

import os
import glob
import json
import argparse
import seaborn as sns
import matplotlib.ticker as tkr
import matplotlib.pyplot as plt
import pandas as pd

EF_CONSTRUCTION = 256
MAX_CONNECTIONS = 32
RUNS = {
    "hnsw",
}

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
        (df["dataset_file"] == dataset)
        & (df['run'].isin(RUNS))
        & (df["maxConnections"] == MAX_CONNECTIONS)
        & (df["efConstruction"] == EF_CONSTRUCTION)
        & (df.apply(custom_filter, axis=1))
    ]

def create_plot(results_df: pd.DataFrame, mode='light'):
    dataset = results_df["dataset_file"].iloc[0]
    
    # Set custom colors for limits
    color_map = {
        100: '#098f73',
        10: '#2b17e7'
    }
    
    # Configure plot style based on mode
    plt.style.use('default')
    plt.rcParams['font.family'] = ['Arial']
    
    # Create new figure
    fig, ax = plt.subplots(figsize=(10.5, 7))
    
    # Set colors based on mode
    if mode == 'dark':
        text_color = 'white'
        grid_color = '#333333'
        spine_color = '#444444'
        bg_color = '#000000'
    else:  # light mode
        text_color = 'black'
        grid_color = '#CCCCCC'
        spine_color = '#DDDDDD'
        bg_color = '#ffffff'
    
    # Configure plot background
    ax.set_facecolor(bg_color)
    fig.patch.set_facecolor(bg_color)
    
    # Plot lines for each limit value
    for limit in sorted(results_df['limit'].unique()):
        data = results_df[results_df['limit'] == limit]
        ax.plot(data['recall'], data['qps'], 
                color=color_map[limit],
                linewidth=1.5,
                marker='o',
                markersize=4,
                label=f'Limit: {limit}')
        for x, y, ef in zip(data['recall'], data['qps'], data['ef']):
            ax.annotate(f'ef={ef}',
                       xy=(x, y),
                       xytext=(3, 7),  # 5 points vertical offset
                       textcoords='offset points',
                       ha='center',  # horizontal alignment
                       va='bottom',  # vertical alignment
                       fontsize=9,
                       color=text_color)
    
    # Customize axes
    ax.set_xlabel('Recall', fontsize=11, fontweight="bold", labelpad=5, color=text_color)
    ax.set_ylabel('QPS', fontsize=11, fontweight="bold", labelpad=5, color=text_color)
    
    # Format axis ticks
    ax.set_xlim(left=None, right=1)
    ax.set_ylim(bottom=0, top=None)
    
    # Set tick colors
    ax.tick_params(colors=text_color, labelsize=10)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color(text_color)
    
    # Customize grid
    ax.grid(True, linestyle='--', alpha=0.7, color=grid_color)
    ax.set_axisbelow(True)
    
    # Add title
    plt.title(f"Query Performance {dataset.replace('.hdf5', '')} (efConstruction={EF_CONSTRUCTION}, maxConnections={MAX_CONNECTIONS})", 
              pad=20, 
              fontdict={'family': 'Arial', 
                   'weight': 'bold',
                   'size': 11},
              color=text_color)
    
    # Customize legend
    legend = ax.legend(
        loc='upper right',
        frameon=True,
        fancybox=True,
        framealpha=0,
        edgecolor=spine_color,
        fontsize=10
    )
    # Set legend text color
    plt.setp(legend.get_texts(), color=text_color)
    
    # Set spines visibility and color
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color(spine_color)
    
    # Adjust layout and save
    plt.tight_layout()
    mode_suffix = 'dark' if mode == 'dark' else 'light'
    plt.savefig(
        f"{dataset.split('.')[0]}-{mode_suffix}.png",
        dpi=300,
        bbox_inches='tight',
        transparent=False
    )
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collate ann results into markdown tables.")
    parser.add_argument('-d', '--dataset', required=True, help="The dataset file to filter by")
    parser.add_argument('-r', '--results', default="./results", help="The directory containing benchmark results")
    args = parser.parse_args()

    # Get the data
    data = get_datapoints(args.dataset, os.path.expanduser(args.results))
    
    # Create both light and dark mode versions
    create_plot(data, mode='light')
    create_plot(data, mode='dark')