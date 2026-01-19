import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

import os, glob
import json
import argparse


def main():
    parser = argparse.ArgumentParser(description="Visualize benchmark results")
    parser.add_argument("--title", help="Custom title for the plot")
    parser.add_argument("--subtitle", help="Custom subtitle for the plot")
    parser.add_argument("--control-branch", help="Control branch name")
    parser.add_argument("--candidate-branch", help="Candidate branch name")
    args = parser.parse_args()

    datapoints = []

    path = "./results"
    for filename in glob.glob(os.path.join(path, "*.json")):
        with open(os.path.join(os.getcwd(), filename), "r") as f:
            parsed = json.loads(f.read())
            datapoints += parsed
    df = pd.DataFrame(datapoints)
    df = df.sort_values(by=["run_id"], ascending=False)
    dataset = df["dataset_file"].iloc[0]

    # df = df[df["after_restart"] == "false"]

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
        style="run_id",
        # style="cloud_provider",
        # style="shards",
        # size="size",
    )

    # Set title and subtitle
    if args.title:
        # Custom title provided - title is the main heading, subtitle is the parameters
        plot.figure.suptitle(args.title, fontsize=16, fontweight="bold", y=1.02)
        if args.subtitle:
            # Subtitle goes below the main title
            plot.figure.text(
                0.5, 0.95, args.subtitle, ha="center", fontsize=10, style="italic"
            )
        # Add extra space at the top for titles
        plt.subplots_adjust(top=0.88)
    else:
        # Default behavior - just use dataset name
        plot.set(title=f"{dataset} Query Performance")

    # Add branch information at the bottom if provided
    if args.control_branch and args.candidate_branch:
        branch_text = f"control: {args.control_branch}  |  candidate: {args.candidate_branch}"
        plot.figure.text(
            0.5, -0.02, branch_text, ha="center", fontsize=9, color="gray"
        )
        # Add extra space at the bottom for branch info
        plt.subplots_adjust(bottom=0.15)

    plt.savefig("output.png", bbox_inches="tight")

    print(df["recall"].idxmax)


if __name__ == "__main__":
    main()
