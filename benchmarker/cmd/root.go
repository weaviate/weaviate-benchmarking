package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var globalConfig Config

func init() {
	initRandomVectors()
	initRandomText()
	initDataset()
	initRaw()
}

var rootCmd = &cobra.Command{
	Use:   "benchmarker",
	Short: "Weaviate Benchmarker",
	Long:  `A Weaviate Benchmarker`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("running the root command, see help or -h for available commands\n")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
