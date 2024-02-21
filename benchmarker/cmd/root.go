package cmd

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var globalConfig Config

func init() {

	logLevel, ok := os.LookupEnv("LOG_LEVEL")

	if ok {
		// If the environment variable is set, parse it to set the log level
		level, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(level)
		} else {
			log.Warn("Invalid log level. Defaulting to Info level.")
			log.SetLevel(log.InfoLevel)
		}
	} else {
		// If the environment variable is not set, default to Info level
		log.SetLevel(log.InfoLevel)
	}

	initRandomVectors()
	initRandomText()
	initDataset()
	initRaw()
	initAnnBenchmark()
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
