package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var globalConfig Config

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.TimeOnly})
	level := zerolog.InfoLevel
	if s, ok := os.LookupEnv("LOG_LEVEL"); ok {
		parsed, err := zerolog.ParseLevel(strings.ToLower(s))
		if err != nil || parsed == zerolog.NoLevel {
			log.Warn().Str("value", s).Msg("Invalid LOG_LEVEL, defaulting to info")
		} else {
			level = parsed
		}
	}
	zerolog.SetGlobalLevel(level)

	initRandomVectors()
	initRandomText()
	initDataset()
	initRaw()
	initAnnBenchmark()
	initColbert()
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
