package cmd

import (
	"github.com/pkg/errors"
)

type Config struct {
	Mode         string
	Origin       string
	Queries      int
	QueriesFile  string
	Parallel     int
	Limit        int
	ClassName    string
	API          string
	Dimensions   int
	DB           string
	WhereFilter  string
	OutputFormat string
	OutputFile   string
}

func (c Config) Validate() error {
	if err := c.validateCommon(); err != nil {
		return err
	}

	// validate specific
	switch c.Mode {
	case "random-vectors":
		return c.validateRandomVectors()
	case "random-text":
		return c.validateRandomText()
	case "dataset":
		return c.validateDataset()
	default:
		return errors.Errorf("unrecognized mode %q", c.Mode)
	}
}

func (c Config) validateCommon() error {
	if c.Origin == "" {
		return errors.Errorf("origin must be set")
	}

	if c.ClassName == "" {
		return errors.Errorf("className must be set\n")
	}

	switch c.API {
	case "graphql", "nearvector":
	default:
		return errors.Errorf("unsupported API %q", c.API)
	}

	switch c.OutputFormat {
	case "text", "":
		c.OutputFormat = "text"
	case "json":
	default:
		return errors.Errorf("unsupported output format %q, must be one of [text, json]",
			c.OutputFormat)

	}

	return nil
}

func (c Config) validateRandomText() error {
	return nil
}

func (c Config) validateRandomVectors() error {
	if c.Dimensions == 0 {
		return errors.Errorf("dimensions must be set and larger than 0\n")
	}

	return nil
}

func (c Config) validateDataset() error {
	if c.QueriesFile == "" {
		return errors.Errorf("a queries input file must be provided")
	}

	return nil
}
