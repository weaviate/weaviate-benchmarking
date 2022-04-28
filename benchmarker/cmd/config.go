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
	// validate common
	if c.Origin == "" {
		return errors.Errorf("origin must be set")
	}

	switch c.API {
	case "graphql", "nearvector":
	default:
		return errors.Errorf("unsupported API %q", c.API)
	}

	// validate specific
	switch c.Mode {
	case "random-vectors":
		return c.validateRandomVectors()
	case "random-text":
		return c.validateRandomText()
	default:
		return errors.Errorf("unrecognized mode %q", c.Mode)

	}
}

func (c Config) validateRandomText() error {
	if c.ClassName == "" {
		return errors.Errorf("className must be set\n")
	}

	return nil
}

func (c Config) validateRandomVectors() error {
	if c.ClassName == "" {
		return errors.Errorf("className must be set\n")
	}

	if c.Dimensions == 0 {
		return errors.Errorf("dimenstions must be set and larger than 0\n")
	}

	return nil
}
