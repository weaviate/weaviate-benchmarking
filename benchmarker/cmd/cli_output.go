package cmd

import (
	"fmt"
	"os"
)

const (
	colorReset = "\033[0m"
	colorRed   = "\033[1;31m"
	colorWhite = "\033[0;37m"
)

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "%s%s%s\n", colorRed, err.Error(), colorReset)
	os.Exit(1)
}

func infof(msg string, format ...interface{}) {
	formatted := fmt.Sprintf(msg, format...)
	fmt.Fprintf(os.Stderr, "%s%s%s\n", colorWhite, formatted, colorReset)
}
