package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the canti version")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "canti: a stateless agent core and durable harness experiment")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Fprintln(os.Stdout, version)
		return
	}

	flag.Usage()
	os.Exit(2)
}
