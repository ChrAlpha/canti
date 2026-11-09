// canti-job is the one-shot capture helper used by prepared environments.
package main

import (
	"flag"
	"fmt"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
	"os"
)

func main() { os.Exit(run(os.Args[1:])) }
func run(args []string) int {
	fs := flag.NewFlagSet("canti-job", flag.ContinueOnError)
	var c jobrunner.Config
	version := fs.Bool("version", false, "print helper protocol")
	fs.StringVar(&c.Directory, "directory", "", "command evidence directory")
	fs.StringVar(&c.EffectID, "effect", "", "accepted effect ID")
	fs.StringVar(&c.Spec, "spec", "", "accepted specification")
	fs.Uint64Var(&c.Generation, "generation", 0, "command generation")
	fs.Int64Var(&c.OutputLimit, "output-limit", 8<<20, "retained output bytes")
	fs.IntVar(&c.PreviewLimit, "preview-limit", 4096, "preview bytes")
	fs.StringVar(&c.Workdir, "workdir", "", "directory the command shell enters")
	fs.StringVar(&c.Init, "init", "", "optional shell initialization file")
	if err := fs.Parse(args); err != nil {
		return 125
	}
	if *version {
		fmt.Println(jobrunner.Protocol)
		return 0
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "canti-job: exactly one command is required")
		return 125
	}
	c.Command = fs.Arg(0)
	rec, err := jobrunner.Run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "canti-job:", err)
		return 125
	}
	return *rec.ExitCode
}
