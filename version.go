package main

import "fmt"

var version = "0.0.5"

var (
	commit    = ""
	branch    = ""
	tag       = ""
	buildInfo = ""
	date      = ""
)

func printVersion() {
	fmt.Printf("%s (commit='%s', branch='%s', tag='%s', date='%s', build='%s')", version, commit, branch, tag, date, buildInfo)
}
