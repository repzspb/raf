package main

import "os"

func main() {
	logger := newLogger()
	if err := run(logger); err != nil {
		logger.Error("raf stopped", "error", err)
		os.Exit(1)
	}
}
