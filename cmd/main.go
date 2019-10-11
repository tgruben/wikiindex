package main

import (
	"log"

	"github.com/tgruben/wikiindex"
		"github.com/jaffee/commandeer"
)

func main() {
	err := commandeer.Run(wikiindex.NewMain())
	if err != nil {
		log.Fatal(err)
	}
}
