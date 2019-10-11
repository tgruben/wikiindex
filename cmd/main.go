package main

import (
	"log"

	"github.com/tgruben/wikiindex"
)

func main() {
	m := wikiindex.NewMain()
	if err := m.Run(); err != nil {
		log.Fatal(err)
	}
}
