package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	log.SetFlags(0)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err := nc.Publish("foo", []byte(fmt.Sprintf("msg no %d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	nc.Flush()
}
