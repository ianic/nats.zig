package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	log.SetFlags(0)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		req := []byte(fmt.Sprintf("msg no %d", i))
		nm, err := nc.Request("foo.svc", req, time.Second)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s", nm.Data)
	}
	nc.Flush()
}
