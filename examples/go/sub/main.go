package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

func main() {
	log.SetFlags(0)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	sub, err := nc.Subscribe("foo", func(nm *nats.Msg) {
		log.Printf("%s", nm.Data)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	waitForInterupt()
}

func waitForInterupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}
