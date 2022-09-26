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

	var no = 0
	var ln = 0
	sub, err := nc.Subscribe("foo", func(nm *nats.Msg) {
		if ln != len(nm.Data) {
			log.Printf("%s", nm.Data)
		}
		ln = len(nm.Data) + 1
		if ln == 1024 {
			ln = 0
		}
		no++
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		log.Printf("number of messages %d", no)
	}()
	defer sub.Unsubscribe()

	waitForInterupt()
}

func waitForInterupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}
