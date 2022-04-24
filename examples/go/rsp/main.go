package main

import (
	"fmt"
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
	sub, err := nc.Subscribe("foo.svc", func(nm *nats.Msg) {
		if nm.Reply != "" {
			rsp := fmt.Sprintf("response sent to %s on %s", nm.Reply, nm.Data)
			if err := nm.Respond([]byte(rsp)); err != nil {
				log.Fatal(err)
			}
		}
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
