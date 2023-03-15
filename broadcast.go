package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	storage := make(map[int]bool)

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s := fmt.Sprintf("%v", body["message"])
		num, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}

		storage[num] = true

		delete(body, "message")

		for _, node := range n.NodeIDs() {
			message := map[string]any{"type": "node_broadcast", "message": num}
			if n.ID() != node {
				n.Send(node, message)
			}
		}

		body["type"] = "broadcast_ok"
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		var arr []int
		for k, _ := range storage {
			arr = append(arr, k)
		}
		body["messages"] = arr

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	n.Handle("node_broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s := fmt.Sprintf("%v", body["message"])
		num, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
			return err
		}

		storage[num] = true

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// ./maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
