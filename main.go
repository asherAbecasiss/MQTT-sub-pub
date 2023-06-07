package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func buildFileName() string {
	return time.Now().Format("2006-01-02-15:04:05")
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())

	filenamePrefix := filepath.Join("./data", buildFileName())
	str := strings.Replace(msg.Topic(), "/", "_", -1)

	filenamePrefix += str
	filenamePrefix += ".json"

	err := ioutil.WriteFile(filenamePrefix, msg.Payload(), 0644)
	if err != nil {
		log.Fatal(err)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}

func main() {
	var broker = ""
	var port = 18883
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtt+ssl://%s:%d", broker, port))
	opts.SetClientID("go_mqtt_client")
	opts.SetUsername("")
	opts.SetPassword("")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	sub(client)
	publish(client)

	time.Sleep(time.Minute * 10)

	client.Disconnect(250)
}

func publish(client mqtt.Client) {
	num := 10
	for i := 0; i < num; i++ {
		text := fmt.Sprintf("Message %d", i)
		token := client.Publish("topic/test", 0, false, text)
		token.Wait()
		time.Sleep(time.Second)
	}
}

func sub(client mqtt.Client) {
	
	t := make(map[string]byte)

	t[""] = 1
	t[""] = 1
	t[""] = 1

	
	token := client.SubscribeMultiple(t, nil)
	//token := client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", topic)
}
