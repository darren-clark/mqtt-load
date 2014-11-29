package main

import (
	"fmt"
	"flag"
	"os"
	"bufio"
	"strings"
	"strconv"
	"time"
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)


var server string;

func publish(process int, messageCount int, qos MQTT.QoS, c chan interface{}){
	opts := MQTT.NewClientOptions().AddBroker(server).SetCleanSession(false)
	client := MQTT.NewClient(opts);
	_,err := client.Start();
	if err != nil{
		c <- err
		return
	}
	c <- process
	fmt.Println("Process %d has connected.",process);
	for i := 0;i<messageCount;i++{
		topic := "loadtest/"+string(process)
		r := client.Publish(qos,topic,"aaaaa")
		<-r
	}
	client.Disconnect(uint(0))
	fmt.Println("Process %d completed sending %d messages.",process,messageCount);
}

func main() {
	var count int;
	var messages int;
	flag.StringVar(&server,"h","tcp://localhost:1883","Host name")
	flag.IntVar(&count,"c",1,"Client count")
	flag.IntVar(&messages,"n",100,"Message Count")
	flag.Parse();

	fmt.Println("Starting subscriber")
	opts := MQTT.NewClientOptions().AddBroker(server).SetCleanSession(false)
	client := MQTT.NewClient(opts);
	client.Start();
	fmt.Println("Subscriber connected")
	filter,_ := MQTT.NewTopicFilter("loadtest/#",byte(1))
	client.StartSubscription(func(client *MQTT.MqttClient, msg MQTT.Message){
			process,_ :=  strconv.Atoi(strings.Split(msg.Topic(),"/")[1])
			fmt.Println("Received %s from %d",string(msg.Payload()),process);
		},filter)

	c := make(chan int,count);
	for i:=0;i<count;i++{
		go publish(i,messages,MQTT.QOS_ONE,c);
	}
	stdin := bufio.NewReader(os.Stdin);
	stdin.ReadLine();
	client.Disconnect(uint(0));
	time.Sleep(1000 * time.Millisecond);
}
