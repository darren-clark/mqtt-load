package main

import (
	"fmt"
	"flag"
//	"os"
//	"bufio"
	"strings"
	"strconv"
	"time"
	"sync"
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)


var server string;
var cwg sync.WaitGroup;
var elapsed chan time.Duration;

func publish(process int, messageCount int, qos MQTT.QoS){
	opts := MQTT.NewClientOptions().AddBroker(server).SetCleanSession(false)
	willTopic := fmt.Sprintf("loadtest/complete/%v",process)
	opts.SetWill(willTopic,"error",MQTT.QOS_TWO,false)
	client := MQTT.NewClient(opts);
	fmt.Printf("Connecting client %v\n",process)
	_,err := client.Start();
	if err != nil{
		fmt.Printf("Error connecting client %v: %v\n",process,err);
		panic(err)
	}
	startTime := time.Now();
	defer func (){
		duration := time.Now().Sub(startTime)
		elapsed <- duration
		fmt.Printf("Process %v completed sending %v messages in %v.\n",process,messageCount,duration);
	}()
	cwg.Done();
	topic := fmt.Sprintf("loadtest/%d",process)
	for i := 0;i<messageCount;i++{
		r := client.Publish(qos,topic,"payload")
		<-r
	}
	r := client.Publish(MQTT.QOS_TWO,willTopic,"done");
	<-r
	client.Disconnect(uint(0))
}

func main() {
	var count int;
	var messages int;
	var qos int;
	flag.StringVar(&server,"h","tcp://localhost:1883","Host name")
	flag.IntVar(&count,"c",1,"Client count")
	flag.IntVar(&messages,"n",100,"Message Count")
	flag.IntVar(&qos,"q",0,"QoS")
	flag.Parse();

	fmt.Println("Starting subscriber")
	opts := MQTT.NewClientOptions().AddBroker(server).SetCleanSession(false)
	client := MQTT.NewClient(opts);
	_,err := client.Start();
	if (err != nil){
		fmt.Printf("Error connecting subscriber: %v\n",err)
		panic(err)
	}
	fmt.Println("Subscriber connected")
	startTime := time.Now()
	filter,_ := MQTT.NewTopicFilter("loadtest/+",byte(MQTT.QOS_TWO))
	receivedCounts := make(map[int]int)
	r,_ := client.StartSubscription(func(client *MQTT.MqttClient, msg MQTT.Message){
			process,_ :=  strconv.Atoi(strings.Split(msg.Topic(),"/")[1])
			receivedCounts[process]++
//			fmt.Printf("Received %v from %v at %v\n",string(msg.Payload()),process,time.Now());
		},filter)
	<-r
	filter,_ = MQTT.NewTopicFilter("loadtest/complete/+",byte(MQTT.QOS_TWO))
	var rwg sync.WaitGroup;
	rwg.Add(count)
	r,_ = client.StartSubscription(func(client *MQTT.MqttClient, msg MQTT.Message){
			process,_ :=  strconv.Atoi(strings.Split(msg.Topic(),"/")[2])
			receiveComplete := time.Now().Sub(startTime)
			fmt.Printf("Process %v received done in %v\n",process,receiveComplete)
			rwg.Done();
		},filter)
	<-r
	cwg.Add(count);
	elapsed = make(chan time.Duration,count);
	for i:=0;i<count;i++{
		go publish(i,messages,MQTT.QoS(qos));
	}
	//Wait for everything to be connected.
	cwg.Wait();
	connected := time.Now().Sub(startTime);
	fmt.Printf("Clients connected in %v\n",connected)
	startTime = time.Now()

	//Wait for all receiving to be done.
	rwg.Wait()
	fmt.Printf("Complete in %v\n",time.Now().Sub(startTime))
	for p,c := range receivedCounts{
		fmt.Printf("Recived %v messages for process %v\n",c,p);
	}
	client.Disconnect(uint(0));
	time.Sleep(1000 * time.Millisecond);
}
