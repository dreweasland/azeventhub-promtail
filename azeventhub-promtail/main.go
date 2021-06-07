package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

var hubAddr *eventhub.Hub
var hubParse *conn.ParsedConn

func init() {
	var err error
	azaddr := os.Getenv("AZ_CONNECTION_STRING")
	if azaddr == "" {
		panic(errors.New("required environmental variable AZ_CONNECTION_STRING not present"))
	}

	hubAddr, err = eventhub.NewHubFromConnectionString(azaddr)
	if err != nil {
		fmt.Println("Error: ", err)
	}

	hubParse, err = conn.ParsedConnectionFromStr(azaddr)
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

func main() {
	handler := func(ctx context.Context, event *eventhub.Event) error {
		text := event.Data
		fmt.Println(text)
		var result map[string][]interface{}
		err := json.Unmarshal(text, &result)
		if err != nil {
			return err
		}
		for _, m := range result {
			for _, data := range m {
				remarshalledJSON, _ := json.Marshal(&data)
				fmt.Println(remarshalledJSON)
			}
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	runtimeInfo, err := hubAddr.GetRuntimeInformation(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, partitionID := range runtimeInfo.PartitionIDs {
		_, err := hubAddr.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
	}
	cancel()

	fmt.Println("Listening to Event Hub", hubParse.Namespace, hubParse.HubName)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan
}
