package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
)

// timestampHolder is a lightweight struct used to extract
// potential timestamp fields from a log record without
// allocating a map.
type timestampHolder struct {
	Time      string `json:"time"`
	TimeStamp string `json:"timeStamp"`
}

// parseTimestamp tries to parse a timestamp from the given JSON data.
// It checks both `time` and `timeStamp` fields and returns the first
// successfully parsed value. If no valid timestamp exists, the zero
// value of time.Time is returned.
func parseTimestamp(data json.RawMessage) time.Time {
	var ts timestampHolder
	if err := json.Unmarshal(data, &ts); err != nil {
		return time.Time{}
	}
	for _, val := range []string{ts.Time, ts.TimeStamp} {
		if val == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t
		}
		fmt.Printf("failed to parse timestamp: %q\n", val)
	}
	return time.Time{}
}

const (
	contentType  = "application/x-protobuf"
	maxErrMsgLen = 1024
)

var (
	hubAddr         *eventhub.Hub
	promtailAddress *url.URL
	hubParse        *conn.ParsedConn
	httpClient      = &http.Client{Timeout: 5 * time.Second}
)

func init() {
	var err error
	ptaddr := os.Getenv("PROMTAIL_ADDRESS")
	if ptaddr == "" {
		panic(errors.New("required environment variable PROMTAIL_ADDRESS not present"))
	}

	promtailAddress, err = url.Parse(ptaddr)
	if err != nil {
		panic(err)
	}

	azaddr := os.Getenv("AZ_CONNECTION_STRING")
	if azaddr == "" {
		panic(errors.New("required environment variable AZ_CONNECTION_STRING not present"))
	}

	hubAddr, err = eventhub.NewHubFromConnectionString(azaddr)
	if err != nil {
		fmt.Println("Error creating EventHub: ", err)
	}

	hubParse, err = conn.ParsedConnectionFromStr(azaddr)
	if err != nil {
		fmt.Println("Error parsing connection string: ", err)
	}
}

func main() {
	handler := func(ctx context.Context, event *eventhub.Event) error {
		text := event.Data
		var result map[string][]json.RawMessage
		if err := json.Unmarshal(text, &result); err != nil {
			return err
		}

		labelSet := model.LabelSet{
			model.LabelName("__az_eventhub_namespace"): model.LabelValue(hubParse.Namespace),
			model.LabelName("__az_eventhub_hub"):       model.LabelValue(hubParse.HubName),
		}

		for _, records := range result {
			entries := make([]logproto.Entry, 0, len(records))

			for _, data := range records {
				ts := parseTimestamp(data)
				if ts.IsZero() {
					ts = time.Now()
				}
				entries = append(entries, logproto.Entry{
					Timestamp: ts,
					Line:      string(data),
				})
			}

			stream := logproto.Stream{
				Labels:  labelSet.String(),
				Entries: entries,
			}

			pushRequest := &logproto.PushRequest{
				Streams: []logproto.Stream{stream},
			}

			buf, err := proto.Marshal(pushRequest)
			if err != nil {
				return err
			}

			// Push to promtail
			buf = snappy.Encode(nil, buf)
			req, err := http.NewRequest("POST", promtailAddress.String(), bytes.NewReader(buf))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", contentType)

			resp, err := httpClient.Do(req.WithContext(ctx))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode/100 != 2 {
				scanner := bufio.NewScanner(io.LimitReader(resp.Body, maxErrMsgLen))
				line := ""
				if scanner.Scan() {
					line = scanner.Text()
				}
				fmt.Printf("server returned HTTP status %s (%d): %s\n", resp.Status, resp.StatusCode, line)
			}
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer hubAddr.Close(ctx)

	runtimeInfo, err := hubAddr.GetRuntimeInformation(ctx)
	if err != nil {
		fmt.Println("Error fetching runtime info: ", err)
		return
	}

	for _, partitionID := range runtimeInfo.PartitionIDs {
		_, err := hubAddr.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
		if err != nil {
			fmt.Println("Error receiving: ", err)
			return
		}
	}

	fmt.Println("Listening to Event Hub", hubParse.Namespace, hubParse.HubName)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan
}
