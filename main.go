package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"fmt"
	"os"
)

const Endpoint = "https://ff0604dfa6ff.ngrok.io"

var sess *session.Session

type SubscribeRequest struct {
	Token        string
	TopicArn     string
	SubscribeURL string
}

type Message struct {
	MessageTime time.Time
}

type SNSMessage struct {
	Type      string
	Timestamp time.Time
	Message   string
}

var totalTime = 0
var calls = 0

func handler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)

	messageType := r.Header.Get("X-Amz-Sns-Message-Type")

	switch messageType {
	case "SubscriptionConfirmation":
		var subReq SubscribeRequest
		err := json.Unmarshal(body, &subReq)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		svc := sns.New(sess)

		confirmation, err := svc.ConfirmSubscription(&sns.ConfirmSubscriptionInput{
			Token:    aws.String(subReq.Token),
			TopicArn: aws.String(subReq.TopicArn),
		})

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(confirmation)

		break
	default:
		var message SNSMessage

		err := json.Unmarshal(body, &message)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var timeMesg Message

		err = json.Unmarshal([]byte(message.Message), &timeMesg)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		totalTime += int(time.Since(timeMesg.MessageTime).Milliseconds())
		calls++

		fmt.Println(time.Since(timeMesg.MessageTime))
	}

}

func httpServer() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file. (~/.aws/credentials).
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sns.New(sess)

	// Create an SNS topic

	createTopicResult, err := svc.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String("SFB-test-topic"),
	})
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Println(*createTopicResult.TopicArn)

	// Subscribe to the topic

	go httpServer()

	_, err = svc.Subscribe(&sns.SubscribeInput{
		Endpoint:              aws.String(Endpoint),
		Protocol:              aws.String("https"),
		ReturnSubscriptionArn: aws.Bool(true), // Return the ARN, even if user has yet to confirm
		TopicArn:              createTopicResult.TopicArn,
	})

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Publish to the topic

	for i := 0; i < 1000; i++ {
		message := Message{
			time.Now(),
		}
		jsonMsg, err := json.Marshal(&message)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		input := &sns.PublishInput{
			Message:  aws.String(string(jsonMsg)),
			TopicArn: createTopicResult.TopicArn,
		}
		_, err = svc.Publish(input)

		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	fmt.Printf("Average time = %d\n", (totalTime / calls))

	fmt.Println("DONE")
}
