package aws_utils

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/twinj/uuid"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/aws_utils/notifier"
)

var (
	useRealAWS = flag.Bool("realAWS", false, "Test against real sqs+sns resources")
)

type handler struct {
	msgs []*sqs.Message
}

func (h *handler) Handle(msg *sqs.Message) error {
	h.msgs = append(h.msgs, msg)
	return nil
}

func TestSqs(t *testing.T) {
	if *useRealAWS == false {
		t.Skip("Skipping test because --realAWS not specified")
	}
	const numMessages = 3
	client := sqs.New(session.New())
	queueName := "aws_utils_test_" + uuid.NewV4().String() // Get a random name for the queue

	_, err := client.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	log.Printf("SQS Queue %s created!", queueName)

	defer func() {
		o, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(queueName),
		})
		_, err = client.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: o.QueueUrl,
		})
		if err != nil {
			t.Errorf("Error deleting queue: %v", err)
		}
		log.Printf("SQS Queue %s destroyed!", queueName)
	}()

	n := notifier.BuildSQSClient(client)
	n.Signer.RegisterMessageType("number", func(args ...interface{}) (string, error) {
		if len(args) != 1 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"number\":%d}", args...), nil
	})

	h := &handler{}
	l := listener.BuildSQSListener(h, 1*time.Second, client, nil)
	if l == nil {
		t.Error("Unable to create SQSListener")
	}

	go func() {
		l.Listen(queueName)
	}()
	defer l.Close()

	for i := 0; i < numMessages; i++ {
		err = n.SendMessage("number", queueName, i)
		if err != nil {
			t.Errorf("Error trying to send message %d: %v", i, err)
		}
	}

	timeout := time.NewTicker(4 * time.Second)

	defer timeout.Stop()
RunLoop:
	for {
		select {
		case <-timeout.C:
			t.Error("Timeout waiting for sqs events")
			break RunLoop
		default:
		}
		if len(h.msgs) == numMessages {
			break RunLoop
		}
	}
}

func TestSns(t *testing.T) {
	if *useRealAWS == false {
		t.Skip("Skipping test because --realAWS not specified")
	}
	const numMessages = 3

	config := &aws.Config{Region: aws.String("us-west-2")}
	session := session.New(config)
	sqsClient := sqs.New(session)
	queueName := "aws_utils_test_" + uuid.NewV4().String() // Get a random name for the queue

	sqsOutput, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	sqsURL := sqsOutput.QueueUrl
	log.Printf("SQS Queue %s created!", queueName)
	qAttributes, err := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       sqsURL,
	})
	if err != nil {
		t.Errorf("Error getting queue's ARN: %v", err)
	}
	sqsARN := qAttributes.Attributes["QueueArn"]
	defer func() {
		_, err = sqsClient.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: sqsURL,
		})
		if err != nil {
			t.Errorf("Error deleting queue: %v", err)
		}
		log.Printf("SQS Queue %s destroyed!", queueName)
	}()

	snsClient := sns.New(session)
	snsOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(queueName),
	})
	if err != nil {
		t.Errorf("Error creating SNS topic: %s", err)
	}
	snsARN := snsOutput.TopicArn
	log.Printf("SNS Topic %s created!", *snsARN)

	defer func() {
		_, err := snsClient.DeleteTopic(&sns.DeleteTopicInput{
			TopicArn: snsARN,
		})
		if err != nil {
			t.Errorf("Error deleting sns topic: %v", err)
		}
		log.Printf("SNS Topic %s destroyed!", *snsARN)
	}()

	_, err = sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl: sqsURL,
		Attributes: map[string]*string{
			"Policy": aws.String(`
		{
		  "Version":"2012-10-17",
		  "Statement":[
		    {
		      "Effect":"Allow",
		      "Principal": {
				  "AWS": "*"
			  },
		      "Action":"SQS:SendMessage",
		      "Resource":"` + *sqsARN + `",
		      "Condition":{
		        "ArnEquals":{
		          "aws:SourceArn":"` + *snsARN + `"
		        }
		      }
		    }
		  ]
		}`),
		},
	})
	if err != nil {
		t.Errorf("Error setting policy on sqs queue: %v", err)
	}
	subOut, err := snsClient.Subscribe(&sns.SubscribeInput{
		TopicArn: snsARN,
		Protocol: aws.String("sqs"),
		Endpoint: sqsARN,
	})
	if err != nil {
		t.Errorf("Error subscribing sqs to sns: %v", err)
	}
	if subOut.SubscriptionArn == nil {
		t.Errorf("Expected automatic subscription of the sqs queue to the sns topic, but no subscription arn was returned.")
	}
	_, err = snsClient.SetSubscriptionAttributes(&sns.SetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	if err != nil {
		t.Errorf("Error setting raw message delivery setting on the SNS topic: %v", err)
	}

	n := notifier.BuildSNSClient(snsClient)
	n.Signer.RegisterMessageType("number", func(args ...interface{}) (string, error) {
		if len(args) != 1 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"number\":%d}", args...), nil
	})

	h := &handler{}
	l := listener.BuildSQSListener(h, 1*time.Second, sqsClient, nil)
	if l == nil {
		t.Error("Unable to create SQSListener")
	}

	go func() {
		l.Listen(queueName)
	}()
	defer l.Close()

	for i := 0; i < numMessages; i++ {
		err = n.SendMessage("number", *snsARN, i)
		if err != nil {
			t.Errorf("Error trying to send message %d: %v", i, err)
		}
	}

	timeout := time.NewTicker(4 * time.Second)

	defer timeout.Stop()
RunLoop:
	for {
		select {
		case <-timeout.C:
			t.Error("Timeout waiting for sqs events")
			break RunLoop
		default:
		}
		if len(h.msgs) == numMessages {
			break RunLoop
		}
	}
}
