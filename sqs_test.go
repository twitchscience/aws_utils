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
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/twinj/uuid"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/aws_utils/notifier"
)

var (
	useRealSQS = flag.Bool("realSqs", false, "Test against real sqs resources")
)

type handler struct {
	msgs []*sqs.Message
}

func (h *handler) Handle(msg *sqs.Message) error {
	h.msgs = append(h.msgs, msg)
	return nil
}

func TestSqs(t *testing.T) {
	if *useRealSQS == false {
		t.Skip("Skipping test because --realSqs not specified")
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
	}()

	n := notifier.BuildSQSClient(client)
	n.Signer.RegisterMessageType("number", func(args ...interface{}) (string, error) {
		if len(args) != 1 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"number\":%d}", args...), nil
	})

	h := &handler{}
	l := listener.BuildSQSListener(h, 1*time.Second, client)
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
