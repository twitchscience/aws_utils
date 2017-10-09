package listener

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/logger"
)

type mockSQSClient struct {
	sqsiface.SQSAPI
}

func (m *mockSQSClient) DeleteMessage(msg *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return nil, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(msg *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	return nil, nil
}

type testSQSHandler struct {
	Called int
	Err    error
}

func (h *testSQSHandler) Handle(msg *sqs.Message) error {
	h.Called++
	return h.Err
}

func TestSQSListenerHandling(t *testing.T) {
	uniq := 1000
	runs := 3
	handler := &testSQSHandler{Called: 0}
	listener := BuildSQSListener(handler, time.Second, &mockSQSClient{}, nil)
	for r := 0; r < runs; r++ {
		for i := 0; i < uniq; i++ {
			s := fmt.Sprintf("%v", i)
			listener.handle(&sqs.Message{Body: &s}, nil)
		}
	}
	if handler.Called != runs*uniq {
		t.Errorf("expected %d but got %d", runs*uniq, handler.Called)
	}
}

func TestSQSListenerHandlingDedup(t *testing.T) {
	logger.Init("warn")
	uniq := 1000
	runs := 3
	handler := &testSQSHandler{Called: 0}
	filter := NewDedupSQSFilter(uniq, time.Hour)
	listener := BuildSQSListener(handler, time.Second, &mockSQSClient{}, filter)
	for r := 0; r < runs; r++ {
		for i := 0; i < uniq; i++ {
			s := fmt.Sprintf("%v", i)
			listener.handle(&sqs.Message{Body: &s}, nil)
		}
	}
	if handler.Called != uniq {
		t.Errorf("expected %d but got %d", uniq, handler.Called)
	}
}

func TestSQSListenerHandlingDedupHandleFailure(t *testing.T) {
	logger.Init("warn")
	uniq := 1000
	runs := 3
	handler := &testSQSHandler{Called: 0, Err: fmt.Errorf("failed")}
	filter := NewDedupSQSFilter(uniq, time.Hour)
	listener := BuildSQSListener(handler, time.Second, &mockSQSClient{}, filter)
	for r := 0; r < runs; r++ {
		for i := 0; i < uniq; i++ {
			s := fmt.Sprintf("%v", i)
			listener.handle(&sqs.Message{Body: &s, ReceiptHandle: &s}, nil)
		}
	}
	if handler.Called != runs*uniq {
		t.Errorf("expected %d but got %d", runs*uniq, handler.Called)
	}
}
