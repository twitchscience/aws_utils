package notifier

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twitchscience/aws_utils/mocks"
)

func TestMessageRegister(t *testing.T) {
	client := BuildSQSClient(&mocks.SQS{})
	client.Signer.RegisterMessageType("test", func(args ...interface{}) (string, error) {
		if len(args) < 3 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"version\":%d,\"keyname\":%q,\"size\":%d}", args...), nil
	})
	expected := "{\"version\":0,\"keyname\":\"TestKey\",\"size\":500}"
	actual, _ := client.Signer.SignBody("test", 0, "TestKey", 500)
	if actual != expected {
		t.Logf("expected %s but got %s", expected, actual)
		t.Fail()
	}
	actual, err := client.Signer.SignBody("test", 0, "TestKey")
	if err == nil {
		t.Logf("expected %s but got %s", expected, actual)
		t.Fail()
	}
}

func TestSendMessage(t *testing.T) {
	client := BuildSQSClient(&mocks.SQS{})
	client.Signer.RegisterMessageType("testMessage", func(args ...interface{}) (string, error) {
		if len(args) != 0 {
			return "", errors.New("Missing correct number of args ")
		}
		return "{}", nil
	})

	err := client.SendMessage("testMessage", "testQeueue")
	if err != nil {
		t.Fail()
	}
}
