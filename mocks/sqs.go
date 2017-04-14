/*
Package mocks provides SQS interface mocks for testing.
*/
package mocks

import (
	"crypto/md5"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// SQS is an implmentation of a mock sqs interface
type SQS struct {
	sqsiface.SQSAPI

	lastSent string
}

// GetQueueUrl mocks the SQS GetQueueUrl method and just prefixes the queueName with "local://"
func (s *SQS) GetQueueUrl(in *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	url := "local://" + aws.StringValue(in.QueueName)
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(url),
	}, nil
}

// SendMessage mocks the SQS SendMessage by storing the message body in the mock object
// and calculating the message body MD5
func (s *SQS) SendMessage(in *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	s.lastSent = aws.StringValue(in.MessageBody)

	hasher := md5.New()
	hasher.Write([]byte(aws.StringValue(in.MessageBody)))
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	return &sqs.SendMessageOutput{
		MD5OfMessageBody: aws.String(hash),
	}, nil
}
