package main

import (
	"flag"
	"log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func resolveQueueUrl(queueName string, svc *sqs.SQS) (error, string) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	resp, err := svc.GetQueueUrl(params)

	if err != nil {
		return err, ""
	}

	return nil, *resp.QueueUrl
}

func main() {

	var (
		sourceQueueName = flag.String("source", "", "Source queue name")
		destQueueName   = flag.String("dest", "", "Destination queue name")
	)

	flag.Parse()

	ses := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := sqs.New(ses)

	err, sourceUrl := resolveQueueUrl(*sourceQueueName, svc)
	if err != nil {
		log.Fatalf("Fatal: %s", err)
	}

	err, destUrl := resolveQueueUrl(*destQueueName, svc)

	if err != nil {
		log.Fatalf("Fatal: %s", err)
	}

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(sourceUrl), // Required
		VisibilityTimeout:   aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(1),
		MaxNumberOfMessages: aws.Int64(10),
	}

	for {
		log.Println("Starting new batch")

		resp, err := svc.ReceiveMessage(params)

		if len(resp.Messages) == 0 {
			log.Println("Batch doesn't have any messages, transfer complete")
			return
		}

		if err != nil {
			// Print the error, cast err to awserr. Error to get the Code and
			// Message from an error.
			log.Fatalln(err.Error())
			return
		}

		log.Printf("Messages to transfer: %s", resp.Messages)

		batch := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(destUrl),
			Entries:  convertToEntries(resp.Messages),
		}

		sendResp, err := svc.SendMessageBatch(batch)

		if err != nil {
			log.Println("Failed to unqueue messages to the destination queue")
			log.Fatalln(err.Error())
		}

		if len(sendResp.Failed) > 0 {
			log.Println("Failed to unqueue messages to the destination queue")
			log.Fatalln(sendResp.Failed)
			return
		}

		log.Println("Unqueued to destination the following: ")
		log.Println(sendResp.Successful)

		if len(sendResp.Successful) == len(resp.Messages) {
			deleteMessageBatch := &sqs.DeleteMessageBatchInput{
				Entries:  convertSuccessfulMessageToBatchRequestEntry(resp.Messages),
				QueueUrl: aws.String(sourceUrl),
			}

			deleteResp, err := svc.DeleteMessageBatch(deleteMessageBatch)

			if err != nil {
				log.Fatalln("Error deleting messages, exiting...")
			}

			if len(deleteResp.Failed) > 0 {
				log.Println("Error deleting messages, the following were not deleted")
				log.Fatalln(deleteResp.Failed)
			}

			log.Printf("Deleted: %d messages \n", len(deleteResp.Successful))
			log.Println("========================")
		}
	}

}

func convertToEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	result := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		result[i] = &sqs.SendMessageBatchRequestEntry{
			MessageBody: message.Body,
			Id:          message.MessageId,
		}
	}

	return result
}

func convertSuccessfulMessageToBatchRequestEntry(messages []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	result := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		result[i] = &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
	}

	return result
}
