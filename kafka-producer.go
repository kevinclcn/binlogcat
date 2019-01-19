package main

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

func newAsyncProducer(brokerList string) sarama.AsyncProducer {

	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		log.Error().Msgf("Failed to start Sarama producer:%v", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Error().Msgf("Failed to write access log entry:%v", err)
		}
	}()

	return producer
}
