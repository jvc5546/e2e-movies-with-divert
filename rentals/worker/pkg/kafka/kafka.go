package kafka

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/okteto/movies/pkg/database"
)

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	ctx          context.Context
	MessageCount int
	namespace    string
	divertKey    string
	db           *sql.DB
	cg           sarama.ConsumerGroup
}

func NewConsumerGroup(ctx context.Context, namespace string, divertKey string, addrs []string, db *sql.DB) (*ConsumerGroupHandler, error) {
	// Create consumer group ID with namespace suffix
	consumerGroupID := fmt.Sprintf("movies-worker-group-%s", namespace)

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Enable manual commit - we'll commit only after successful API calls
	config.Consumer.Offsets.AutoCommit.Enable = false

	consumerGroup, err := sarama.NewConsumerGroup(addrs, consumerGroupID, config)
	if err != nil {
		return nil, err
	}

	handler := &ConsumerGroupHandler{
		ctx:          ctx,
		MessageCount: 0,
		namespace:    namespace,
		divertKey:    divertKey,
		db:           db,
		cg:           consumerGroup,
	}

	return handler, nil
}

func (c *ConsumerGroupHandler) Close() {
	c.cg.Close()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Consume(topics []string) error {
	return h.cg.Consume(h.ctx, topics, h)
}

func (c *ConsumerGroupHandler) shouldProcessMessage(baggage string) bool {
	// Extract okteto-divert value from baggage
	divertValue := extractOktetoDivertFromBaggage(baggage)

	// Rule 1: If message has okteto-divert key, process only if value matches environment variable
	if divertValue != "" {
		return divertValue == c.divertKey
	}

	// Rule 2: If message doesn't have okteto-divert key, process only if environment variable is empty
	return c.divertKey == ""

	// Rule 3: If this doesn't belong to anybody else, the 'shared' should get it
}

// extractOktetoDivertFromBaggage parses baggage string and extracts okteto-divert value
func extractOktetoDivertFromBaggage(baggage string) string {
	if baggage == "" {
		return ""
	}

	// Parse baggage format: "key1=value1,key2=value2,..."
	pairs := strings.Split(baggage, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == "okteto-divert" {
			return strings.TrimSpace(kv[1])
		}
	}

	return ""
}

// extractHeader extracts a named header's value from Kafka message headers.
// Used for "baggage" (divert routing), "title" and "request-id" (both purely
// for log correlation — see ConsumeClaim).
func extractHeader(headers []*sarama.RecordHeader, key string) string {
	for _, header := range headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Extract headers once for the message. "title" and "request-id" carry
		// no routing logic — they only exist so a log line on one worker can be
		// correlated with the matching accept/skip line on every other worker
		// consuming the same shared topic.
		baggageHeader := extractHeader(message.Headers, "baggage")
		title := extractHeader(message.Headers, "title")
		requestID := extractHeader(message.Headers, "request-id")

		// Check if we should process this message based on divert logic
		if !h.shouldProcessMessage(baggageHeader) {
			target := extractOktetoDivertFromBaggage(baggageHeader)
			log.Printf("[req=%s] Not processing %q (target=%s) — it belongs to a diverted worker", requestID, title, target)
			continue
		}

		h.MessageCount++

		// Determine message type based on topic
		if message.Topic == "rentals" {
			if !h.processRentalMessage(string(message.Key), string(message.Value), title, requestID) {
				// Don't commit if processing failed
				log.Printf("[req=%s] Failed to process rental message, will retry on next poll", requestID)
				continue
			}
		} else if message.Topic == "returns" {
			if !h.processReturnMessage(string(message.Value), title, requestID) {
				// Don't commit if processing failed
				log.Printf("[req=%s] Failed to process return message, will retry on next poll", requestID)
				continue
			}
		}

		// Only mark message as consumed if processing was successful
		session.MarkMessage(message, "")
		// Commit the offset immediately after successful processing
		session.Commit()
	}
	return nil
}

// processRentalMessage handles rental messages and returns true if successful
func (h *ConsumerGroupHandler) processRentalMessage(movieID string, priceStr string, title string, requestID string) bool {
	fmt.Printf("[req=%s] Received message: %q (movie %s) price %s\n", requestID, title, movieID, priceStr)

	if err := database.CreateOrUpdateRental(h.db, movieID, priceStr, h.namespace, title, requestID); err != nil {
		log.Printf("[req=%s] Error processing the rental request: %v", requestID, err)
		return false
	}

	fmt.Printf("[req=%s] Successfully created/updated rental: %q (movie %s) - message committed\n", requestID, title, movieID)
	return true
}

// processReturnMessage handles return messages and returns true if successful
func (h *ConsumerGroupHandler) processReturnMessage(catalogID string, title string, requestID string) bool {
	fmt.Printf("[req=%s] Received return message: %q (catalogID %s)\n", requestID, title, catalogID)

	if err := database.DeleteRental(h.db, catalogID, title, requestID); err != nil {
		log.Printf("[req=%s] Error processing the delete rental request: %v", requestID, err)
		return false
	}

	fmt.Printf("[req=%s] Successfully deleted rental: %q (catalogID %s) - message committed\n", requestID, title, catalogID)
	return true
}
