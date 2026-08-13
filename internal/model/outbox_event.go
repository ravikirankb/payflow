package model

import "time"

type OutboxEvent struct {
	ID          string
	EventType   string
	AggregateID string
	Payload     []byte
	CreatedAt   time.Time
}
