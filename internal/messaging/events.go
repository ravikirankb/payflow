package messaging

// PaymentCreatedEvent represents the payment payload published
// by the outbox publisher to Kafka.
type PaymentCreatedEvent struct {
	ID        string `json:"ID"`
	Amount    int64  `json:"Amount"`
	Status    string `json:"Status"`
	Currency  string `json:"Currency"`
	CreatedAt string `json:"CreatedAt"`
}
