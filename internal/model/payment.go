package model

import "time"

type Payment struct {
	ID        string
	Amount    int64
	Currency  string
	Status    string
	CreatedAt time.Time
}
