package model

import (
	"encoding/json"
	"errors"
	"time"
)

type Event struct {
	Sequence  uint64          `gorm:"primaryKey" json:"sequence"`
	UserID    string          `gorm:"size:100;not null" json:"user_id"`
	EventType string          `gorm:"size:50;not null" json:"event_type"`
	Amount    float64         `gorm:"type:decimal(10,2)" json:"amount"`
	Metadata  json.RawMessage `gorm:"type:json" json:"metadata"`
	CreatedAt time.Time       `gorm:"index" json:"created_at"`
}

func (e *Event) Validate() error {
	if e.UserID == "" {
		return errors.New("user_id is required")
	}
	return nil
}
