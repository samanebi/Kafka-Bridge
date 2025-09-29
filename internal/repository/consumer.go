package repository

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type ConsumerRepository interface {
	CommitOffset(ctx context.Context, msg kafka.Message) error
	GetLastOffset() int64
}
