package repository

import (
	"context"
	"kafak-bridge/internal/model"
)

type EventRepository interface {
	Create(ctx context.Context, event *model.Event) error
	GetBySequence(ctx context.Context, sequence uint64) (*model.Event, error)
	List(ctx context.Context, page, size int) (PaginatedResponse, error)
	Exists(ctx context.Context, sequence uint64) (bool, error)
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

type PaginatedResponse struct {
	Events     []model.Event `json:"events"`
	Total      int           `json:"total"`
	Page       int           `json:"page"`
	Size       int           `json:"size"`
	TotalPages int           `json:"total_pages"`
	HasNext    bool          `json:"has_next"`
}
