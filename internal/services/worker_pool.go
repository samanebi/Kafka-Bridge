package services

import (
	"context"
	"encoding/json"
	"kafak-bridge/internal/model"
	"log"
	"log/slog"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	Event     *model.Event
	Topic     string
	Partition int
	Offset    int64
}

type WorkerPool struct {
	workers      int
	messageChan  chan kafka.Message
	eventService *EventService
	wg           sync.WaitGroup
}

func NewWorkerPool(messageChan chan kafka.Message, eventService *EventService, workers int) *WorkerPool {
	return &WorkerPool{
		workers:      workers,
		messageChan:  messageChan,
		eventService: eventService,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-wp.messageChan:
			if !ok {
				return
			}

			var event model.Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Error unmarshaling message: %v", err)

				continue
			}

			if err := wp.eventService.ProcessEvent(ctx, &event, msg); err != nil {
				slog.Error("processing event failed", slog.Any("err", err), slog.String("topic", msg.Topic), slog.Int("partition", msg.Partition), slog.Int64("offset", msg.Offset))
			} else {
				slog.Info("processing event success", slog.Any("err", err), slog.String("topic", msg.Topic), slog.Int("partition", msg.Partition), slog.Int64("offset", msg.Offset))
			}
		}
	}
}

func (wp *WorkerPool) Stop() chan struct{} {
	close(wp.messageChan)
	result := make(chan struct{})
	go wp.close(result)

	return result
}

func (wp *WorkerPool) close(result chan struct{}) {
	wp.wg.Wait()

	result <- struct{}{}
}

func (wp *WorkerPool) ForceClose() {
	wp.wg.Wait()
}
