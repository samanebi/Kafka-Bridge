package main

import (
	"context"
	"kafak-bridge/internal/broker"
	"kafak-bridge/internal/database"
	"kafak-bridge/internal/services"
	config "kafak-bridge/pkg/configs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load("./pkg/configs")

	switch cfg.Logging.Level {
	case "debug":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case "info":
		slog.SetLogLoggerLevel(slog.LevelInfo)

	}

	slog.Info("Starting KafkaFlow Consumer...")
	slog.Info("Configuration loaded", "environment", cfg.App.Environment, "workers", cfg.App.WorkerCount)

	dbRepo, err := database.NewPostgresRepository(cfg.GetDSN())
	if err != nil {
		slog.Error("Failed to connect to database:", "error", err)

		os.Exit(1)
	}

	slog.Info("Database connection established")

	messageChan := make(chan kafka.Message)
	consumer := broker.NewKafkaConsumer(cfg.GetKafkaBrokers(), cfg.Kafka.Topic, cfg.Kafka.GroupID, messageChan)
	eventService := services.NewEventService(dbRepo, consumer)
	workerPool := services.NewWorkerPool(messageChan, eventService, cfg.App.WorkerCount)

	slog.Info("Kafka consumer initialized",
		"topic", cfg.Kafka.Topic,
		"brokers", cfg.GetKafkaBrokers())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPool.Start(ctx)
	slog.Info("Worker pool started", "worker_count", cfg.App.WorkerCount)

	go func() {
		if err := consumer.StartConsuming(ctx); err != nil {
			slog.Error("Error in consumer:", "error", err)

			cancel()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Consumer is running. Press Ctrl+C to stop.")
	<-sigChan
	slog.Info("Received shutdown signal")

	cancel()

	shutdownTimeout := time.Duration(cfg.App.RetryDelayMs) * time.Millisecond * 2
	slog.Info("Waiting for in-flight messages to complete", "timeout", shutdownTimeout)

	select {
	case <-time.After(shutdownTimeout):
		slog.Warn("Shutdown timeout reached, forcing shutdown")
	case <-workerPool.Stop():
		slog.Info("All workers finished processing")
	}

	workerPool.ForceClose()
	err = consumer.Close()
	if err != nil {
		slog.Error("Error closing KafkaFlow consumer:", "error", err)

		os.Exit(1)
	}

	slog.Info("Application shutdown completed")
}
