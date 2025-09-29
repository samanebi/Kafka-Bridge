package main

import (
	"context"
	"kafak-bridge/internal/broker"
	config "kafak-bridge/pkg/configs"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.Load("./pkg/configs")
	producer := broker.NewKafkaProducer(cfg.GetKafkaBrokers(), cfg.Kafka.Topic)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Generating 100,000 sample events...")
	if err := producer.GenerateSampleEvents(ctx, 1); err != nil {
		log.Fatal("Failed to generate events:", err)
	}

	log.Println("Event generation completed")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Producer shutdown completed")
}
