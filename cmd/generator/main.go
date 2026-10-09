package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CaueTech/metron-engine/internal/application/generator"
	"github.com/CaueTech/metron-engine/internal/infra/kafka"
)

var (
	processLog = "generator"
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback

	/*
		This if block may seem strange, but it is a Go technique called a simple statement. It first executes value, exists := os.LookupEnv() and then checks the condition using exists.
	*/
}

func main() {
	rawBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "events")

	brokers := strings.Split(rawBrokers, ",")
	// This strings.Split() simulates a case where there are multiple Kafka brokers in a cluster, even though the project only needs a single Kafka instance at the moment.

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// When this function ends, we stop listening to signals
	defer stop()

	log.Printf("[MESSAGE - %s] Connecting Kafka in brokers: %v | Topic: %s", processLog, brokers, topic)

	publisher := kafka.NewKafkaPublisher(brokers, topic)
	defer publisher.Close()

	service := generator.NewGeneratorService(publisher)

	// Defines the duration for every Event generated (which are posted in Kafka's topic "gen-pool").
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := service.Run(ctx); err != nil {
				log.Printf("[WARNING - %s] - Failed to run generator service: %v\n", processLog, err)
				continue
			}

			log.Printf("[MESSAGE - %s] - Event generated and published successfully\n", processLog)

		case <-ctx.Done():
			log.Printf("[MESSAGE - %s] - ctx.Done() received, shutting down...", processLog)
			return
		}
	}
}