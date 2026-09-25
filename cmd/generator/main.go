package main

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/CaueTech/metron-engine/internal/application/generator"
	"github.com/CaueTech/metron-engine/internal/domain"
	// Importa o código gerado pelo protoc
	eventv1 "github.com/CaueTech/metron-engine/internal/infra/proto"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "events")
	cooldown := getEnvDuration("GENERATOR_COOLDOWN", 30*time.Second)

	log.Printf("[generator] starting with cooldown: %s, kafka: %s, topic: %s", cooldown, kafkaBrokers, kafkaTopic)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("[generator] error closing kafka writer: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(cooldown)
	defer ticker.Stop()

	fixedSourceID := uuid.MustParse("018e692a-3b91-7d12-8e11-cf93911f97a1")

	for {
		select {
		case <-ctx.Done():
			log.Println("[generator] shutdown signal received, exiting gracefully...")
			return

		case <-ticker.C:
			randomType := domain.EventType(rand.IntN(3) + 1)

			event, err := generator.NewEvent(fixedSourceID, randomType)
			if err != nil {
				log.Printf("[generator] failed to generate event: %v", err)
				continue
			}

			// 1. Mapeia a entidade de domínio para a mensagem Protobuf gerada
			protoMsg := &eventv1.Event{
				Id:        event.ID().String(),
				SourceId:  event.SourceID().String(),
				EventType: eventv1.EventType(event.Type()),
				Timestamp: timestamppb.New(event.Timestamp()),
			}

			// 2. Serializa em formato binário compacto
			payloadBytes, err := proto.Marshal(protoMsg)
			if err != nil {
				log.Printf("[generator] failed to marshal protobuf message: %v", err)
				continue
			}

			// 3. Publica os bytes brutos no Kafka
			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(protoMsg.SourceId),
				Value: payloadBytes,
			})
			if err != nil {
				log.Printf("[generator] failed to publish event to kafka: %v", err)
				continue
			}

			log.Printf("[generator] published protobuf event: id=%s type=%s size=%d bytes",
				protoMsg.Id, protoMsg.EventType, len(payloadBytes))
		}
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
		log.Printf("[config] invalid duration for %s, using default %s", key, fallback)
	}
	return fallback
}