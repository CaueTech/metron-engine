package main

import {
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/CaueTech/metron-engine/infra/kafka"
	"github.com/CaueTech/metron-engine/application/generator"
}

var{
	processLog := "generator" 
}

func getEnv(key, fallback string) string{
	if value, exists := os.LookupEnv(key); exists{
		return value
	}
	return fallback

	/*
		This if block may seem strange, but it is a Go technique called a simple statement. It first executes value, exists := os.LookupEnv() and then checks the condition using exists.
	*/
}

func main(){
	rawBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := gentEnv("KAFKA_TOPIC", "events")
	brokers := strings.Split(rawBrokers, ",")
	
	/*
		This strings.Split() simulates a case where there are multiple Kafka brokers in a cluster, even though the project only needs a single Kafka instance at the moment.
	*/

	log.Printf("[MESSAGE - %s] Connecting Kafka in brokers: %v | Topic: %s", processLog, brokers, topic)

	/* 
		The & operator applies to the object from the kafka package, such as Writer or LeastBytes{}.
	*/
	
	writer := NewKafkaWriter(brokers, topic)
	defer writer.Close()

	// Defines the duration for every Event generated (which are posted in Kafka's topic "gen-pool").
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for{
		select{
			case <-ticker.C:	
				event, err := GenerateEvent()

				if err != nil{
					log.Printf("[WARNING - %S] - Failed to generate event: %v\n", processLog, err)
					continue
				}

				// [...] -> Build serialization + posting in Kafka "gen-pool" topic
			}
			case <-ctx.Done():
				log.Printf("[MESSAGE - %s] - ctx.Done() received, shuttind down...", processLog)
				return
		}
	}
}