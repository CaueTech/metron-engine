package main

import {
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmetion/kafka-go"
}

func getEnv(key, fallback string) string{
	if value, exists := os.LookupEnv(key); exists{
		return value
	}
	return fallback

	/*
		Esse bloco if parece estranho, mas é uma técnica em Go chamada simple statement, ele executa o value, exits := os.LookupEnv() primeiro e depois faz a verificação da condição de exists
	*/
}

func main(){
	rawBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	// [...]
}