package main

import (
	"testing"
)

/*
This test will focus on the environment variables logic more than anything else. The event-publishing testing will be dedicated to service_test.go in application/
*/

func TestGetEnv(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		fallback     string
		envValue     string
		setEnv       bool
		wantExpected string
	}{
		{
			name:         "must return env value when it exists",
			key:          "TEST_KAFKA_BROKERS",
			fallback:     "localhost:9092",
			envValue:     "kafka-broker-1:9092",
			setEnv:       true,
			wantExpected: "kafka-broker-1:9092",
		},
		{
			name:         "must return fallback when env does not exist",
			key:          "TEST_UNSET_VAR",
			fallback:     "default-topic",
			setEnv:       false,
			wantExpected: "default-topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setEnv {
				t.Setenv(tt.key, tt.envValue)
			}

			got := getEnv(tt.key, tt.fallback)
			if got != tt.wantExpected {
				t.Errorf("getEnv(%q, %q) = %q; expected %q", tt.key, tt.fallback, got, tt.wantExpected)
			}
		})
	}
}