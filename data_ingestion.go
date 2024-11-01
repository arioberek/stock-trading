package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type DataIngestionService struct {
	producer *kafka.Producer
	logger   *zap.Logger
	stopCh   chan struct{}
}

func NewDataIngestionService() *DataIngestionService {
	logger, _ := zap.NewProduction()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "stock-data-producer",
		"acks":              "all",
	})

	if err != nil {
		logger.Fatal("Failed to create Kafka producer", zap.Error(err))
	}

	return &DataIngestionService{
		producer: p,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

func (s *DataIngestionService) Start(ctx context.Context) error {
	s.logger.Info("Starting Data Ingestion Service")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return nil
		case <-ticker.C:
			s.ingestStockData()
		}
	}
}

func (s *DataIngestionService) Stop() error {
	s.logger.Info("Stopping Data Ingestion Service")
	close(s.stopCh)
	s.producer.Close()
	return nil
}

func (s *DataIngestionService) ingestStockData() {
	stocks := []string{"AAPL", "GOOGL", "MSFT", "AMZN", "FB"}

	for _, stock := range stocks {
		price := rand.Float64() * 1000
		volume := rand.Int63n(1000000)

		data := []byte(fmt.Sprintf(`{"symbol":"%s","price":%.2f,"volume":%d,"timestamp":"%s"}`,
			stock, price, volume, time.Now().Format(time.RFC3339)))

		err := s.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &stock, Partition: kafka.PartitionAny},
			Value:          data,
		}, nil)

		if err != nil {
			s.logger.Error("Failed to produce message", zap.Error(err))
		}
	}
}
