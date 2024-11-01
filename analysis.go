package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type StockData struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    int64     `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
}

type AnalysisService struct {
	consumer *kafka.Consumer
	redis    *redis.Client
	logger   *zap.Logger
	stopCh   chan struct{}
}

func NewAnalysisService() *AnalysisService {
	logger, _ := zap.NewProduction()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "stock-analysis-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		logger.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	return &AnalysisService{
		consumer: c,
		redis:    rdb,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

func (s *AnalysisService) Start(ctx context.Context) error {
	s.logger.Info("Starting Analysis Service")

	err := s.consumer.SubscribeTopics([]string{"AAPL", "GOOGL", "MSFT", "AMZN", "FB"}, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return nil
		default:
			msg, err := s.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				s.logger.Error("Failed to read message", zap.Error(err))
				continue
			}

			var stockData StockData
			err = json.Unmarshal(msg.Value, &stockData)
			if err != nil {
				s.logger.Error("Failed to unmarshal stock data", zap.Error(err))
				continue
			}

			s.analyzeStockData(ctx, stockData)
		}
	}
}

func (s *AnalysisService) Stop() error {
	s.logger.Info("Stopping Analysis Service")
	close(s.stopCh)
	s.consumer.Close()
	return s.redis.Close()
}

func (s *AnalysisService) analyzeStockData(ctx context.Context, data StockData) {
	// Simple moving average calculation
	key := fmt.Sprintf("%s:prices", data.Symbol)
	s.redis.LPush(ctx, key, data.Price)
	s.redis.LTrim(ctx, key, 0, 19) // Keep last 20 prices

	prices, err := s.redis.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		s.logger.Error("Failed to get prices from Redis", zap.Error(err))
		return
	}

	var sum float64
	for _, p := range prices {
		price, _ := strconv.ParseFloat(p, 64)
		sum += price
	}

	movingAvg := sum / float64(len(prices))

	s.logger.Info("Stock analysis",
		zap.String("symbol", data.Symbol),
		zap.Float64("current_price", data.Price),
		zap.Float64("moving_average", movingAvg),
	)

	// Store the moving average
	s.redis.Set(ctx, fmt.Sprintf("%s:ma", data.Symbol), movingAvg, 0)
}
