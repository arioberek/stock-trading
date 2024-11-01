package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Main application struct
type App struct {
	dataIngestion *DataIngestionService
	analysis      *AnalysisService
	trading       *TradingService
	notification  *NotificationService
	logger        *zap.Logger
}

func NewApp() (*App, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	return &App{
		dataIngestion: NewDataIngestionService(),
		analysis:      NewAnalysisService(),
		trading:       NewTradingService(),
		notification:  NewNotificationService(),
		logger:        logger,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return a.dataIngestion.Start(ctx)
	})

	g.Go(func() error {
		return a.analysis.Start(ctx)
	})

	g.Go(func() error {
		return a.trading.Start(ctx)
	})

	g.Go(func() error {
		return a.notification.Start(ctx)
	})

	g.Go(func() error {
		return a.startAPIServer(ctx)
	})

	return g.Wait()
}

func (a *App) Stop() {
	a.dataIngestion.Stop()
	a.analysis.Stop()
	a.trading.Stop()
	a.notification.Stop()
	a.logger.Sync()
}

func (a *App) startAPIServer(ctx context.Context) error {
	router := gin.Default()

	// API routes
	router.GET("/health", a.healthCheck)
	router.GET("/metrics", a.getMetrics)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("Server forced to shutdown", zap.Error(err))
		}
	}()

	a.logger.Info("Starting API server", zap.String("addr", srv.Addr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (a *App) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *App) getMetrics(c *gin.Context) {
	// Implement metrics collection and reporting
	c.JSON(http.StatusOK, gin.H{"metrics": "not implemented"})
}

func main() {
	app, err := NewApp()
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		app.logger.Info("Received shutdown signal")
		cancel()
	}()

	if err := app.Start(ctx); err != nil {
		app.logger.Error("Application error", zap.Error(err))
		os.Exit(1)
	}

	app.Stop()
	app.logger.Info("Application stopped")
}
