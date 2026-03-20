package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"anchordb/internal/api"
	"anchordb/internal/config"
	"anchordb/internal/crypto"
	"anchordb/internal/metadata"
	"anchordb/internal/repository"
	"anchordb/internal/scheduler"
)

func main() {
	cfg := config.Load()

	db, err := metadata.Open(cfg)
	if err != nil {
		log.Fatalf("open metadata db: %v", err)
	}

	if err := metadata.Migrate(db); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	cryptoSvc := crypto.New(cfg.SecretKey)
	repo := repository.New(db, cryptoSvc)
	exec := scheduler.NewExecutor(cfg)
	sch := scheduler.New(repo, exec, cfg)

	ctx := context.Background()
	if err := sch.LoadAll(ctx); err != nil {
		log.Fatalf("load schedules: %v", err)
	}
	sch.Start()

	h := api.NewHandler(repo, sch)
	server := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      h.Router(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("AnchorDB listening on %s", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	<-shutdown

	log.Println("shutting down")
	sch.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
}
