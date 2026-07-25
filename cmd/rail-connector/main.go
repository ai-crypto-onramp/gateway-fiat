package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/ai-crypto-onramp/rail-connector/internal/dummy"
	"github.com/ai-crypto-onramp/rail-connector/internal/otel"
	"github.com/ai-crypto-onramp/rail-connector/internal/server"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
	"github.com/ai-crypto-onramp/rail-connector/internal/store/postgres"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func devMode() bool { return os.Getenv("DEV_MODE") == "1" }

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v != "" {
		return v
	}
	if devMode() {
		log.Printf("DEV_MODE=1: env var %s unset — using dummy (NOT FOR PRODUCTION)", name)
		return ""
	}
	log.Fatalf("required env var %s not set and DEV_MODE!=1; refusing to start in production mode", name)
	return ""
}

func main() {
	shutdown, err := otel.Init("rail-connectors")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = ":8080"
	}
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}
	rail := os.Getenv("RAIL_FAMILY")
	if rail == "" {
		rail = "dummy"
	}
	if devMode() {
		log.Printf("DEV_MODE=1: using dummy rail connector for family %s — NOT FOR PRODUCTION", rail)
	} else {
		_ = mustEnv("RAIL_CONNECTORS_URL")
		log.Fatalf("RAIL_CONNECTORS_URL required in production mode; real rail connector client not yet implemented — set DEV_MODE=1 for local dev")
	}
	st := newStore()
	srv := server.New(server.Config{
		Addr:          addr,
		Rail:          rail,
		WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
		Store:         st,
		Ready:         true,
	})
	log.Printf("rail-connectors listening on %s (rail=%s)", addr, rail)
	if err := http.ListenAndServe(addr, otelhttp.NewHandler(srv.Mux(), "rail-connectors")); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}

func newStore() store.Store {
	dsn := os.Getenv("DB_URL")
	if dsn != "" {
		db, err := postgres.Open(context.Background(), dsn)
		if err != nil {
			log.Fatalf("postgres: open: %v", err)
		}
		return db
	}
	if devMode() {
		log.Printf("WARNING: DEV_MODE=1 with no DB_URL — using in-memory store; all state is lost on restart")
		return store.New()
	}
	log.Fatalf("DB_URL required in production mode — set DEV_MODE=1 to allow in-memory store for development")
	return store.New()
}
