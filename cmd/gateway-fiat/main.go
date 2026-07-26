package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/ach"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/card"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/dummy"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/otel"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/pix"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/sepa"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/server"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/settlement"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store/postgres"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/upi"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func devMode() bool { return os.Getenv("DEV_MODE") == "1" }

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
	railName := strings.ToLower(strings.TrimSpace(os.Getenv("RAIL_FAMILY")))
	if railName == "" {
		railName = "dummy"
	}
	st := newStore()
	tracker := settlement.New(st)
	as := audit.NewRecorder()
	conn, err := buildConnector(railName, st, tracker, as)
	if err != nil {
		log.Fatalf("rail connector: %v", err)
	}
	srv := server.New(server.Config{
		Addr:          addr,
		Rail:          railName,
		WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
		Store:         st,
		Tracker:       tracker,
		AuditSink:     as,
		Connector:     conn,
		Ready:         true,
	})
	log.Printf("rail-connectors listening on %s (rail=%s)", addr, railName)
	if err := http.ListenAndServe(addr, otelhttp.NewHandler(srv.Mux(), "rail-connectors")); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}

func buildConnector(railName string, st store.Store, tracker *settlement.Tracker, as audit.Sink) (rail.Connector, error) {
	switch railName {
	case "dummy":
		if !devMode() {
			return nil, fmt.Errorf("rail connector %q not configured in production mode — set RAIL_FAMILY to a real rail (ach|sepa|card|pix|upi) or set DEV_MODE=1 for local dev", railName)
		}
		log.Printf("DEV_MODE=1: using dummy rail connector — NOT FOR PRODUCTION")
		return dummy.New(st, tracker, dummy.Config{Rail: railName, AuditSink: as}), nil
	case "ach":
		baseURL := os.Getenv("RAIL_ACH_PARTNER_URL")
		if baseURL == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_ACH_PARTNER_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		return ach.New(ach.Config{BaseURL: baseURL, APIKey: os.Getenv("RAIL_ACH_API_KEY")}, st, as)
	case "sepa":
		baseURL := os.Getenv("RAIL_SEPA_API_URL")
		if baseURL == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_SEPA_API_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		return sepa.New(sepa.Config{
			BaseURL:  baseURL,
			APIKey:   os.Getenv("RAIL_SEPA_API_KEY"),
			MTLSCert: os.Getenv("RAIL_SEPA_MTLS_CERT"),
			MTLSKey:  os.Getenv("RAIL_SEPA_MTLS_KEY"),
		}, st, as)
	case "card":
		proc := os.Getenv("RAIL_CARD_PROCESSOR")
		if proc == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_CARD_PROCESSOR not set and DEV_MODE!=1; refusing to start in production mode (must be stripe|adyen)")
		}
		apiKey := os.Getenv("RAIL_CARD_API_KEY")
		if apiKey == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_CARD_API_KEY not set and DEV_MODE!=1; refusing to start in production mode")
		}
		return card.New(card.Config{
			Processor: proc,
			APIKey:    apiKey,
			Merchant:  os.Getenv("RAIL_CARD_MERCHANT"),
		}, st, as)
	case "pix":
		baseURL := os.Getenv("RAIL_PIX_API_URL")
		if baseURL == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_PIX_API_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		return pix.New(pix.Config{BaseURL: baseURL, APIKey: os.Getenv("RAIL_PIX_API_KEY")}, st, as)
	case "upi":
		baseURL := os.Getenv("RAIL_UPI_API_URL")
		if baseURL == "" && !devMode() {
			return nil, fmt.Errorf("RAIL_UPI_API_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		return upi.New(upi.Config{BaseURL: baseURL, APIKey: os.Getenv("RAIL_UPI_API_KEY"), PayeeVPA: os.Getenv("RAIL_UPI_PAYEE_VPA")}, st, as)
	default:
		return nil, fmt.Errorf("unknown rail family %q — set RAIL_FAMILY to one of ach|sepa|card|pix|upi (or dummy with DEV_MODE=1)", railName)
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
