package dummy

import (
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/settlement"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

func init() {
	for _, f := range []string{"card", "ach", "sepa", "pix", "upi", "dummy"} {
		family := f
		rail.Register(family, func(cfg map[string]string) (rail.Connector, error) {
			d := NewDefault(family)
			if v, ok := cfg["fail"]; ok && (v == "true" || v == "1") {
				d.SetFail(true)
			}
			return d, nil
		})
	}
}

// NewDefault returns a DummyRailConnector with a fresh in-memory store and
// tracker. Used by the registry constructor.
func NewDefault(rail string) *Connector {
	s := store.New()
	t := settlement.New(s)
	return New(s, t, Config{Rail: rail})
}
