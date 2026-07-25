package pix

import "github.com/ai-crypto-onramp/rail-connector/internal/rail"

// mapReasonCode translates a PIX return reason onto the normalized rail
// error taxonomy.
func mapReasonCode(code string) *rail.Error {
	switch code {
	case "00", "BE01", "BE02": // Success / balance error variants
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "PI01": // Payer not found
		return rail.NewError(rail.CodeInvalidRequest, code)
	case "PI02": // Payer not authorized
		return rail.NewError(rail.CodeFraudDecline, code)
	case "PI03": // Account closed
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "PI04": // Account blocked
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "PI05": // Inconsistent amount
		return rail.NewError(rail.CodeInvalidRequest, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}

// MapReasonCode is the exported wrapper for tests.
func MapReasonCode(code string) *rail.Error { return mapReasonCode(code) }
