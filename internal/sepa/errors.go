package sepa

import "github.com/ai-crypto-onramp/rail-connector/internal/rail"

// mapReasonCode translates a SEPA reason code onto the normalized rail error
// taxonomy.
func mapReasonCode(code string) *rail.Error {
	switch code {
	case "AC01": // Account identifier incorrect
		return rail.NewError(rail.CodeInvalidRequest, code)
	case "AM04": // Insufficient funds
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "NOAS": // No answer from creditor
		return rail.NewError(rail.CodeDoNotHonor, code)
	case "AC04": // Account closed
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "AG01", "AG02", "AG03": // Transaction not allowed / forbidden
		return rail.NewError(rail.CodeFraudDecline, code)
	case "FF01", "FF02", "FF03": // Invalid file format
		return rail.NewError(rail.CodeInvalidRequest, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}

// MapReasonCode is the exported wrapper for tests.
func MapReasonCode(code string) *rail.Error { return mapReasonCode(code) }
