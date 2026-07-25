package upi

import "github.com/ai-crypto-onramp/gateway-fiat/internal/rail"

// mapResponseCode translates an NPCI response code onto the normalized rail
// error taxonomy.
func mapResponseCode(code string) *rail.Error {
	switch code {
	case "00": // Success
		return nil
	case "ZP": // Pending
		return rail.NewError(rail.CodeDoNotHonor, code)
	case "ZD": // Declined
		return rail.NewError(rail.CodeDoNotHonor, code)
	case "ZF": // Fraud rejection
		return rail.NewError(rail.CodeFraudDecline, code)
	case "ZM": // Insufficient funds
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "ZE": // Expired instrument / account closed
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "ZI": // Invalid request
		return rail.NewError(rail.CodeInvalidRequest, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}

// MapResponseCode is the exported wrapper for tests.
func MapResponseCode(code string) *rail.Error { return mapResponseCode(code) }
