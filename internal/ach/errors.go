package ach

import "github.com/ai-crypto-onramp/gateway-fiat/internal/rail"

// mapReturnCode translates an ACH return code onto the normalized rail error
// taxonomy.
func mapReturnCode(code string) *rail.Error {
	switch code {
	case "R01": // Non-sufficient funds
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "R02", "R14", "R15": // Account closed / account holder deceased / beneficiary deceased
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "R03": // No account / unable to locate
		return rail.NewError(rail.CodeInvalidRequest, code)
	case "R10", "R11": // Customer advises not authorized / unknown
		return rail.NewError(rail.CodeFraudDecline, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}

// MapReturnCode is the exported wrapper around mapReturnCode for tests.
func MapReturnCode(code string) *rail.Error { return mapReturnCode(code) }
