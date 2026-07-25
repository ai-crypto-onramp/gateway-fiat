package card

import "github.com/ai-crypto-onramp/gateway-fiat/internal/rail"

// mapStripeDecline translates a Stripe decline code (or API error code) onto
// the normalized rail error taxonomy.
func mapStripeDecline(code string) *rail.Error {
	switch code {
	case "insufficient_funds", "card_declined_insufficient_funds":
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "expired_card":
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "fraudulent", "card_declined_fraud":
		return rail.NewError(rail.CodeFraudDecline, code)
	case "do_not_honor":
		return rail.NewError(rail.CodeDoNotHonor, code)
	case "processing_error", "invalid_request":
		return rail.NewError(rail.CodeInvalidRequest, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}

// mapAdyenDecline translates an Adyen refusal code onto the normalized rail
// error taxonomy.
func mapAdyenDecline(code string) *rail.Error {
	switch code {
	case "Refused", "Not enough balance":
		return rail.NewError(rail.CodeInsufficientFunds, code)
	case "Expired Card":
		return rail.NewError(rail.CodeExpiredInstrument, code)
	case "Fraud":
		return rail.NewError(rail.CodeFraudDecline, code)
	case "CVC Declined", "Transaction Not Permitted":
		return rail.NewError(rail.CodeDoNotHonor, code)
	case "Invalid Request", "RefusalReason required":
		return rail.NewError(rail.CodeInvalidRequest, code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}
