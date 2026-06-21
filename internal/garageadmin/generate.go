// Package garageadmin is a typed client for Garage's Admin API v2, shared by the
// operator's controllers. The bulk of the client is generated from the upstream
// OpenAPI spec; see client.go for the operator-facing wrapper.
package garageadmin

// The upstream spec is OpenAPI 3.1.0, which oapi-codegen cannot parse, so we first
// down-convert it to a 3.0 subset (see openapi/downconvert) and then generate from
// the converted copy. Both the converted spec and the generated client are committed
// so CI can verify them with `git diff --exit-code`.
//
//go:generate go run ./openapi/downconvert openapi/garage-admin-v2.json openapi/garage-admin-v2.openapi30.json
//go:generate go tool oapi-codegen -config oapi-codegen.yaml openapi/garage-admin-v2.openapi30.json
