package garageadmin

import (
	"context"
	"net/http"
)

// AdminClient is the operator-facing handle on Garage's Admin API v2. It embeds the
// generated ClientWithResponses (so every typed `<Operation>WithResponse` method is
// available) and injects bearer-token authentication on every request.
//
// The wrapper exists because the generated package already owns the names Client /
// NewClient for its low-level (untyped) client; AdminClient is the stable surface the
// controllers consume.
type AdminClient struct {
	*ClientWithResponses
}

// NewAdminClient builds an AdminClient for the admin endpoint at baseURL,
// authenticating with adminToken. Extra ClientOptions (e.g. WithHTTPClient) are
// applied after the auth editor and are primarily useful in tests.
func NewAdminClient(baseURL, adminToken string, opts ...ClientOption) (*AdminClient, error) {
	withAuth := append([]ClientOption{WithRequestEditorFn(bearerAuth(adminToken))}, opts...)
	generated, err := NewClientWithResponses(baseURL, withAuth...)
	if err != nil {
		return nil, err
	}
	return &AdminClient{ClientWithResponses: generated}, nil
}

func bearerAuth(token string) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
}
