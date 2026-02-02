package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithCORSPreflightAllowedOrigin(t *testing.T) {
	handler := withCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatalf("preflight request should not reach wrapped handler")
	}), []string{"https://app.example.com"})

	req := httptest.NewRequest(http.MethodOptions, "/contracts", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, X-Client-Version")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d, got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("expected allowed origin header to echo request origin, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, X-Client-Version" {
		t.Fatalf("expected allow headers to mirror preflight request, got %q", got)
	}
}

func TestWithCORSPreflightDisallowedOrigin(t *testing.T) {
	handler := withCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatalf("disallowed preflight should not reach wrapped handler")
	}), []string{"https://allowed.example.com"})

	req := httptest.NewRequest(http.MethodOptions, "/contracts", nil)
	req.Header.Set("Origin", "https://blocked.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d", http.StatusForbidden, rec.Code)
	}
}

func TestWithCORSWildcardAllowsAnyOrigin(t *testing.T) {
	called := false
	handler := withCORS(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://any-origin.example.com")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("expected wrapped handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected wildcard allow origin, got %q", got)
	}
}
