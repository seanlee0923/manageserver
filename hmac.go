package manageserver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"
)

const (
	hmacTimestampHeader = "X-Manageserver-Timestamp"
	hmacNonceHeader     = "X-Manageserver-Nonce"
	hmacSignatureHeader = "X-Manageserver-Signature"

	// DefaultHMACMaxSkew bounds how far a request's timestamp may drift
	// from the server's clock before it's rejected. This is the only
	// replay protection in play — manageserver keeps no nonce store, so a
	// captured signature stays valid for the rest of this window. In
	// practice that window is further narrowed by the server's existing
	// duplicate-connection rejection: replaying against an id that's
	// already connected fails regardless.
	DefaultHMACMaxSkew = 5 * time.Minute
)

func hmacSign(id, secret, nonce, tsStr string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(id))
	mac.Write([]byte{0})
	mac.Write([]byte(nonce))
	mac.Write([]byte{0})
	mac.Write([]byte(tsStr))
	return hex.EncodeToString(mac.Sum(nil))
}

// signHMACRequest computes an HMAC-SHA256 signature over id, nonce and ts
// using secret, and sets the corresponding headers on header. Used by the
// client-side WithHMACAuth option.
func signHMACRequest(header http.Header, id, secret, nonce string, ts time.Time) {
	tsStr := strconv.FormatInt(ts.Unix(), 10)
	header.Set(hmacTimestampHeader, tsStr)
	header.Set(hmacNonceHeader, nonce)
	header.Set(hmacSignatureHeader, hmacSign(id, secret, nonce, tsStr))
}

// VerifyHMACRequest checks the signature headers set by the client-side
// WithHMACAuth(secret) against secret, rejecting requests whose timestamp
// has drifted from now by more than maxSkew (use DefaultHMACMaxSkew if
// unsure). It's a standalone building block usable directly from a custom
// WithRequestValidator func; most callers will instead reach for
// HMACRequestValidator below.
func VerifyHMACRequest(r *http.Request, id, secret string, maxSkew time.Duration) bool {
	tsStr := r.Header.Get(hmacTimestampHeader)
	nonce := r.Header.Get(hmacNonceHeader)
	sig := r.Header.Get(hmacSignatureHeader)
	if tsStr == "" || nonce == "" || sig == "" {
		return false
	}

	tsUnix, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	if skew := time.Since(time.Unix(tsUnix, 0)); skew > maxSkew || skew < -maxSkew {
		return false
	}

	expected := hmacSign(id, secret, nonce, tsStr)
	return hmac.Equal([]byte(expected), []byte(sig))
}

// HMACRequestValidator builds a WithRequestValidator func that verifies the
// HMAC signature set by a client's WithHMACAuth(secret) against the per-id
// secret returned by secretLookup (ok=false rejects immediately, e.g. for an
// unknown id or one with no secret provisioned). If maxSkew <= 0,
// DefaultHMACMaxSkew is used.
//
// This is the ready-made option for the common shared-secret case; for
// anything else (bearer tokens, mTLS client cert inspection, ...) write a
// plain func(r *http.Request, id string) bool and pass it to
// WithRequestValidator directly — VerifyHMACRequest is exported precisely so
// that custom function can still reuse the HMAC check if it wants to layer
// it with something else.
func HMACRequestValidator(secretLookup func(id string) (secret string, ok bool), maxSkew time.Duration) func(r *http.Request, id string) bool {
	if maxSkew <= 0 {
		maxSkew = DefaultHMACMaxSkew
	}
	return func(r *http.Request, id string) bool {
		secret, ok := secretLookup(id)
		if !ok || secret == "" {
			return false
		}
		return VerifyHMACRequest(r, id, secret, maxSkew)
	}
}
