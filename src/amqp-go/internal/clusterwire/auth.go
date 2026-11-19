package clusterwire

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	headerMAC     = "X-Strangeq-Mac"
	headerTime    = "X-Strangeq-Time"
	headerNonce   = "X-Strangeq-Nonce"
	headerDigest  = "X-Strangeq-Digest"
	freshness     = 15
	nonceBuckets  = 2*freshness + 1
	maxRemembered = 1 << 20
)

type authenticator struct {
	cluster string
	key     []byte
}

type credentials struct {
	timestamp string
	nonce     string
	digest    string
}

func bodyDigest(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (a authenticator) mac(label, path string, auth credentials) []byte {
	sum := hmac.New(sha256.New, a.key)
	for _, field := range []string{label, a.cluster, path, auth.timestamp, auth.nonce, auth.digest} {
		_, _ = sum.Write([]byte(field))
		_, _ = sum.Write([]byte("\n"))
	}
	return sum.Sum(nil)
}

func (a authenticator) signRequest(req *http.Request, body []byte) (credentials, error) {
	return a.signRequestParts(req, nil, body)
}

func (a authenticator) signRequestParts(req *http.Request, prefix, body []byte) (credentials, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return credentials{}, err
	}
	auth := credentials{
		timestamp: strconv.FormatInt(time.Now().Unix(), 10),
		nonce:     hex.EncodeToString(nonce[:]),
		digest:    bodyPartsDigest(prefix, body),
	}
	req.Header.Set(headerTime, auth.timestamp)
	req.Header.Set(headerNonce, auth.nonce)
	req.Header.Set(headerDigest, auth.digest)
	req.Header.Set(headerMAC, hex.EncodeToString(a.mac("request", req.URL.Path, auth)))
	return auth, nil
}

func (a authenticator) verifyRequest(req *http.Request, cache *replayCache) (credentials, bool) {
	auth := credentials{
		timestamp: req.Header.Get(headerTime), nonce: req.Header.Get(headerNonce),
		digest: req.Header.Get(headerDigest),
	}
	now := time.Now().Unix()
	second, err := strconv.ParseInt(auth.timestamp, 10, 64)
	if err != nil || second < now-freshness || second > now+freshness {
		return credentials{}, false
	}
	var nonce [16]byte
	var digest, signature [sha256.Size]byte
	if !decodeHex(nonce[:], auth.nonce) || !decodeHex(digest[:], auth.digest) ||
		!decodeHex(signature[:], req.Header.Get(headerMAC)) ||
		!hmac.Equal(signature[:], a.mac("request", req.URL.Path, auth)) {
		return credentials{}, false
	}
	return auth, cache.remember(nonce, second, now)
}

func decodeHex(dest []byte, value string) bool {
	if len(value) != len(dest)*2 {
		return false
	}
	_, err := hex.Decode(dest, []byte(value))
	return err == nil
}

func bodyPartsDigest(prefix, body []byte) string {
	switch {
	case len(prefix) == 0:
		return bodyDigest(body)
	case len(body) == 0:
		return bodyDigest(prefix)
	default:
		hash := sha256.New()
		_, _ = hash.Write(prefix)
		_, _ = hash.Write(body)
		var digest [sha256.Size]byte
		return hex.EncodeToString(hash.Sum(digest[:0]))
	}
}

func (a authenticator) signResponse(header http.Header, path string, auth credentials, prefix, body []byte) {
	auth.digest = bodyPartsDigest(prefix, body)
	header.Set(headerMAC, hex.EncodeToString(a.mac("response", path, auth)))
}

func (a authenticator) verifyResponse(header http.Header, path string, auth credentials, result []byte) error {
	auth.digest = bodyDigest(result)
	var sig [sha256.Size]byte
	if !decodeHex(sig[:], header.Get(headerMAC)) || !hmac.Equal(sig[:], a.mac("response", path, auth)) {
		return errors.New("invalid peer response authentication")
	}
	return nil
}

type nonceBucket struct {
	second int64
	values map[[16]byte]struct{}
}

type replayCache struct {
	mu     sync.Mutex
	lastGC int64
	count  int
	slots  [nonceBuckets]nonceBucket
}

func (r *replayCache) remember(nonce [16]byte, second, now int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now != r.lastGC {
		for slot := range r.slots {
			if r.slots[slot].second < now-freshness {
				r.count -= len(r.slots[slot].values)
				r.slots[slot] = nonceBucket{}
			}
		}
		r.lastGC = now
	}
	bucket := &r.slots[(second%nonceBuckets+nonceBuckets)%nonceBuckets]
	if bucket.second != second {
		r.count -= len(bucket.values)
		*bucket = nonceBucket{second: second, values: make(map[[16]byte]struct{})}
	}
	if bucket.values == nil {
		bucket.values = make(map[[16]byte]struct{})
	}
	if _, exists := bucket.values[nonce]; exists || r.count >= maxRemembered {
		return false
	}
	bucket.values[nonce] = struct{}{}
	r.count++
	return true
}
