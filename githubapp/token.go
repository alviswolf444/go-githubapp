package githubapp

import (
	"context"
	sync "sync"
	time "time"

	"golang.org/x/sync/singleflight"
)

// Token represents a GitHub installation token.
type Token struct {
	Token     string
	ExpiresAt time.Time
}

// TokenProvider manages installation tokens with thread-safety and request coalescing.
type TokenProvider struct {
	mu           sync.RWMutex
	cache        map[int64]*Token
	sfGroup      singleflight.Group
	expiryBuffer time.Duration
}

func NewTokenProvider(expiryBuffer time.Duration) *TokenProvider {
	return &TokenProvider{
		cache:        make(map[int64]*Token),
		expiryBuffer: expiryBuffer,
	}
}

// GetToken retrieves a valid token, coalescing concurrent requests for the same installation ID.
func (p *TokenProvider) GetToken(ctx context.Context, installationID int64, fetcher func() (*Token, error)) (*Token, error) {
	p.mu.RLock()
	if token, ok := p.cache[installationID]; ok && time.Now().Add(p.expiryBuffer).Before(token.ExpiresAt) {
		p.mu.RUnlock()
		return token, nil
	}
	p.mu.RUnlock()

	val, err, _ := p.sfGroup.Do(string(installationID), func() (interface{}, error) {
		token, err := fetcher()
		if err != nil {
			return nil, err
		}

		p.mu.Lock()
		p.cache[installationID] = token
		p.mu.Unlock()
		return token, nil
	})

	if err != nil {
		return nil, err
	}

	return val.(*Token), nil
}