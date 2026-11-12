// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package signing is the signer's shared code: the fenced nonce reservation, a fake chain and the
// /sign server.
//
// A signer cannot be fenced on a chain, so its fence is the nonce it reserves. Before it signs a
// transaction, a signer reserves the wallet's next nonce from Nonces at the term of its shard; a
// stale signer, one that still runs after another member took the shard, is rejected there with
// ErrStale and so cannot sign. Both signer programs, the in-memory one and the one on Kubernetes,
// serve the same Signer; the package exists so that they can share it.
package signing

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

const (
	// SpaceName is the name of the wallets space, and of the fence space of the nonces in the
	// fake downstream.
	SpaceName = "wallets"

	// Shards is the number of shards of the wallets space.
	Shards = 8

	// SigningKey is the key the fake signer signs with.
	SigningKey = "fake-signing-key"

	// settle is how long Run waits after it raised the fence before it serves. A reservation
	// that the previous owner made just before the raise needs that long to reach the chain;
	// without the wait the new owner's first broadcast could overtake it and be a gap.
	settle = 100 * time.Millisecond

	// broadcastTimeout bounds a broadcast that no longer follows the request's context.
	broadcastTimeout = 10 * time.Second
)

// ErrStale means the term of a reservation is below the shard's fence. It is fencedb.ErrStale,
// one sentinel for the whole example: a client of a remote Nonces maps the service's stale answer
// back to it.
var ErrStale = fencedb.ErrStale

// Tx is one transaction of a wallet.
type Tx struct {
	Wallet  string `json:"wallet"`
	Nonce   uint64 `json:"nonce"`
	Payload string `json:"payload"`
	Sig     string `json:"sig"`
}

// Nonces hands out the nonces of the wallets, fenced by term.
type Nonces interface {
	// Raise raises the fence of the shard to term. It returns an error that wraps ErrStale when
	// term is below the fence.
	Raise(ctx context.Context, shard int, term core.Term) error

	// Reserve returns the wallet's next nonce, starting at 0, and advances it in the same step.
	// It returns an error that wraps ErrStale, and leaves the next nonce as it was, when term
	// is below the shard's fence.
	Reserve(ctx context.Context, shard int, term core.Term, wallet string) (uint64, error)
}

// Chain takes signed transactions.
type Chain interface {
	// Broadcast submits tx. It returns an error that wraps ErrConflict or ErrGap when the chain
	// rejects its nonce.
	Broadcast(ctx context.Context, tx Tx) error
}

// MemNonces is Nonces over an in-memory fencedb.DB, in the space "wallets". Make one with
// NewNonces.
type MemNonces struct {
	db *fencedb.DB
}

// NewNonces returns Nonces that keep their state, and their fence, in db.
func NewNonces(db *fencedb.DB) *MemNonces {
	return &MemNonces{db: db}
}

// Raise raises the fence of the shard in db to term.
func (n *MemNonces) Raise(_ context.Context, shard int, term core.Term) error {
	return n.db.Raise(SpaceName, shard, term)
}

// Reserve reads the wallet's next nonce, stores it plus one and returns it, in one db.Update at
// term, so a stale term fails with ErrStale and changes nothing.
func (n *MemNonces) Reserve(_ context.Context, shard int, term core.Term, wallet string) (uint64, error) {
	key := "nonce/" + wallet
	var next uint64
	err := n.db.Update(SpaceName, shard, term, func(tx *fencedb.Tx) error {
		if v, ok := tx.Get(key); ok {
			var err error
			if next, err = strconv.ParseUint(string(v), 10, 64); err != nil {
				return fmt.Errorf("signing: nonce of %s: %w", wallet, err)
			}
		}
		tx.Put(key, []byte(strconv.FormatUint(next+1, 10)))
		return nil
	})
	if err != nil {
		return 0, err
	}
	return next, nil
}

// Signature returns the hex HMAC-SHA256, under SigningKey, of the wallet, the nonce and the
// payload, the three joined by line feeds.
func Signature(wallet string, nonce uint64, payload string) string {
	mac := hmac.New(sha256.New, []byte(SigningKey))
	mac.Write([]byte(wallet + "\n" + strconv.FormatUint(nonce, 10) + "\n" + payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// Signer serves the wallets space: for each shard it owns, it reserves nonces, signs and
// broadcasts. Make one with New.
type Signer struct {
	nonces Nonces
	chain  Chain

	// settle is the wait between the raise and the publication of a shard.
	settle time.Duration

	mu     sync.Mutex
	shards map[shardKey]*shardState
}

type shardKey struct {
	shard int
	term  core.Term
}

// shardState is a shard published for one term. Its mutex serialises the requests of the shard,
// so that one owner never broadcasts the nonces of a wallet out of order.
type shardState struct {
	mu       sync.Mutex
	refusing bool
}

// New returns a Signer that reserves from nonces and broadcasts to chain.
func New(nonces Nonces, chain Chain) *Signer {
	return &Signer{nonces: nonces, chain: chain, settle: settle, shards: make(map[shardKey]*shardState)}
}

// Space returns the wallets space served by s: 8 shards, fenced, so that a node that cannot
// reach the store keeps its shards running and relies on the nonce reservation to stop it.
func (s *Signer) Space() tenure.Space {
	return tenure.Space{Name: SpaceName, Shards: Shards, Fenced: true, Run: s.Run}
}

// Run serves one shard of the wallets space while the node owns it, as the Run of the space. It
// raises the shard's fence to the term of sh first, so that an older owner can no longer reserve;
// an error that wraps ErrStale means a newer owner got there first, and Run returns it. After a
// short wait, which lets a reservation of the old owner reach the chain, it publishes the shard
// for (shard, term) and blocks until ctx ends, when it unpublishes the shard and returns nil.
func (s *Signer) Run(ctx context.Context, sh *tenure.Shard) error {
	if err := s.nonces.Raise(ctx, sh.ID, sh.Term); err != nil {
		return err
	}
	t := time.NewTimer(s.settle)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		return nil
	}

	k := shardKey{sh.ID, sh.Term}
	st := new(shardState)
	s.mu.Lock()
	s.shards[k] = st
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	if s.shards[k] == st {
		delete(s.shards, k)
	}
	s.mu.Unlock()
	st.mu.Lock()
	st.refusing = true
	st.mu.Unlock()
	return nil
}

// shard returns the state published for the shard at exactly this term, or nil.
func (s *Signer) shard(shard int, term core.Term) *shardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shards[shardKey{shard, term}]
}

var (
	// errRefusing means the shard takes no more requests at its term: the fence rejected a
	// reservation, or its Run has ended.
	errRefusing = errors.New("signing: shard refuses requests")

	// errNotPublished means no Run has published the shard for the term yet.
	errNotPublished = errors.New("signing: the shard is not ready")
)

// sign reserves the wallet's next nonce at the term of the shard, signs the payload and
// broadcasts the transaction, all under the shard's lock. An error that wraps ErrStale means the
// fence rejected the reservation: the shard then refuses every further request of its term.
//
// A reserved nonce is spent whatever happens next, so a failed broadcast leaves a gap that the
// chain rejects for every later transaction of the wallet. The broadcast therefore does not stop
// when the client goes away: it runs on a context that is not cancelled with ctx, and ends after
// broadcastTimeout. A chain that rejects or fails the transaction itself still burns the nonce;
// the example does not repair that, since the fake chain only rejects what a correct signer
// never sends.
func (s *Signer) sign(ctx context.Context, shard int, term core.Term, wallet, payload string) (Tx, error) {
	st := s.shard(shard, term)
	if st == nil {
		return Tx{}, errNotPublished
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.refusing {
		return Tx{}, errRefusing
	}
	nonce, err := s.nonces.Reserve(ctx, shard, term, wallet)
	if err != nil {
		if errors.Is(err, ErrStale) {
			st.refusing = true
		}
		return Tx{}, err
	}
	tx := Tx{Wallet: wallet, Nonce: nonce, Payload: payload, Sig: Signature(wallet, nonce, payload)}
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()
	if err := s.chain.Broadcast(bctx, tx); err != nil {
		return Tx{}, err
	}
	return tx, nil
}
