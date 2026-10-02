// Package bundle defines signed release manifests and signed offline transfer
// packages. A region agent trusts only public keys it was provisioned with,
// never the network path or the control plane that delivered the bytes, so a
// tampered artifact or a replayed old package is rejected locally.
package bundle

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrDigestMismatch = errors.New("artifact digest mismatch")
	ErrBadSignature   = errors.New("signature verification failed")
	ErrUnknownKey     = errors.New("unknown signing key")
)

// Manifest describes one release artifact. It is what gets signed.
type Manifest struct {
	Service   string    `json:"service"`
	Version   string    `json:"version"`
	Digest    string    `json:"digest"` // hex SHA-256 of the artifact bytes
	Size      int       `json:"size"`
	KeyID     string    `json:"key_id"`
	CreatedAt time.Time `json:"created_at"`
	Signature string    `json:"signature,omitempty"`
}

// Digest returns the hex SHA-256 of b.
func Digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// KeyID derives a short stable identifier from a public key.
func KeyID(pub ed25519.PublicKey) string {
	s := sha256.Sum256(pub)
	return hex.EncodeToString(s[:6])
}

// payload is the canonical byte string covered by a manifest signature.
// Fields are length-delimited by JSON encoding of a fixed struct, so no two
// distinct manifests share a payload.
func (m Manifest) payload() []byte {
	b, _ := json.Marshal(struct {
		S string `json:"s"`
		V string `json:"v"`
		D string `json:"d"`
		N int    `json:"n"`
		K string `json:"k"`
		T string `json:"t"`
	}{m.Service, m.Version, m.Digest, m.Size, m.KeyID, m.CreatedAt.UTC().Format(time.RFC3339Nano)})
	return b
}

// Signer holds a release signing key.
type Signer struct {
	Priv ed25519.PrivateKey
	Pub  ed25519.PublicKey
	ID   string
}

// NewSigner generates a fresh key, or derives one deterministically from seed
// when seed is 32 bytes (used for reproducible demos and tests only).
func NewSigner(seed []byte) (*Signer, error) {
	var priv ed25519.PrivateKey
	if len(seed) == ed25519.SeedSize {
		priv = ed25519.NewKeyFromSeed(seed)
	} else {
		_, p, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		priv = p
	}
	pub := priv.Public().(ed25519.PublicKey)
	return &Signer{Priv: priv, Pub: pub, ID: KeyID(pub)}, nil
}

// Sign builds and signs a manifest for artifact.
func (s *Signer) Sign(service, version string, artifact []byte, at time.Time) Manifest {
	m := Manifest{Service: service, Version: version, Digest: Digest(artifact), Size: len(artifact), KeyID: s.ID, CreatedAt: at.UTC()}
	m.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(s.Priv, m.payload()))
	return m
}

// Keyring is the set of public keys an agent trusts.
type Keyring map[string]ed25519.PublicKey

func NewKeyring(pubs ...ed25519.PublicKey) Keyring {
	k := Keyring{}
	for _, p := range pubs {
		k[KeyID(p)] = p
	}
	return k
}

// VerifyManifest checks the manifest signature only.
func (k Keyring) VerifyManifest(m Manifest) error {
	pub, ok := k[m.KeyID]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownKey, m.KeyID)
	}
	sig, err := base64.StdEncoding.DecodeString(m.Signature)
	if err != nil || !ed25519.Verify(pub, m.payload(), sig) {
		return ErrBadSignature
	}
	return nil
}

// Verify checks the signature and that artifact matches the signed digest.
func (k Keyring) Verify(m Manifest, artifact []byte) error {
	if err := k.VerifyManifest(m); err != nil {
		return err
	}
	if len(artifact) != m.Size || Digest(artifact) != m.Digest {
		return ErrDigestMismatch
	}
	return nil
}

// OfflineJob is one deployment instruction carried in an offline package.
type OfflineJob struct {
	JobID      string   `json:"job_id"`
	Generation uint64   `json:"generation"`
	Manifest   Manifest `json:"manifest"`
}

// Package is a signed transfer bundle for a region with no network path to
// the control plane. It is carried across the air gap as a file.
type Package struct {
	Region    string            `json:"region"`
	Sequence  uint64            `json:"sequence"` // strictly increasing per region; blocks replay
	IssuedAt  time.Time         `json:"issued_at"`
	Jobs      []OfflineJob      `json:"jobs"`
	Artifacts map[string][]byte `json:"artifacts"` // digest -> bytes
	KeyID     string            `json:"key_id"`
	Signature string            `json:"signature,omitempty"`
}

func (p Package) payload() []byte {
	c := p
	c.Signature = ""
	c.Artifacts = nil // artifacts are bound through the digests in the signed manifests
	b, _ := json.Marshal(c)
	return b
}

// SignPackage signs p with the control-plane key.
func (s *Signer) SignPackage(p Package) Package {
	p.KeyID = s.ID
	p.Signature = ""
	p.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(s.Priv, p.payload()))
	return p
}

// VerifyPackage checks the package signature. Each job's manifest and artifact
// must still be verified separately against the release keyring.
func (k Keyring) VerifyPackage(p Package) error {
	pub, ok := k[p.KeyID]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownKey, p.KeyID)
	}
	sig, err := base64.StdEncoding.DecodeString(p.Signature)
	if err != nil || !ed25519.Verify(pub, p.payload(), sig) {
		return ErrBadSignature
	}
	return nil
}
