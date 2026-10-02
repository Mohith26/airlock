package bundle

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func seed(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestSignedManifestVerifies(t *testing.T) {
	s, _ := NewSigner(seed(1))
	art := []byte("payments-api v1.4.2 binary")
	m := s.Sign("payments-api", "1.4.2", art, time.Unix(0, 0))
	if err := NewKeyring(s.Pub).Verify(m, art); err != nil {
		t.Fatal(err)
	}
}

func TestTamperedArtifactRejected(t *testing.T) {
	s, _ := NewSigner(seed(1))
	art := []byte("payments-api v1.4.2 binary")
	m := s.Sign("payments-api", "1.4.2", art, time.Unix(0, 0))
	bad := append([]byte(nil), art...)
	bad[3] ^= 0x01
	if err := NewKeyring(s.Pub).Verify(m, bad); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestTamperedManifestRejected(t *testing.T) {
	s, _ := NewSigner(seed(1))
	art := []byte("x")
	m := s.Sign("svc", "1.0.0", art, time.Unix(0, 0))
	m.Version = "9.9.9"
	if err := NewKeyring(s.Pub).Verify(m, art); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("got %v", err)
	}
}

func TestUntrustedKeyRejected(t *testing.T) {
	trusted, _ := NewSigner(seed(1))
	attacker, _ := NewSigner(seed(2))
	art := []byte("x")
	m := attacker.Sign("svc", "1.0.0", art, time.Unix(0, 0))
	if err := NewKeyring(trusted.Pub).Verify(m, art); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("got %v", err)
	}
	// Even claiming the trusted key id does not help without the private key.
	m.KeyID = trusted.ID
	if err := NewKeyring(trusted.Pub).Verify(m, art); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("got %v", err)
	}
}

func TestOfflinePackageSignature(t *testing.T) {
	rel, _ := NewSigner(seed(1))
	cp, _ := NewSigner(seed(3))
	art := []byte("bin")
	m := rel.Sign("svc", "2.0.0", art, time.Unix(0, 0))
	p := cp.SignPackage(Package{Region: "adc-1", Sequence: 4, IssuedAt: time.Unix(10, 0), Jobs: []OfflineJob{{JobID: "j1", Generation: 7, Manifest: m}}, Artifacts: map[string][]byte{m.Digest: art}})
	ring := NewKeyring(cp.Pub)
	if err := ring.VerifyPackage(p); err != nil {
		t.Fatal(err)
	}
	p.Jobs[0].Generation = 99
	if err := ring.VerifyPackage(p); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("modified package accepted: %v", err)
	}
}
