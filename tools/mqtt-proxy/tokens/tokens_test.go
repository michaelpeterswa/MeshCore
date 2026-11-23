package tokens

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const dev = "6C265917E1D4BA04FB14A9C2C985B5143FD74C9DA89472C2D731A9A1413A9FAD"
const aud = "mqtt-us-v1.letsmesh.net"

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newCache() (*Cache, *clock) {
	c := &clock{t: time.Unix(1795400000, 0)}
	return New(c.now), c
}

func payload(username, audience, jwt string, iat, exp int64) []byte {
	return []byte(fmt.Sprintf(`{"audience":%q,"username":%q,"token":%q,"iat":%d,"exp":%d}`, audience, username, jwt, iat, exp))
}

func TestPutAndGet(t *testing.T) {
	c, _ := newCache()
	if _, err := c.Put(dev, aud, payload(dev, aud, "jwt-a", 1795399000, 1795486000)); err != nil {
		t.Fatal(err)
	}
	tok, ok := c.Get(dev, aud)
	if !ok || tok.Username != "v1_"+dev || tok.JWT != "jwt-a" || tok.Audience != aud || tok.Device != dev ||
		!tok.IssuedAt.Equal(time.Unix(1795399000, 0)) || !tok.ExpiresAt.Equal(time.Unix(1795486000, 0)) {
		t.Fatalf("%+v %v", tok, ok)
	}
}

func TestRejectsExpiredToken(t *testing.T) {
	// A gateway that booted without NTP signs tokens dated 2024.
	c, _ := newCache()
	if _, err := c.Put(dev, aud, payload(dev, aud, "old", 1715850000, 1715936400)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := c.Get(dev, aud); ok {
		t.Fatal("expired token cached")
	}
}

func TestRejectsUsernameDeviceMismatch(t *testing.T) {
	c, _ := newCache()
	other := strings.Repeat("AB", 32)
	if _, err := c.Put(dev, aud, payload(other, aud, "x", 1795399000, 1795486000)); err == nil {
		t.Fatal("accepted another device's token")
	}
}

func TestRejectsAudienceMismatch(t *testing.T) {
	c, _ := newCache()
	if _, err := c.Put(dev, aud, payload(dev, "mqtt-eu-v1.letsmesh.net", "x", 1795399000, 1795486000)); err == nil {
		t.Fatal("accepted a token for another audience")
	}
}

func TestLowercaseDeviceMatches(t *testing.T) {
	c, _ := newCache()
	if _, err := c.Put(strings.ToLower(dev), aud, payload(strings.ToLower(dev), aud, "x", 1795399000, 1795486000)); err != nil {
		t.Fatal(err)
	}
	tok, ok := c.Get(dev, aud)
	if !ok || tok.Username != "v1_"+dev {
		t.Fatalf("%+v %v", tok, ok)
	}
}

func TestOlderTokenDoesNotReplaceNewer(t *testing.T) {
	c, _ := newCache()
	c.Put(dev, aud, payload(dev, aud, "new", 1795399500, 1795486000))
	if _, err := c.Put(dev, aud, payload(dev, aud, "older", 1795399000, 1795486000)); err == nil {
		t.Fatal("older token accepted")
	}
	if tok, _ := c.Get(dev, aud); tok.JWT != "new" {
		t.Fatalf("got %q", tok.JWT)
	}
}

func TestSameTokenAgainIsNotAnError(t *testing.T) {
	// The broker replays the retained token on every proxy reconnect.
	c, _ := newCache()
	p := payload(dev, aud, "a", 1795399000, 1795486000)
	c.Put(dev, aud, p)
	ch := c.Changed(dev, aud)
	if _, err := c.Put(dev, aud, p); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("replay signalled a change")
	default:
	}
}

func TestNewerReplacesAndSignalsChanged(t *testing.T) {
	c, _ := newCache()
	c.Put(dev, aud, payload(dev, aud, "a", 1795399000, 1795486000))
	ch := c.Changed(dev, aud)
	c.Put(dev, aud, payload(dev, aud, "b", 1795399900, 1795486900))
	select {
	case <-ch:
	default:
		t.Fatal("Changed not closed")
	}
	if tok, _ := c.Get(dev, aud); tok.JWT != "b" {
		t.Fatalf("got %q", tok.JWT)
	}
}

func TestChangedBeforeFirstToken(t *testing.T) {
	c, _ := newCache()
	ch := c.Changed(dev, aud)
	c.Put(dev, aud, payload(dev, aud, "a", 1795399000, 1795486000))
	select {
	case <-ch:
	default:
		t.Fatal("first token did not signal")
	}
}

func TestGetFalseAfterExpiry(t *testing.T) {
	c, clk := newCache()
	c.Put(dev, aud, payload(dev, aud, "a", 1795399000, 1795400100))
	clk.t = time.Unix(1795400100, 0)
	if _, ok := c.Get(dev, aud); ok {
		t.Fatal("expired token returned")
	}
}

func TestMalformed(t *testing.T) {
	c, _ := newCache()
	for _, p := range []string{`not json`, `[]`, `{"audience":"` + aud + `","username":"` + dev + `","iat":1,"exp":1795486000}`} {
		if _, err := c.Put(dev, aud, []byte(p)); err == nil {
			t.Errorf("accepted %s", p)
		}
	}
}
