# WisMesh MQTT LAN Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go service that subscribes to the gateway's LAN broker and forwards only the analyzer topics (`status`, `packets`, `raw`, `neighbors`) to public MeshCore analyzers over WSS/mqtts. Each device authenticates with the JWT it signs itself; room topics never leave the LAN.

**Architecture:** One binary, `tools/mqtt-proxy`. A local subscriber feeds every `meshcore/+/+/#` message to a router. The router caches `auth/{audience}` tokens and runs everything else through a table-driven topic policy. It then enqueues each forwardable message on a lazily opened per-device, per-upstream session. Each session owns one paho client, a bounded drop-oldest queue and a reconnect loop.

**Tech Stack:** Go 1.26, `github.com/eclipse/paho.mqtt.golang` v1.5.x (MQTT 3.1.1, `tcp`/`ssl`/`wss`), `gopkg.in/yaml.v3`, `github.com/prometheus/client_golang`, `log/slog`. Tests use `github.com/mochi-mqtt/server/v2` as an embedded broker. Distroless static image.

**Spec:** `docs/superpowers/specs/2026-09-13-wismesh-room-server-mqtt-observer-design.md`, Part 2 (2.1–2.8). Firmware facts it must match: `src/helpers/MQTTPresets.h`, `src/helpers/MQTTTopicRouter.h`, `src/helpers/MQTTEthernetPayloads.h` (`buildAuthMessage`), `src/helpers/bridges/MQTTBridge.cpp:2298` (JWT username).

## Global Constraints

- Module path `github.com/agessaman/MeshCore/tools/mqtt-proxy`; all code under `tools/mqtt-proxy/`. Internal packages per spec 2.1: `config`, `local`, `tokens`, `upstream`, `policy`, `metrics`, plus `presets` and `proxy` (wiring) and `cmd/mqtt-proxy`.
- Holds no device keys. JWT credentials come only from retained `auth/{audience}` messages: `{"audience","username","token","iat","exp"}`.
- Upstream JWT login: username `v1_` + device public key hex **uppercase** (ESP32 observer, `MQTTBridge.cpp:2298`), password = token. The auth message's `username` is the bare hex. The proxy adds the prefix.
- Forward allowlist: `status`, `packets`, `raw`, `neighbors`. Never forward `room/*`, `link`, `auth/*` or any other suffix.
- MeshCore style: topic unchanged `meshcore/{iata}/{device}/{type}`. MeshRank style: `meshrank/uplink/{token}/{device}/{type}`, never `raw`, token from YAML.
- QoS: `status` and `neighbors` QoS 1, `packets` and `raw` QoS 0. Retain upstream only for `status`, and only when the preset's `allow_retain` is true.
- TLS verified against system roots. The only override is a per-upstream `ca_file`, for tests and private brokers.
- Defaults: session idle timeout 10m, queue 256 messages per session, renew-before 10m, backoff 1s doubling to 5m with ±20% jitter, HTTP `:8080`.
- Structured logs (`slog` JSON) to stdout. `/healthz` and `/metrics` on the HTTP listener.

## Review Focus

1. **Retained deliveries on (re)subscribe.** The local broker replays retained `status`, `link`, `room/stats`, `auth/*` whenever the proxy connects. Only `auth/*` may be acted on. A retained `status` must not be forwarded: it is stale, and the device republishes on its interval. Test in Task 7.
2. **Bad-clock tokens.** A device that booted without NTP signs tokens dated 2024 (seen on the tower unit). The cache must refuse an already-expired token, log once per device+audience, and never open a session with it. Test in Task 4.
3. **Device hex case.** Topics carry uppercase hex. A lowercase device segment or auth `username` must still match the same device, and the upstream username is always uppercase. Tests in Tasks 3 and 4.
4. **Malformed forwardable payloads.** Non-JSON or non-object payloads on `packets`/`status`, and topics with extra or missing segments, are dropped and counted, and the subscriber keeps running. Tests in Tasks 3 and 7.
5. **Upstream down for hours.** Memory stays bounded (queue drop-oldest, drops counted), and when the upstream returns the session reconnects with the newest cached token, not the one it started with. Test in Task 6.

---

### Task 1: Module scaffold and preset table with firmware parity

**Files:**
- Create: `tools/mqtt-proxy/go.mod`, `tools/mqtt-proxy/presets/presets.go`, `tools/mqtt-proxy/presets/presets_test.go`

**Interfaces:**
- Produces:
  ```go
  type AuthType int   // AuthNone, AuthUserPass, AuthJWT
  type TopicStyle int // StyleMeshCore, StyleMeshRank
  type Preset struct {
      Name, URL, Audience string
      Auth        AuthType
      Style       TopicStyle
      AllowRetain bool
      KeepAlive   time.Duration // 0 = paho default
      Username, Password string  // embedded USERPASS creds; Username "{pubkey}" = device hex
  }
  const UsernamePubkey = "{pubkey}"
  func Lookup(name string) (Preset, bool)
  func All() []Preset
  ```

- [ ] **Step 1: Write the failing parity test.** `TestPresetsMatchFirmware` reads `../../../src/helpers/MQTTPresets.h` and extracts every `{ "name", "url", aud|nullptr, cert, MQTT_AUTH_*, MQTT_TOPIC_*, lifetime, retain, keepalive, user|nullptr|MQTT_USERPASS_USERNAME_PUBKEY, pass|nullptr },` row with a regexp. It asserts `All()` has the same names in the same order, with equal URL, audience, auth, style, allow_retain, keepalive and embedded user/pass. Also `TestLookup`: `Lookup("analyzer-us")` → URL `wss://mqtt-us-v1.letsmesh.net:443/mqtt`, audience `mqtt-us-v1.letsmesh.net`, `AuthJWT`, `AllowRetain` true, KeepAlive 55s; `Lookup("meshrank")` → `StyleMeshRank`, `AuthNone`; `Lookup("nope")` → false.
- [ ] **Step 2: Run** `cd tools/mqtt-proxy && go test ./presets/` → FAIL (package does not exist).
- [ ] **Step 3: Create `go.mod`** (`go 1.26`), then `presets.go` with the table transcribed from `MQTTPresets.h`. The CA column is dropped (system roots), as is token lifetime (the device decides).
- [ ] **Step 4: Run** `go test ./presets/` → PASS. Then corrupt one URL locally and confirm the parity test fails, and revert.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): scaffold module and preset table with firmware parity test`

### Task 2: Config

**Files:**
- Create: `tools/mqtt-proxy/config/config.go`, `tools/mqtt-proxy/config/config_test.go`, `tools/mqtt-proxy/config/testdata/*.yaml`

**Interfaces:**
- Consumes: `presets.Lookup`
- Produces:
  ```go
  type Config struct {
      Listen   string        // default ":8080"
      Local    Local
      Session  Session
      Upstreams []Upstream
  }
  type Local struct { Broker, ClientID, Username, Password string } // ClientID default "meshcore-mqtt-proxy"
  type Session struct { IdleTimeout, RenewBefore time.Duration; QueueSize int }
  type Upstream struct {
      Name   string
      Preset presets.Preset // resolved; for `url:` upstreams a synthetic preset named "custom"
      MeshRankToken string
      CAFile string
  }
  func Load(path string) (*Config, error)
  ```
  YAML keys: `listen`, `local.{broker,client_id,username,password,password_file}`, `session.{idle_timeout,renew_before,queue_size}`, `upstreams[].{name,preset,url,audience,username,password,password_file,meshrank_token,ca_file,allow_retain}`. `*_file` is read and trimmed, and wins over the inline value.

- [ ] **Step 1: Write failing tests:** `TestLoadMinimal` (one `preset: analyzer-us` upstream → defaults applied: Listen `:8080`, IdleTimeout 10m, RenewBefore 10m, QueueSize 256, ClientID `meshcore-mqtt-proxy`). `TestPasswordFileWins`. `TestCustomURLUpstream` (`url: mqtts://broker.example:8883`, username/password → Auth `AuthUserPass`, Style MeshCore). Then errors, each with a message naming the field: unknown preset, both `preset` and `url`, neither, duplicate upstream name, meshrank without `meshrank_token`, `queue_size: 0`, local broker missing, URL scheme not `mqtt`/`mqtts`/`wss`/`ws`.
- [ ] **Step 2: Run** `go test ./config/` → FAIL.
- [ ] **Step 3: Implement `Load`.** Decode with `yaml.v3` `KnownFields(true)` so typos fail.
- [ ] **Step 4: Run** `go test ./config/` → PASS.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): YAML config with preset resolution and validation`

### Task 3: Topic policy

**Files:**
- Create: `tools/mqtt-proxy/policy/policy.go`, `tools/mqtt-proxy/policy/policy_test.go`

**Interfaces:**
- Consumes: `presets.TopicStyle`, `presets.Preset`
- Produces:
  ```go
  type Topic struct { IATA, Device, Suffix string } // Device uppercased
  func Parse(topic string) (Topic, bool)           // meshcore/{iata}/{device}/{suffix...}
  func IsAuth(t Topic) (audience string, ok bool)  // suffix "auth/<aud>"
  type Route struct { Topic string; QoS byte; Retain bool }
  func Forward(t Topic, p presets.Preset, meshrankToken string) (Route, bool)
  ```

- [ ] **Step 1: Write failing table tests.** Parse: accepts `meshcore/SEA/<64 hex>/packets` and `.../room/posts` (Suffix `room/posts`) and `.../auth/mqtt-us-v1.letsmesh.net`. A lowercase device comes back uppercase. Rejects: wrong prefix, IATA not 3 alnum, IATA `XXX`, device not 64 hex, empty suffix, fewer than 4 segments. Forward mirrors `mqttBuildPublicationTopic`: MeshCore style passes the four allowlisted types unchanged with QoS 1/0/0/1. Retain is true only for `status` with `AllowRetain`, and false for `status` on a preset with `AllowRetain` false (`waev`). MeshRank gives `meshrank/uplink/TOK/<dev>/packets`, drops `raw`, and drops everything when the token is empty. `room/posts`, `room/events`, `room/dm`, `room/stats`, `link`, `auth/x` and `unknown` → not forwarded for every style.
- [ ] **Step 2: Run** `go test ./policy/` → FAIL.
- [ ] **Step 3: Implement.** The allowlist is a `map[string]byte` of suffix → QoS; no other suffix passes.
- [ ] **Step 4: Run** `go test ./policy/` → PASS.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): table-driven topic policy mirroring MQTTTopicRouter`

### Task 4: Token cache

**Files:**
- Create: `tools/mqtt-proxy/tokens/tokens.go`, `tools/mqtt-proxy/tokens/tokens_test.go`

**Interfaces:**
- Produces:
  ```go
  type Token struct { Device, Audience, Username, JWT string; IssuedAt, ExpiresAt time.Time }
  type Cache struct { /* mutex-guarded */ }
  func New(now func() time.Time) *Cache
  func (c *Cache) Put(device string, payload []byte) (Token, error) // payload = auth JSON
  func (c *Cache) Get(device, audience string) (Token, bool)          // false if missing or expired
  func (c *Cache) Changed(device, audience string) <-chan struct{}    // closed on the next accepted Put for this key
  ```
  `Username` on the returned Token is the upstream login form `v1_<UPPERCASE HEX>`.

- [ ] **Step 1: Write failing tests:** `TestPutAndGet` (Username `v1_6C26...`, JWT, times from `iat`/`exp`). `TestRejectsExpiredToken` (exp ≤ now → error, Get false: Review Focus 2). `TestRejectsUsernameDeviceMismatch` (auth `username` ≠ topic device, case-insensitive compare → error). `TestOlderTokenDoesNotReplaceNewer` (lower `iat` ignored). `TestNewerReplacesAndSignalsChanged` (channel from `Changed` closes). `TestGetFalseAfterExpiry` (advance the fake clock). `TestMalformedJSON` → error. `TestLowercaseDeviceMatches`.
- [ ] **Step 2: Run** `go test ./tokens/` → FAIL.
- [ ] **Step 3: Implement.** The key is `device|audience`, with the device uppercased. `Changed` returns the current per-key channel, which `Put` closes and replaces.
- [ ] **Step 4: Run** `go test -race ./tokens/` → PASS.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): token cache keyed by device and audience`

### Task 5: Queue and backoff

**Files:**
- Create: `tools/mqtt-proxy/upstream/queue.go`, `tools/mqtt-proxy/upstream/backoff.go`, `tools/mqtt-proxy/upstream/queue_test.go`, `tools/mqtt-proxy/upstream/backoff_test.go`

**Interfaces:**
- Produces:
  ```go
  type Message struct { Topic string; Payload []byte; QoS byte; Retain bool }
  type Queue struct { /* bounded, goroutine-safe */ }
  func NewQueue(capacity int) *Queue
  func (q *Queue) Push(m Message) (dropped bool) // drops the oldest when full
  func (q *Queue) Pop(ctx context.Context) (Message, error) // blocks until a message or ctx done
  func (q *Queue) Unpop(m Message)  // put back at the front after a failed publish (drops it if full)
  func (q *Queue) Len() int
  func (q *Queue) Drops() uint64
  type Backoff struct { Min, Max time.Duration; Jitter float64; Rand func() float64 }
  func (b *Backoff) Next() time.Duration
  func (b *Backoff) Reset()
  ```

- [ ] **Step 1: Write failing tests:** FIFO order. Capacity 3, push 5 → Len 3, Drops 2, oldest two gone. `Pop` blocks then returns on `Push`. `Pop` returns `ctx.Err()` on cancel. `Unpop` goes to the front. Backoff with `Rand` fixed at 0.5 (no jitter offset) → 1s, 2s, 4s … capped at 5m. `Reset` → 1s. With `Rand` at 0 and 1, results within ±20%.
- [ ] **Step 2: Run** `go test ./upstream/` → FAIL.
- [ ] **Step 3: Implement.** Use a ring slice plus `sync.Mutex` and a signal channel.
- [ ] **Step 4: Run** `go test -race ./upstream/` → PASS.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): bounded drop-oldest queue and jittered backoff`

### Task 6: Upstream session

**Files:**
- Create: `tools/mqtt-proxy/upstream/session.go`, `tools/mqtt-proxy/upstream/session_test.go`, `tools/mqtt-proxy/internal/testbroker/testbroker.go` (shared mochi helper)

**Interfaces:**
- Consumes: `Queue`, `Backoff`, `tokens.Cache`, `config.Upstream`, `config.Session`
- Produces:
  ```go
  type Credentials func() (username, password string, ok bool)
  type SessionConfig struct {
      Upstream    config.Upstream
      Device      string
      Creds       Credentials      // JWT: from tokens.Cache.Get; userpass: static
      TokenChanged func() <-chan struct{} // nil for non-JWT
      TokenExpiry func() time.Time         // zero for non-JWT
      Session     config.Session
      Logger      *slog.Logger
      Hooks       Hooks             // metrics callbacks, may be zero
  }
  type Hooks struct { Connected func(bool); Published func(); Dropped func(reason string); Reconnect func() }
  func NewSession(cfg SessionConfig) *Session
  func (s *Session) Enqueue(m Message)
  func (s *Session) Run(ctx context.Context) // returns when ctx done or idle timeout elapses
  func (s *Session) Done() <-chan struct{}
  ```
  testbroker: `func Start(t *testing.T, opts Options) *Broker`. `Options{TCP bool; WSS bool; Auth func(user, pass string) bool}`. `Broker` has `URL`, `CAFile`, and `Received() []Received` (topic, payload, qos, retain, username).

- [ ] **Step 1: Write failing tests against a WSS testbroker with a self-signed CA:**
  `TestPublishesWithJWTCredentials`: the broker sees username `v1_<HEX>` and the token as password, and a status message arrives QoS 1 retained.
  `TestRejectedTokenWaitsForNewToken`: Auth rejects token A. The session logs once, makes no further connect attempts for 3× the backoff, then connects within 1s of `TokenChanged` firing with token B.
  `TestReconnectsWithNewestTokenAfterOutage`: broker stopped, two tokens rotated, broker restarted on the same port → the login uses the latest (Review Focus 5).
  `TestQueueBoundedWhileDown`: 1000 enqueues with capacity 256 while the upstream is down → `Dropped("queue_full")` called 744 times.
  `TestRenewsBeforeExpiry`: token expiring within `RenewBefore` with a newer token available → disconnect and reconnect with the newer one.
  `TestIdleTimeoutEndsRun`: `IdleTimeout` 200ms → `Done` closes.
- [ ] **Step 2: Run** `go test ./upstream/ -run Session` → FAIL.
- [ ] **Step 3: Implement.** paho options: `SetAutoReconnect(false)`, `SetCredentialsProvider`, `SetKeepAlive(preset.KeepAlive)` when non-zero, `SetCleanSession(true)`, client ID `mcproxy-<first 16 hex of device>`. The URL scheme maps `mqtts`→`ssl`, `mqtt`→`tcp`, `wss`/`ws` unchanged. TLS config uses system roots plus `CAFile` when set. Connect errors `packets.ErrorRefusedBadUsernameOrPassword` / `ErrorRefusedNotAuthorised` mean "rejected": wait on `TokenChanged` (or forever for non-JWT, logged at error level). Other errors use backoff. A publish failure `Unpop`s once and reconnects.
- [ ] **Step 4: Run** `go test -race ./upstream/` → PASS.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): per-device upstream session with token-aware reconnect`

### Task 7: Local subscriber, router, metrics, HTTP, main

**Files:**
- Create: `tools/mqtt-proxy/local/subscriber.go`, `tools/mqtt-proxy/metrics/metrics.go`, `tools/mqtt-proxy/proxy/router.go`, `tools/mqtt-proxy/proxy/router_test.go`, `tools/mqtt-proxy/cmd/mqtt-proxy/main.go`

**Interfaces:**
- Consumes: everything above.
- Produces:
  ```go
  // local
  type Handler func(topic string, payload []byte, retained bool)
  func Run(ctx context.Context, cfg config.Local, h Handler, connected func(bool), log *slog.Logger) error // subscribes meshcore/+/+/# QoS 1; resubscribes on reconnect
  // proxy
  type Router struct { /* sessions map[device|upstream]*upstream.Session */ }
  func NewRouter(cfg *config.Config, cache *tokens.Cache, m *metrics.Metrics, log *slog.Logger) *Router
  func (r *Router) Handle(topic string, payload []byte, retained bool)
  func (r *Router) Run(ctx context.Context) // owns session goroutines, reaps finished ones
  // metrics (Prometheus, prefix meshcore_proxy_)
  //   messages_in_total{device}, messages_out_total{device,upstream}, dropped_total{upstream,reason},
  //   malformed_total, reconnects_total{upstream}, token_expiry_seconds{device,audience} (time until exp),
  //   upstream_connected{device,upstream}, local_connected
  ```
  HTTP: `/healthz` is 200 `ok` while the local broker is connected and 503 otherwise. `/metrics` is promhttp. Flags: `-config /etc/mqtt-proxy/config.yaml`.

- [ ] **Step 1: Write failing router tests** (fake session factory injected through an unexported field):
  `TestAuthPopulatesCacheEvenWhenRetained`.
  `TestRetainedStatusIsNotForwarded` (Review Focus 1).
  `TestRoomAndLinkNeverReachASession` (all room suffixes, `link`, `auth/*`).
  `TestNonJSONPacketDroppedAndCounted` (`malformed_total` = 1, the next good message still forwarded: Review Focus 4).
  `TestNoTokenSkipsJWTUpstreamLoggedOnce` (two messages, one log line, a userpass upstream still gets both).
  `TestOneSessionPerDevicePerUpstream`.
  `TestFinishedSessionIsReplacedOnNextMessage`.
- [ ] **Step 2: Run** `go test ./proxy/` → FAIL.
- [ ] **Step 3: Implement.** A forwardable payload must be a JSON object (`json.Valid` plus first non-space byte `{`). The router creates sessions lazily and logs "no token for audience" once per device+audience until a token arrives. `main` wires config → cache → router → local.Run, serves HTTP, and handles SIGTERM with a 5s drain.
- [ ] **Step 4: Run** `go test -race ./...` → PASS. Then `go vet ./...` → clean.
- [ ] **Step 5: Commit** `feat(mqtt-proxy): local subscriber, router, metrics and entrypoint`

### Task 8: End-to-end test

**Files:**
- Create: `tools/mqtt-proxy/proxy/e2e_test.go`, `tools/mqtt-proxy/proxy/testdata/device/*.json` (real payloads captured from the tower gateway: status, packets, neighbors, link, room/stats, auth, with the token replaced by a synthetic JWT whose exp is a year ahead)

**Interfaces:**
- Consumes: `testbroker`, `local.Run`, `Router`, `config.Load`.

Deviation from spec 2.8: the spec's integration test starts mosquitto in a container. This one uses embedded mochi brokers so `go test` needs no Docker and also covers the WSS/TLS path. Real mosquitto is exercised by the compose smoke test in Task 9 and by the deployment.

- [ ] **Step 1: Write the test.** Start a local TCP testbroker and two fake upstreams: a WSS JWT one using the `analyzer-us` preset with a URL/`ca_file` override, and a TLS one using `meshrank` with a token. Run the proxy in-process. Publish the device's retained auth first, then status, 3 packets, a raw, neighbors, link, room/posts, room/events, room/dm and room/stats. Assert:
  - JWT upstream: exactly status, 3 packets, raw and neighbors on the original topics, username `v1_<HEX>`, password = token.
  - MeshRank upstream: status, packets and neighbors under `meshrank/uplink/TOK/<HEX>/`, no raw.
  - Neither upstream ever saw a topic containing `/room/`, `/link` or `/auth/`.
- [ ] **Step 2: Run** `go test -race ./proxy/ -run E2E` → PASS. If it fails, fix the owning package with a unit test first.
- [ ] **Step 3: Commit** `test(mqtt-proxy): end-to-end forwarding and room-topic isolation`

### Task 9: Image, CI, docs

**Files:**
- Create: `tools/mqtt-proxy/Dockerfile`, `tools/mqtt-proxy/config.example.yaml`, `tools/mqtt-proxy/README.md`, `tools/mqtt-proxy/compose.smoke.yaml`, `.github/workflows/mqtt-proxy.yml`

- [ ] **Step 1: Dockerfile.** Multi-stage `golang:1.26` → `gcr.io/distroless/static-debian12:nonroot`, `CGO_ENABLED=0`, `EXPOSE 8080`, entrypoint `/mqtt-proxy -config /etc/mqtt-proxy/config.yaml`.
- [ ] **Step 2: Compose smoke test.** `compose.smoke.yaml` runs mosquitto (anonymous, LAN stand-in), the proxy built locally, and a second mosquitto standing in for the analyzer (custom `url: mqtt://analyzer:1883` upstream with user/pass). README documents `docker compose -f compose.smoke.yaml up`, then `mosquitto_pub` of a testdata file, and seeing it arrive with `mosquitto_sub` on the analyzer side. Run it once → the status arrives and `room/posts` does not.
- [ ] **Step 3: Workflow.** On push or PR touching `tools/mqtt-proxy/**`: `go vet`, `go test -race ./...`. On push to `feat/nrf52-ethernet-observer` or tags `mqtt-proxy-v*`: build multi-arch (amd64, arm64) and push `ghcr.io/${{ github.repository_owner }}/meshcore-mqtt-proxy` tagged with the short SHA and tag.
- [ ] **Step 4: README.** Covers the config reference (every YAML key, its default and when it is required), metrics list, the `mqtt.auth.aud` firmware setting each JWT upstream needs, and the clock caveat.
- [ ] **Step 5: Commit** `build(mqtt-proxy): container image, CI workflow, smoke test and docs`

### Task 10: Deploy on rattle (GitOps repo `rattlesnakemountain/kubernetes`)

**Files (in that repo, on a branch, via PR):**
- Modify: `apps/mosquitto/secrets.sops.yaml` (add user `mqtt-proxy`; hash in Mosquitto 2.0 format, see that repo's README), `apps/mosquitto/mosquitto.yaml` (bump `rattlesnakemtn.com/passwd-revision`)
- Create: `apps/mqtt-proxy/{namespace.yaml,kustomization.yaml,deployment.yaml,secrets.sops.yaml,config/config.yaml}`
- Modify: `clusters/rattle/apps.yaml` (Kustomization `mqtt-proxy`, `dependsOn: [infra-configs, mosquitto]`), `README.md` (section)

- [ ] **Step 1:** Config: local broker `tcp://mosquitto.mosquitto.svc.cluster.local:1883`, user `mqtt-proxy`, `password_file: /secrets/local-password`; one upstream `letsmesh-us` (`preset: analyzer-us`). Image pinned by SHA tag from Task 9. ConfigMap generated from `config/config.yaml`. `readinessProbe`/`livenessProbe` on `/healthz`. Resources: 10m CPU / 32Mi request, 128Mi limit, non-root, read-only root fs.
- [ ] **Step 2:** `kustomize build apps/mqtt-proxy` and `kustomize build apps/mosquitto` succeed. Open a PR and wait for the user to merge.
- [ ] **Step 3: Verify after merge.** Pod Ready. Proxy log shows a session for device `6C265917…` connected to `letsmesh-us`. The gateway appears on the letsmesh analyzer map/feed. The proxy logs no `room/` topic, `meshcore_proxy_messages_out_total` is rising, and `dropped_total` is flat.
