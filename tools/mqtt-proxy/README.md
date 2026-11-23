# MeshCore MQTT LAN proxy

Forwards observer traffic from MeshCore Ethernet gateways (the RAK WisMesh Ethernet Gateway room server, `RAK_4631_room_server_observer_mqtt_ethernet`) to public analyzers.

The gateway speaks plain MQTT to a broker on the LAN; it has no TLS. The proxy subscribes to that broker and republishes the analyzer topics over WSS or mqtts, logging in as each device with the JWT the device signs itself. The proxy holds no device keys.

Design: `docs/superpowers/specs/2026-09-13-wismesh-room-server-mqtt-observer-design.md`, Part 2.

## What leaves the LAN

| Local topic `meshcore/{iata}/{device}/…` | Upstream |
|---|---|
| `status` | same topic, QoS 1, retained if the preset allows |
| `packets`, `raw` | same topic, QoS 0 |
| `neighbors` | same topic, QoS 1 |
| `room/*`, `link`, `auth/*`, anything else | **never forwarded** |

For the `meshrank` preset the topic becomes `meshrank/uplink/{token}/{device}/{type}`, and `raw` is not sent.

Messages the local broker replays as retained when the proxy (re)connects are not forwarded. They are stale, and the device republishes on its own interval. The one exception is `auth/{audience}`, which the proxy reads to fill its token cache. Payloads on forwardable topics must be JSON objects; anything else is dropped and counted.

## Device tokens

For each JWT upstream, the device must publish a token for that preset's audience:

```
set mqtt.auth.aud mqtt-us-v1.letsmesh.net,mqtt-v1.cascadiamesh.org
```

The gateway publishes `meshcore/{iata}/{device}/auth/{audience}` (retained) with `{"audience","username","token","iat","exp"}`. The proxy logs in upstream as `v1_<DEVICE HEX>` with the token as password, the same as the ESP32 observer firmware. If a device has no token for an upstream's audience, its messages for that upstream are dropped (`dropped_total{reason="no_token"}`) and the proxy logs it once.

**Clock:** tokens carry the device's notion of time. A gateway that has not synced NTP signs tokens dated 2024, which the proxy refuses as expired (logged once per device and audience). Firmware from `2850e34c` onward syncs NTP without waiting for a broker.

When an upstream refuses a token, the session stops trying until the device publishes a newer one. When the token in use is within `renew_before` of expiring and a newer one is cached, the session reconnects with it.

## Configuration

`-config` (default `/etc/mqtt-proxy/config.yaml`), `-debug`. See `config.example.yaml`. Unknown keys are an error.

| Key | Default | Notes |
|---|---|---|
| `listen` | `:8080` | `/healthz` (200 while the local broker is connected, else 503) and `/metrics` |
| `local.broker` | required | `tcp://host:1883` (also `mqtt://`, `ssl://`, `mqtts://`) |
| `local.client_id` | `meshcore-mqtt-proxy` | |
| `local.username`, `local.password` | none | `local.password_file` wins over `local.password` |
| `session.idle_timeout` | `10m` | closes a device's upstream connection after this long without traffic |
| `session.renew_before` | `10m` | see Device tokens |
| `session.queue_size` | `256` | per device per upstream; the oldest message is dropped when full |
| `upstreams[].name` | required | unique; used in logs and metrics |
| `upstreams[].preset` | one of `preset`/`url` | a name from `src/helpers/MQTTPresets.h` (`analyzer-us`, `cascadiamesh`, `meshrank`, …) |
| `upstreams[].url` | | `mqtt://`, `mqtts://`, `ws://`, `wss://`; with `preset`, overrides its URL |
| `upstreams[].audience` | preset's | with `url` alone, makes it a JWT upstream |
| `upstreams[].username`, `password`, `password_file` | preset's | with `url` alone and a username, makes it a user/password upstream |
| `upstreams[].meshrank_token` | | required for `meshrank` |
| `upstreams[].ca_file` | none | extra trust root, added to the system roots |
| `upstreams[].allow_retain` | preset's; `false` for `url` | retain flag on upstream `status` |

TLS is always verified. The preset table is a copy of the firmware's, and `presets` tests fail if they drift.

## Metrics

All are prefixed `meshcore_proxy_`.

| Metric | Labels | Meaning |
|---|---|---|
| `messages_in_total` | `device` | forwardable messages received locally |
| `messages_out_total` | `device`, `upstream` | messages published upstream |
| `dropped_total` | `upstream`, `reason` | `queue_full`, `no_token` |
| `malformed_total` | | unparseable topics, non-JSON payloads |
| `reconnects_total` | `upstream` | upstream connections re-established after a loss |
| `token_expiry_timestamp_seconds` | `device`, `audience` | when the cached token expires |
| `upstream_connected` | `device`, `upstream` | 1 while connected |
| `local_connected` | | 1 while connected to the LAN broker |

## Development

```sh
go test -race ./...                                   # unit + end-to-end (embedded brokers, no Docker)
docker compose -f compose.smoke.yaml up --build -d    # real mosquitto on both sides
./smoke/run.sh                                        # PASS: status/packets forwarded, room/link not
docker compose -f compose.smoke.yaml down
```

CI (`.github/workflows/mqtt-proxy.yml`) runs vet and tests on changes here or to `MQTTPresets.h`. On push, it builds `ghcr.io/<owner>/meshcore-mqtt-proxy:<short sha>` for amd64 and arm64.
