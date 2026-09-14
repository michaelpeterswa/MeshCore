# Hardware verification checklist (consolidated from task reports)

## task-8-report.md
- Hardware verification is still fully pending (see the "PENDING hardware
  verification" section above) — none of this fix has been exercised against
  real DNS, a real broker, or real NTP traffic.


## What was implemented
## PENDING hardware verification

No device was available in this environment. The following checklist from the brief's Step 7
has NOT been run and needs a real RAK4631 + RAK13800(W5100S) + reachable MQTT broker:

- [ ] Flash the gateway (`pio run -e RAK_4631_room_server_observer_mqtt_ethernet -t upload`)
- [ ] Open a serial monitor at 115200
- [ ] `set mqtt.iata SEA`
- [ ] `set mqtt1.preset custom`
- [ ] `set mqtt1.server 192.168.1.10`
- [ ] `set mqtt1.port 1883`
- [ ] `get mqtt.config.valid` (expected to still answer through the Task 1 stub CLI)
- [ ] On the broker host: `mosquitto_sub -h 192.168.1.10 -v -t 'meshcore/#'`
- [ ] Expected within one status interval: one retained `.../link` message with
      `"state":"up"`
- [ ] Expected within one status interval: one `.../status` message with `"status":"online"`
      and a `stats` object
- [ ] Serial shows `MQTT: connected`
- [ ] Serial shows `MQTT: ntp ok`
- [ ] (Fallback if the stub CLI cannot persist settings) configure via build-flag defaults:
      uncomment `MQTT_SERVER` and `MQTT_PORT` in the env and set
      `-D MQTT_DEFAULT_IATA='"SEA"'`

No claim is made about any of the above having been observed to work at runtime.

## Self-review findings

## task-9-report.md
## PENDING hardware verification

No device was available for this task. The following checklist from the brief's Step 3 is **PENDING** and must be run on real hardware before this can be considered fully verified:

- With `mosquitto_sub -v -t 'meshcore/#'` running, send an advert from a phone within range. Expected: one `.../packets` message with `"direction":"rx"`, a `raw` hex string, non-default `SNR` and `RSSI`, and one `.../raw` message.
- Send the room's own advert with `advert` on the serial CLI. Expected: a `"direction":"tx"` packet.
- Unplug the Ethernet cable for 30 seconds while adverts arrive, then reconnect. Expected: serial logs `link down` then `link up`, a `link` message with `"link_changes":2`, and the buffered packets publish in order after reconnect.
- `get mqtt.status` (once Task 12 lands) shows non-zero drops only if more than 16 packets arrived while down.

No runtime behavior beyond a clean build has been observed or claimed.

## Self-review findings

## task-10-report.md
## PENDING hardware verification

The following (brief Step 8, hardware portion) could not be performed — no
device available in this session:

- On hardware with `mosquitto_sub -v -t 'meshcore/#'`: log in from a phone
  with the room password, post "test one". Expected in order: `room/events`
  with `"event":"login_ok"`, `room/posts` with `"text":"test one"` and
  `"role":"read_write"`.
- Log in with a wrong password: `room/events` with `"login_fail"` and
  `"result":"bad_password"`.
- Send an admin CLI command from the app: `room/dm` with
  `"kind":"cli_data"` and a `room/events` `admin_cmd`, and no field
  containing the reply.
- Within one status interval: a retained `room/stats` with `posts_total` 1.

## Concerns

## task-11-report.md
## PENDING hardware verification

No RAK4631 + RAK13800 device was available. Still needed before shipping:

- [ ] Flash `RAK_4631_room_server_observer_mqtt_ethernet`, confirm boot
      and Ethernet link as in prior tasks.
- [ ] Enable neighbors (`set mqtt.neighbors on` from Task 12, or a
      temporary forced-true edit) and confirm a `meshcore/{iata}/{device}/neighbors`
      message appears after one interval, listing zero-hop repeater
      adverts heard, with correct `snr` and `heard_secs_ago`.
- [ ] Confirm the neighbors message's node count and JSON shape are sane
      when 0, 1, and >1 repeaters are heard; confirm it is *not* retained
      (published with `retain=false`).
- [ ] Set an audience (`set mqtt.auth.aud mqtt-us-v1.letsmesh.net` from
      Task 12) and confirm a retained `auth/mqtt-us-v1.letsmesh.net`
      message appears containing a 3-part token; paste the token into
      jwt.io and confirm `aud` and `exp` match expectations, and that the
      signature verifies against the device's Ed25519 public key.
- [ ] Confirm the auth token is only (re-)published after NTP sync
      (`_time_synced`), and that renewal follows `tokenRenewalDue` timing
      (approximately 2/3 of `auth_life_secs` after issuance) rather than
      re-publishing every pass.
- [ ] Confirm multiple audiences (up to 4, comma-separated in
      `auth_audiences`) each get their own retained topic and independent
      renewal timers.
- [ ] Confirm `_publish_ok`/`_publish_err` counters (visible via
      `mqtt status`/stats) move as expected during the above.

## Self-review findings

## task-12-report.md
## PENDING hardware verification (brief Step 2 — no device available)

```
set mqtt.iata SEA
set mqtt1.preset custom
set mqtt1.server 192.168.1.10
set mqtt1.port 1883
get mqtt.config.valid
set mqtt1.preset analyzer-us
get mqtt.presets
set wifi.ssid foo
set mqtt.auth.aud mqtt-us-v1.letsmesh.net
get mqtt.status
```

Expected replies in order: `OK`, `OK`, `OK`, `OK`, `> valid`, `ERR: preset needs TLS; route
it through the LAN proxy`, a list containing `tennmesh nashmesh ctmesh eastidahomesh` and
no `analyzer-us`, `ERR: not supported on this build`, `OK`, and a status line starting
`> MQTT: ok link=up broker=192.168.1.10:1883`. Reboot the device and run `get mqtt.auth.aud`;
expected `> mqtt-us-v1.letsmesh.net`, proving `/mqtt_eth_prefs` persisted.

Note: `get mqtt.status`'s exact wording comes from `MQTTBridge::formatMqttStatusReply`,
which actually emits `> msgs: <on|off>, 1: <preset> (<state>), ..., q:<n>` rather than the
`> MQTT: ok link=up broker=...` string in the brief's checklist — this is pre-existing
shared bridge code, not something this task's CLI file controls, so the checklist is
reproduced verbatim above without editing it, and the actual on-device string should be
compared against `formatMqttStatusReply`'s real format, not the checklist's literal text.

## Self-review findings

## task-13-report.md
## PENDING hardware verification

No physical RAK4631 + RAK13800 device was available in this environment.
The following from the brief's Step 4 hardware soak is **not done** and
must be run on real hardware before merge:

- [ ] Leave the gateway running against the local broker for one hour with
      a phone posting occasionally.
- [ ] Confirm no reboots — `status`'s `uptime` keeps increasing across the
      hour.
- [ ] `get mqtt.stats` shows `pub_err` well under 1% of `pub_ok`.
- [ ] `get mqtt.status` shows `ntp=1`.

All build and test verification (native suite, four-env build) was
completed as specified; only this device-dependent soak is outstanding.

## Self-review findings

## final-fix-report.md

