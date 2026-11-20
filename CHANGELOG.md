# Changelog

## Unreleased

- add `Player.Subscribe`: cancellable, ordered state-event stream aggregating
  PlaybackStatus, Metadata, Position property changes and Seeked signals into
  consistent `PlayerState` snapshots
- track switches (Metadata changes) reset the track Position; explicit Position
  changes in the same signal take precedence
- multiple independent subscribers share one D-Bus signal registration and hub
  goroutine, each with its own buffered channel; slow subscribers either drop the
  oldest event or terminate with `ErrSlowConsumer`
- player disappearance is handled with an explicit policy: `DisconnectReconnect`
  (default, emits disconnected/reconnected events) or `DisconnectTerminate`
  (`ErrPlayerDisconnected`); D-Bus connection loss terminates with
  `ErrConnectionClosed`
- tolerate missing, invalidated and wrongly typed properties, malformed signals and
  duplicate signals without panicking or leaking goroutines

## v0.2.2

- add go mod retract directive to fix accidental released v1.1.0 fail

## v0.2.1

- make goreportcard 100% happy

## v0.2.0

- fix dbusConnWrapper close handling
- update go deps

## v0.1.0

- add mpris MediaPlayer2.Player signals  (all)
- add ability to close dbus connection

## v0.0.1

- add mpris MediaPlayer2.Player methods (all)
- add mpris MediaPlayer2.Player properties (all)
