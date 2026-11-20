# go-mpris

[![Go](https://github.com/leberKleber/go-mpris/workflows/go/badge.svg)](https://github.com/leberKleber/go-mpris/actions?query=workflow%3Ago)
[![GoDoc](https://godoc.org/github.com/leberKleber/go-mpris?status.png)](https://godoc.org/github.com/leberKleber/go-mpris)
[![Go Report Card](https://goreportcard.com/badge/github.com/leberKleber/go-mpris)](https://goreportcard.com/report/github.com/leberKleber/go-mpris)
[![codecov](https://codecov.io/gh/leberKleber/go-mpris/branch/main/graph/badge.svg)](https://codecov.io/gh/leberKleber/go-mpris)

go-mpris is an implementation of the mpris dbus interface written in go (golang).
Implemented and tested against version 2.2. See: https://specifications.freedesktop.org/mpris-spec/2.2.

* [Example](#example)
* [Features](#features)
  * [Player](#player)
    * [Methods](#methods)
    * [Properties](#properties)
    * [Signals](#signals)
  * [TrackList](#tracklist)
    * [Methods](#methods-1)
    * [Properties](#properties-1)
    * [Signals](#signals-1)
* [Development](#development)
  * [Versioning](#versioning)
  * [Commits](#commits)
  * [Mocks](#mocks)
  * [Go Docs](#go-docs)

## Example

Example cli has been implemented.

```shell
git clone git@github.com:leberKleber/go-mpris.git

go build examples/cli.go

./cli-client
```

## Features

### Player

https://specifications.freedesktop.org/mpris-spec/2.2/Player_Interface.html

#### Methods

| method      | library path                                                            | implemented        |
|-------------|-------------------------------------------------------------------------|--------------------|
| Next        | `mpris.Player.Next()`                                                   | :heavy_check_mark: |
| Previous    | `mpris.Player.Previous()`                                               | :heavy_check_mark: |
| Pause       | `mpris.Player.Pause()`                                                  | :heavy_check_mark: |
| PlayPause   | `mpris.Player.PlayPause()`                                              | :heavy_check_mark: |
| Stop        | `mpris.Player.Stop()`                                                   | :heavy_check_mark: |
| Seek        | `mpris.Player.SeekTo(<offset> int64)`¹                                  | :heavy_check_mark: |
| SetPosition | `mpris.Player.SetPosition(<trackID> dbus.ObjectPath, <position> int64)` | :heavy_check_mark: |
| OpenUri     | `mpris.Player.OpenUri(<uri> string)`                                    | :heavy_check_mark: |

¹ Could not be named Seek, it's a reserved function name.

#### Properties

| property       | library path                                                            | implemented        |
|----------------|-------------------------------------------------------------------------|--------------------|
| PlaybackStatus | `mpris.Player.PlaybackStatus() (mpris.PlaybackStatus, error)`           | :heavy_check_mark: |
| LoopStatus     | `mpris.Player.LoopStatus() (mpris.LoopStatus, error)`                   | :heavy_check_mark: |
| LoopStatus     | `mpris.Player.SetLoopStatus(<loopStatus> mpris.LoopStatus) error`       | :heavy_check_mark: |
| Rate           | `mpris.Player.Rate() (float64, error)`                                  | :heavy_check_mark: |
| Rate           | `mpris.Player.SetRate(<rate> float64) error`                            | :heavy_check_mark: |
| Shuffle        | `mpris.Player.Shuffle() (bool, error)`                                  | :heavy_check_mark: |
| Shuffle        | `mpris.Player.SetShuffle(<shuffle> bool) error`                         | :heavy_check_mark: |
| Metadata       | `mpris.Player.Metadata() (mpris.Metadata, error)`                       | :heavy_check_mark: |
| Volume         | `mpris.Player.Volume() (float64, error)`                                | :heavy_check_mark: |
| Volume         | `mpris.Player.SetVolume(<volume> float64) (error)`                      | :heavy_check_mark: |
| Position       | `mpris.Player.Position() (int64, error)`                                | :heavy_check_mark: |
| Position       | `mpris.Player.SetPosition(<trackID> dbus.ObjectPath, <position> int64)` | :heavy_check_mark: |
| MinimumRate    | `mpris.Player.MinimumRate() (float64, error)`                           | :heavy_check_mark: |
| MaximumRate    | `mpris.Player.MaximumRate() (float64, error)`                           | :heavy_check_mark: |
| CanGoNext      | `mpris.Player.CanGoNext() (bool, error)`                                | :heavy_check_mark: |
| CanGoPrevious  | `mpris.Player.CanGoPrevious() (bool, error)`                            | :heavy_check_mark: |
| CanPlay        | `mpris.Player.CanPlay() (bool, error)`                                  | :heavy_check_mark: |
| CanPause       | `mpris.Player.CanPause() (bool, error)`                                 | :heavy_check_mark: |
| CanSeek        | `mpris.Player.CanSeek() (bool, error)`                                  | :heavy_check_mark: |
| CanControl     | `mpris.Player.CanControl() (bool, error)`                               | :heavy_check_mark: |

#### Signals

| signal | library path                                                      | implemented        |
|--------|-------------------------------------------------------------------|--------------------|
| Seeked | `mpris.Player.Seeked(<ctx> context.Context) (<-chan int, error) ` | :heavy_check_mark: |

#### State subscription

Polling several MPRIS properties misses fast track changes. `Player.Subscribe` aggregates
`PlaybackStatus`, `Metadata` and `Position` property changes together with `Seeked` signals
and player name-ownership changes into one ordered, cancellable event stream. Every event
carries a consistent `PlayerState` snapshot built by applying the D-Bus signals in receive
order; a track switch resets `Position` to zero.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

player, err := mpris.NewPlayer("org.mpris.MediaPlayer2.vlc")
if err != nil { /* handle */ }
defer player.Close()

sub, err := player.Subscribe(ctx,
    mpris.WithEventBufferSize(64),
    mpris.WithDisconnectPolicy(mpris.DisconnectReconnect), // or DisconnectTerminate
    mpris.WithSlowConsumerPolicy(mpris.SlowConsumerDropOldest),
)
if err != nil { /* player not reachable */ }

for event := range sub.Events {
    if event.Err != nil {
        // terminal error: ErrPlayerDisconnected, ErrConnectionClosed,
        // ErrSlowConsumer or ErrSubscriptionClosed; the channel is closed afterwards
        log.Fatal(event.Err)
    }

    title, _ := event.State.Metadata.XESAMTitle()
    fmt.Printf("%s: %s @ %dµs\n", event.Kind, title, event.State.Position)
}
```

Guarantees:

- Multiple subscribers on one `Player` share a single D-Bus match registration and hub
  goroutine; each has an independent buffered channel, so subscribers never consume or
  block each other.
- Slow subscribers never stall the hub: the oldest event is dropped (default) or the
  subscription terminates with `ErrSlowConsumer`.
- A disappearing player emits `EventDisconnected`; with `DisconnectReconnect` the hub waits
  for the name to reappear (ownership signal plus polling fallback) and emits
  `EventReconnected` with a fresh snapshot. Loss of the D-Bus connection itself terminates
  every subscription with `ErrConnectionClosed`.
- Canceling the context, calling `Subscription.Unsubscribe` or `Player.Close` releases all
  goroutines and signal registrations without leaks.

### TrackList

https://specifications.freedesktop.org/mpris-spec/2.2/Track_List_Interface.html

#### Methods

| method            | library path        | implemented              |
|-------------------|---------------------|--------------------------|
| GetTracksMetadata | Not implemented yet | :heavy_multiplication_x: |
| AddTrack          | Not implemented yet | :heavy_multiplication_x: |
| RemoveTrack       | Not implemented yet | :heavy_multiplication_x: |
| GoTo              | Not implemented yet | :heavy_multiplication_x: |

#### Properties

| property       | library path        | implemented              |
|----------------|---------------------|--------------------------|
| Tracks         | Not implemented yet | :heavy_multiplication_x: |
| CanEditTracks  | Not implemented yet | :heavy_multiplication_x: |


#### Signals

| signal               | library path        | implemented              |
|----------------------|---------------------|--------------------------|
| TrackListReplaced    | Not implemented yet | :heavy_multiplication_x: |
| TrackAdded           | Not implemented yet | :heavy_multiplication_x: |
| TrackRemoved         | Not implemented yet | :heavy_multiplication_x: |
| TrackMetadataChanged | Not implemented yet | :heavy_multiplication_x: |

## Development

### Versioning

This library follows the semantic versioning concept.

### Commits

Commits should follow the conventional commit rules.  
See: https://conventionalcommits.org.

### Mocks

Mocks will be generated with `github.com/matryer/moq`. It can be installed with
`go install github.com/matryer/moq@latest`. Generation can be triggered with `go generate ./...`.

### Go Docs

Read the docs at https://pkg.go.dev/github.com/leberKleber/go-mpris