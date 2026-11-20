package mpris

import (
	"errors"
	"sync"
	"time"
)

// EventKind classifies a PlayerStateEvent delivered on a subscription.
type EventKind string

const (
	// EventInitial is emitted exactly once as the first event of every subscription.
	// The snapshot is fetched synchronously from the player, so a subscriber always
	// starts from a consistent state.
	EventInitial EventKind = "initial"
	// EventPropertiesChanged is emitted when the player broadcasts an
	// org.freedesktop.DBus.Properties.PropertiesChanged signal. PlaybackStatus and
	// Metadata are merged into the snapshot; a Metadata change (track switch) resets
	// the Position to 0 unless the same signal carries a new Position.
	EventPropertiesChanged EventKind = "properties-changed"
	// EventSeeked is emitted when the player broadcasts an
	// org.mpris.MediaPlayer2.Player.Seeked signal. The snapshot position equals the
	// signal argument.
	EventSeeked EventKind = "seeked"
	// EventDisconnected is emitted when the player name vanishes from the bus.
	// With DisconnectReconnect, further events resume once the player returns.
	EventDisconnected EventKind = "disconnected"
	// EventReconnected is emitted after the player name reappeared on the bus and a
	// fresh full snapshot was successfully fetched.
	EventReconnected EventKind = "reconnected"
)

// DisconnectPolicy defines what happens to a subscription when the watched player
// name disappears from the bus (an org.freedesktop.DBus.NameOwnerChanged signal with
// an empty new owner).
type DisconnectPolicy string

const (
	// DisconnectReconnect keeps the subscription alive while the player is gone.
	// An EventDisconnected event is delivered, the player is polled until it
	// reappears, a full snapshot is re-fetched and an EventReconnected event is
	// delivered. This is the default policy.
	DisconnectReconnect DisconnectPolicy = "reconnect"
	// DisconnectTerminate terminates every subscription of the affected player with
	// ErrPlayerDisconnected when the player disappears from the bus.
	DisconnectTerminate DisconnectPolicy = "terminate"
)

// SlowConsumerPolicy defines how a subscription whose event buffer is full is handled.
type SlowConsumerPolicy string

const (
	// SlowConsumerDropOldest drops the oldest buffered event to make room for the new
	// one. A slow subscriber therefore always sees the latest state, possibly with
	// intermediate events skipped. This is the default policy.
	SlowConsumerDropOldest SlowConsumerPolicy = "drop-oldest"
	// SlowConsumerTerminate terminates the subscription with ErrSlowConsumer instead
	// of letting one slow subscriber block the shared signal processing for all
	// other subscribers.
	SlowConsumerTerminate SlowConsumerPolicy = "terminate"
)

// Default values for SubscribeOption.
const (
	defaultEventBufferSize   = 64
	defaultReconnectInterval = time.Second
)

// Sentinel errors delivered through a subscription. The event channel is always
// closed after the terminal event carrying one of these errors.
var (
	// ErrPlayerDisconnected is delivered when DisconnectTerminate is configured and
	// the player name disappears from the bus.
	ErrPlayerDisconnected = errors.New("mpris: player disconnected from bus")
	// ErrSlowConsumer is delivered when SlowConsumerTerminate is configured and the
	// subscriber event buffer was full.
	ErrSlowConsumer = errors.New("mpris: slow consumer, subscription terminated")
	// ErrSubscriptionClosed is delivered when Player.Close shuts down the
	// subscription hub.
	ErrSubscriptionClosed = errors.New("mpris: subscription closed")
)

// PlayerState is a consistent snapshot of the watched player. All fields reflect the
// state at the moment the event that carries the snapshot was processed.
type PlayerState struct {
	// PlaybackStatus is the current playback status. It is zero-valued
	// (PlaybackStatus("")) when the player did not provide the property.
	PlaybackStatus PlaybackStatus
	// Metadata is the current track metadata. A track switch replaces the whole map.
	Metadata Metadata
	// Position is the current track position in microseconds. It is reset to 0 on a
	// track switch and updated by Seeked signals and Position property changes.
	Position int64
}

// PlayerStateEvent is a single notification on a subscription. Events are delivered
// in the exact order in which the underlying D-Bus signals were received; the State
// snapshot is built by applying that signal to the previous snapshot, so consecutive
// events always form a consistent, ordered sequence.
type PlayerStateEvent struct {
	// Kind classifies the event.
	Kind EventKind
	// At is the time at which the event was produced by the hub.
	At time.Time
	// State is the snapshot resulting from this event. It holds the zero value on a
	// terminal event where Err != nil.
	State PlayerState
	// Err is set on exactly one terminal event. After a terminal event the event
	// channel is closed.
	Err error
}

// SubscribeOption configures the behavior of Player.Subscribe.
type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	bufferSize         int
	disconnectPolicy   DisconnectPolicy
	slowConsumerPolicy SlowConsumerPolicy
	reconnectInterval  time.Duration
}

func defaultSubscribeConfig() subscribeConfig {
	return subscribeConfig{
		bufferSize:         defaultEventBufferSize,
		disconnectPolicy:   DisconnectReconnect,
		slowConsumerPolicy: SlowConsumerDropOldest,
		reconnectInterval:  defaultReconnectInterval,
	}
}

// WithEventBufferSize sets the per-subscriber event buffer capacity. Values below 1
// are clamped to 1.
func WithEventBufferSize(size int) SubscribeOption {
	return func(c *subscribeConfig) {
		if size < 1 {
			size = 1
		}
		c.bufferSize = size
	}
}

// WithDisconnectPolicy selects the behavior when the watched player disappears from
// the bus. Defaults to DisconnectReconnect.
func WithDisconnectPolicy(policy DisconnectPolicy) SubscribeOption {
	return func(c *subscribeConfig) {
		c.disconnectPolicy = policy
	}
}

// WithSlowConsumerPolicy selects the behavior when a subscriber cannot keep up with
// the event rate. Defaults to SlowConsumerDropOldest.
func WithSlowConsumerPolicy(policy SlowConsumerPolicy) SubscribeOption {
	return func(c *subscribeConfig) {
		c.slowConsumerPolicy = policy
	}
}

// WithReconnectInterval sets the polling interval at which a disappeared player is
// probed for reappearance while DisconnectReconnect is active. Values below one
// millisecond are clamped to one millisecond.
func WithReconnectInterval(interval time.Duration) SubscribeOption {
	return func(c *subscribeConfig) {
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
		c.reconnectInterval = interval
	}
}

// Subscription represents a cancellable stream of PlayerStateEvent.
//
// The Events channel is closed when:
//   - the context passed to Player.Subscribe is canceled,
//   - Unsubscribe is called,
//   - Player.Close is called,
//   - or a terminal event with a non-nil Err was delivered (ErrPlayerDisconnected or
//     ErrSlowConsumer, depending on the configured policies).
//
// Multiple subscriptions on the same Player are independent: draining one channel
// never consumes or blocks events of another subscription.
type Subscription struct {
	// Events receives all state events in D-Bus signal order. It is never written to
	// after it has been closed.
	Events <-chan PlayerStateEvent

	id      subscriptionID
	hub     *stateHub
	closeMu sync.Mutex
	closed  bool
}

// Unsubscribe detaches this subscription from the player hub and closes the Events
// channel. It is safe to call concurrently with event processing and safe to call
// multiple times; subsequent calls are no-ops. Canceling the context passed to
// Subscribe has the same effect.
func (s *Subscription) Unsubscribe() {
	if s == nil || s.hub == nil {
		return
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return
	}
	s.closed = true
	s.closeMu.Unlock()

	s.hub.unsubscribe(s.id)
}
