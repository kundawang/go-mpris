package mpris

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	signalNamePropertiesChanged = "org.freedesktop.DBus.Properties.PropertiesChanged"
	signalNameNameOwnerChanged  = "org.freedesktop.DBus.NameOwnerChanged"
	dbusInterface               = "org.freedesktop.DBus"
)

// ErrConnectionClosed is delivered on every active subscription when the underlying
// D-Bus connection is lost (the signal channel of the connection was closed). Unlike
// a disappearing player name, a closed connection cannot be recovered: callers must
// create a new Player with NewPlayer and subscribe again.
var ErrConnectionClosed = errors.New("mpris: dbus connection closed, subscription terminated")

// errHubStopped is used internally when a subscribe request races with hub shutdown.
var errHubStopped = errors.New("mpris: state hub stopped")

type subscriptionID uint64

// stateHub is a single-goroutine actor that owns the D-Bus signal channel of one
// Player. All signal processing, state mutation and fan-out happens on runLoop, so
// snapshots are always consistent with D-Bus signal ordering and no locking is
// required on the hot path.
type stateHub struct {
	player *Player
	conn   dbusConn
	name   string

	signals chan *dbus.Signal
	reqs    chan hubRequest
	done    chan struct{}

	// nextID starts at 1: ID 0 is reserved as the "no unsubscribe request" sentinel
	// in hubRequest, so a subscriber must never receive it.
	nextID subscriptionID
	subs   map[subscriptionID]*subscriber
	state  PlayerState

	// removed guards RemoveSignal so it runs exactly once even though several exit
	// paths (shutdown, disconnect, reconnect-loop failure) converge on terminateAll.
	removed bool

	// disconnectedOwner is the bus owner that disappeared while the hub is waiting
	// for a reconnect. Polling-based fallback reconnects only once the current owner
	// differs, so stale readable properties of the vanished player can never be
	// mistaken for a successful reconnect. Empty while connected.
	disconnectedOwner string

	// disconnectPolicy and reconnectInterval are hub-wide settings fixed by the first
	// subscriber: all subscribers share one signal stream and one reconnect state
	// machine, so per-subscriber divergence here would be undecidable. Later
	// Subscribe calls accept these options but the running hub keeps its originals
	// until Player.Close.
	disconnectPolicy  DisconnectPolicy
	reconnectInterval time.Duration
}

type subscriber struct {
	id  subscriptionID
	ch  chan PlayerStateEvent
	ctx context.Context
	cfg subscribeConfig

	// stopped is closed when the subscriber is removed from the hub, releasing the
	// context watcher.
	stopped chan struct{}
}

type hubRequest struct {
	subscribe   *subscriber
	reply       chan error
	unsubscribe subscriptionID
	shutdown    bool
}

// runLoopOutcome tells the actor loop what to do after a nested activity returns.
type runLoopOutcome int

const (
	loopContinue runLoopOutcome = iota
	loopStop
)

func newStateHub(p *Player) *stateHub {
	return &stateHub{
		player:  p,
		conn:    p.connection,
		name:    p.name,
		signals: make(chan *dbus.Signal, 1024),
		// reqs is buffered: lifecycle requests are enqueued without ever blocking the
		// caller on the actor reaching its select. The actor drains reqs on every loop
		// iteration; ordering between requests and signals is preserved because both
		// are consumed serially by the single actor goroutine.
		reqs: make(chan hubRequest, 32),
		done: make(chan struct{}),
		subs: make(map[subscriptionID]*subscriber),
		// ID 0 is reserved as the "no unsubscribe request" sentinel in hubRequest.
		nextID: 1,

		disconnectPolicy:  DisconnectReconnect,
		reconnectInterval: defaultReconnectInterval,
	}
}

// start registers the D-Bus match rules, fetches the initial snapshot and launches
// the actor goroutine. It fails before any goroutine is spawned, so a failed start
// leaks nothing.
func (h *stateHub) start() error {
	matchRules := [][]dbus.MatchOption{
		{dbus.WithMatchSender(h.name), dbus.WithMatchInterface(dbusInterface + ".Properties"), dbus.WithMatchMember("PropertiesChanged")},
		{dbus.WithMatchSender(h.name), dbus.WithMatchInterface(playerInterface), dbus.WithMatchMember("Seeked")},
		{dbus.WithMatchInterface(dbusInterface), dbus.WithMatchMember("NameOwnerChanged"), dbus.WithMatchArg(0, h.name)},
	}
	for _, rule := range matchRules {
		if err := h.conn.AddMatchSignal(rule...); err != nil {
			return err
		}
	}

	state, err := h.fetchState()
	if err != nil {
		return err
	}
	h.state = state

	h.conn.Signal(h.signals)
	go h.runLoop()
	return nil
}

func (h *stateHub) shutdown() {
	h.sendControl(hubRequest{shutdown: true})
	<-h.done
}

func (h *stateHub) unsubscribe(id subscriptionID) {
	h.sendControl(hubRequest{unsubscribe: id})
}

// sendControl enqueues a lifecycle request. It never blocks: the request buffer is
// sized for all possible concurrent controls, and an exited actor is detected via
// done. reqs is never closed, so a send-on-closed-channel panic is impossible.
func (h *stateHub) sendControl(req hubRequest) {
	select {
	case h.reqs <- req:
	case <-h.done:
	}
}

func (h *stateHub) subscribe(parentCtx context.Context, cfg subscribeConfig) (*Subscription, error) {
	id := h.nextID
	h.nextID++
	sub := &subscriber{
		id:      id,
		ch:      make(chan PlayerStateEvent, cfg.bufferSize),
		ctx:     parentCtx,
		cfg:     cfg,
		stopped: make(chan struct{}),
	}

	reply := make(chan error, 1)
	select {
	case h.reqs <- hubRequest{subscribe: sub, reply: reply}:
	case <-h.done:
		return nil, errHubStopped
	case <-parentCtx.Done():
		return nil, parentCtx.Err()
	}

	select {
	case err := <-reply:
		if err != nil {
			return nil, err
		}
	case <-h.done:
		return nil, errHubStopped
	case <-parentCtx.Done():
		// The hub may already have registered the subscriber; make sure it is removed
		// again before returning.
		h.unsubscribe(id)
		return nil, parentCtx.Err()
	}

	go h.watchContext(sub)

	return &Subscription{Events: sub.ch, id: id, hub: h}, nil
}

// watchContext translates context cancellation into an unsubscribe request. It exits
// when the context is done, when the subscriber is removed for any other reason, or
// when the hub terminates — so it can never leak.
func (h *stateHub) watchContext(sub *subscriber) {
	select {
	case <-sub.ctx.Done():
		h.unsubscribe(sub.id)
	case <-sub.stopped:
	case <-h.done:
	}
}

func (h *stateHub) runLoop() {
	defer close(h.done)

	for {
		select {
		case req := <-h.reqs:
			// Drain any other queued lifecycle requests so unsubscribes and
			// shutdowns are handled in one batch without being starved by signals.
			if h.handleRequestBatch(req) {
				h.terminateAll(ErrSubscriptionClosed)
				return
			}
		case sig, ok := <-h.signals:
			if !ok {
				// The D-Bus connection itself died. This is not recoverable for this
				// Player instance.
				h.terminateAll(ErrConnectionClosed)
				return
			}
			if h.handleSignal(sig) == loopStop {
				return
			}
		}
	}
}

// handleRequestBatch processes one request and any immediately following queued
// requests. It returns true when the actor must stop.
func (h *stateHub) handleRequestBatch(first hubRequest) bool {
	if h.handleRequest(first) {
		return true
	}
	for {
		select {
		case req := <-h.reqs:
			if h.handleRequest(req) {
				return true
			}
		default:
			return false
		}
	}
}

// handleRequest returns true when the actor must stop.
func (h *stateHub) handleRequest(req hubRequest) bool {
	switch {
	case req.shutdown:
		return true
	case req.subscribe != nil:
		if len(h.subs) == 0 {
			// The first subscriber fixes the hub-wide lifecycle settings.
			h.disconnectPolicy = req.subscribe.cfg.disconnectPolicy
			h.reconnectInterval = req.subscribe.cfg.reconnectInterval
		}
		h.subs[req.subscribe.id] = req.subscribe
		// A new subscriber always receives the current snapshot as its first event.
		h.deliver(req.subscribe, PlayerStateEvent{Kind: EventInitial, At: time.Now(), State: h.state})
		if req.reply != nil {
			req.reply <- nil
		}
	case req.unsubscribe != 0:
		h.removeSubscriber(req.unsubscribe)
	}
	return false
}

func (h *stateHub) removeSubscriber(id subscriptionID) {
	sub, ok := h.subs[id]
	if !ok {
		// Already removed by a context cancellation and Unsubscribe racing each
		// other, or swept by terminateAll. Nothing to do.
		return
	}
	h.closeSubscriber(sub)
}

// closeSubscriber removes a subscriber, releases its context watcher and closes its
// event channel. Only the actor goroutine calls this, so a double close is
// impossible.
func (h *stateHub) closeSubscriber(sub *subscriber) {
	delete(h.subs, sub.id)
	close(sub.stopped)
	close(sub.ch)
}

// terminateAll sends a terminal event to every subscriber, closes every channel and
// detaches the D-Bus signal registration. Only the actor goroutine ever closes
// subscriber channels, so a send-on-closed-channel panic is impossible.
func (h *stateHub) terminateAll(err error) {
	terminal := PlayerStateEvent{At: time.Now(), Err: err}
	for id, sub := range h.subs {
		h.deliver(sub, terminal)
		delete(h.subs, id)
		close(sub.stopped)
		close(sub.ch)
	}
	h.detachSignal()
}

// detachSignal deregisters the hub channel from the D-Bus connection. The channel
// itself is owned by the connection, which closes it on Conn.Close — so the hub must
// never close it itself (a double close would panic).
func (h *stateHub) detachSignal() {
	if h.removed {
		return
	}
	h.removed = true
	h.conn.RemoveSignal(h.signals)
}

// fanOut dispatches an event to every active subscriber. A slow subscriber can never
// block signal processing or other subscribers: its buffer overflow is handled
// according to its own SlowConsumerPolicy.
func (h *stateHub) fanOut(ev PlayerStateEvent) {
	for id, sub := range h.subs {
		select {
		case sub.ch <- ev:
			continue
		default:
		}

		switch sub.cfg.slowConsumerPolicy {
		case SlowConsumerTerminate:
			terminal := PlayerStateEvent{At: time.Now(), Err: ErrSlowConsumer}
			// Make room for the guaranteed terminal event.
			select {
			case <-sub.ch:
			default:
			}
			sub.ch <- terminal
			delete(h.subs, id)
			close(sub.stopped)
			close(sub.ch)
		default: // SlowConsumerDropOldest
			select {
			case <-sub.ch:
			default:
			}
			sub.ch <- ev
		}
	}
}

func (h *stateHub) deliver(sub *subscriber, ev PlayerStateEvent) {
	select {
	case sub.ch <- ev:
	default:
		// Initial and terminal events must not get lost on a full buffer either.
		select {
		case <-sub.ch:
		default:
		}
		sub.ch <- ev
	}
}

// handleSignal applies one D-Bus signal to the snapshot. Signals are processed
// serially on the actor goroutine, preserving D-Bus ordering. The returned outcome
// tells runLoop whether to keep running.
func (h *stateHub) handleSignal(sig *dbus.Signal) runLoopOutcome {
	switch sig.Name {
	case signalNamePropertiesChanged:
		h.handlePropertiesChanged(sig)
	case signalNameSeeked:
		h.handleSeeked(sig)
	case signalNameNameOwnerChanged:
		return h.handleNameOwnerChanged(sig)
	}
	return loopContinue
}

func (h *stateHub) handlePropertiesChanged(sig *dbus.Signal) {
	if len(sig.Body) < 3 {
		return // broken signal: ignore instead of panicking
	}

	iface, _ := sig.Body[0].(string)
	if iface != playerInterface {
		return
	}

	changed, _ := sig.Body[1].(map[string]dbus.Variant)
	invalidated, _ := sig.Body[2].([]string)
	if changed == nil && len(invalidated) == 0 {
		return
	}

	previous := h.state
	next := previous

	if variant, ok := changed[playerPlaybackStatusProperty]; ok {
		if status, ok := variant.Value().(string); ok {
			next.PlaybackStatus = PlaybackStatus(status)
		}
		// A present but wrongly typed property is ignored, not fatal.
	}

	if variant, ok := changed[playerMetadataProperty]; ok {
		if metadata, ok := variant.Value().(map[string]dbus.Variant); ok {
			next.Metadata = cloneMetadata(metadata)
			// A track switch resets the progress; a Position carried by the very
			// same signal is applied afterwards and wins.
			next.Position = 0
		}
	}

	if variant, ok := changed[playerPositionProperty]; ok {
		if position, ok := variant.Value().(int64); ok {
			next.Position = position
		}
	}

	for _, property := range invalidated {
		switch property {
		case "Metadata":
			next.Metadata = nil
			next.Position = 0
		case "PlaybackStatus":
			next.PlaybackStatus = ""
		}
	}

	h.state = next
	h.emitIfChanged(next, previous, EventPropertiesChanged)
}

func (h *stateHub) handleSeeked(sig *dbus.Signal) {
	if len(sig.Body) != 1 {
		return
	}
	position, ok := sig.Body[0].(int64)
	if !ok {
		return
	}

	previous := h.state
	next := previous
	next.Position = position
	h.state = next
	h.emitIfChanged(next, previous, EventSeeked)
}

func (h *stateHub) handleNameOwnerChanged(sig *dbus.Signal) runLoopOutcome {
	if len(sig.Body) != 3 {
		return loopContinue
	}
	name, _ := sig.Body[0].(string)
	if name != h.name {
		return loopContinue
	}
	oldOwner, _ := sig.Body[1].(string)
	newOwner, _ := sig.Body[2].(string)
	if newOwner != "" {
		// Player (re)appeared. Outside a reconnect wait this is a no-op; the wait
		// loop evaluates it.
		return loopContinue
	}

	// The player name disappeared from the bus.
	if h.effectiveDisconnectPolicy() == DisconnectTerminate {
		h.terminateAll(ErrPlayerDisconnected)
		return loopStop
	}

	h.disconnectedOwner = oldOwner
	h.fanOut(PlayerStateEvent{Kind: EventDisconnected, At: time.Now(), State: h.state})
	return h.waitForReconnect()
}

// waitForReconnect blocks the actor while the player is absent, still serving
// unsubscribe/shutdown requests and watching the signal channel for connection loss.
//
// Reappearance is detected primarily from a NameOwnerChanged signal carrying a new
// owner (this cannot be spoofed by stale readable properties); periodic polling acts
// as a fallback for buses that do not deliver the ownership signal.
func (h *stateHub) waitForReconnect() runLoopOutcome {
	ticker := time.NewTicker(h.effectiveReconnectInterval())
	defer ticker.Stop()

	for {
		select {
		case req := <-h.reqs:
			if h.handleRequest(req) {
				h.terminateAll(ErrSubscriptionClosed)
				return loopStop
			}
		case sig, ok := <-h.signals:
			if !ok {
				h.terminateAll(ErrConnectionClosed)
				return loopStop
			}
			if h.isReconnectSignal(sig) {
				if state, err := h.fetchStateWithRetry(); err == nil {
					h.disconnectedOwner = ""
					h.state = state
					h.fanOut(PlayerStateEvent{Kind: EventReconnected, At: time.Now(), State: state})
					return loopContinue
				}
				// Owner announced but properties are not readable yet; keep waiting
				// for the next tick/signal.
			}
			// Other signals while disconnected are ignored.
		case <-ticker.C:
			if !h.ownerActuallyChanged() {
				continue // properties still belong to the vanished player
			}
			state, err := h.fetchState()
			if err != nil {
				continue // player still gone or properties unreadable
			}
			h.disconnectedOwner = ""
			h.state = state
			h.fanOut(PlayerStateEvent{Kind: EventReconnected, At: time.Now(), State: state})
			return loopContinue
		}
	}
}

// ownerActuallyChanged is the polling fallback guard: it asks the bus for the current
// owner of the player name and only reports true once it differs from the owner that
// disappeared. The mock and real bus both answer org.freedesktop.DBus.GetNameOwner.
func (h *stateHub) ownerActuallyChanged() bool {
	// GetNameOwner is a method call on the bus object; route it through the same
	// object abstraction used for properties. The connection mock implements it via
	// Call.Store.
	var currentOwner string
	call := h.conn.Object(dbusInterface, dbus.ObjectPath("/org/freedesktop/DBus")).
		Call("org.freedesktop.DBus.GetNameOwner", 0, h.name)
	if call == nil {
		return false
	}
	if err := call.Store(&currentOwner); err != nil {
		return false // name unowned: player is still gone
	}
	return currentOwner != "" && currentOwner != h.disconnectedOwner
}

// fetchStateWithRetry waits briefly for a re-announced player to expose its
// properties again. D-Bus property updates can lag the NameOwnerChanged signal by a
// few milliseconds.
func (h *stateHub) fetchStateWithRetry() (PlayerState, error) {
	interval := h.effectiveReconnectInterval()
	if interval > 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	deadline := time.Now().Add(5 * interval)
	for {
		state, err := h.fetchState()
		if err == nil {
			return state, nil
		}
		if time.Now().After(deadline) {
			return PlayerState{}, err
		}
		select {
		case <-time.After(interval):
		case <-h.done:
			return PlayerState{}, errHubStopped
		}
	}
}

// isReconnectSignal reports whether sig is the NameOwnerChanged announcing a new
// owner for the watched player name.
func (h *stateHub) isReconnectSignal(sig *dbus.Signal) bool {
	if sig == nil || sig.Name != signalNameNameOwnerChanged || len(sig.Body) != 3 {
		return false
	}
	name, _ := sig.Body[0].(string)
	if name != h.name {
		return false
	}
	newOwner, _ := sig.Body[2].(string)
	return newOwner != ""
}

func (h *stateHub) effectiveDisconnectPolicy() DisconnectPolicy {
	return h.disconnectPolicy
}

func (h *stateHub) effectiveReconnectInterval() time.Duration {
	return h.reconnectInterval
}

func (h *stateHub) emitIfChanged(next, previous PlayerState, kind EventKind) {
	// Duplicate signals (identical resulting snapshot) are suppressed so subscribers
	// are only notified when the state actually changed.
	if reflect.DeepEqual(next, previous) {
		return
	}
	h.fanOut(PlayerStateEvent{Kind: kind, At: time.Now(), State: next})
}

// fetchState reads the aggregated snapshot directly from the player. Individual
// property failures degrade to zero values; only when every property is unreadable is
// the player considered unreachable and an error returned.
func (h *stateHub) fetchState() (PlayerState, error) {
	var state PlayerState
	var failures int

	if variant, err := h.conn.Object(h.name, playerObjectPath).GetProperty(playerPlaybackStatusProperty); err != nil {
		failures++
	} else if status, ok := variant.Value().(string); ok {
		state.PlaybackStatus = PlaybackStatus(status)
	} else {
		failures++
	}

	if variant, err := h.conn.Object(h.name, playerObjectPath).GetProperty(playerMetadataProperty); err != nil {
		failures++
	} else if metadata, ok := variant.Value().(map[string]dbus.Variant); ok {
		state.Metadata = cloneMetadata(metadata)
	} else {
		failures++
	}

	if variant, err := h.conn.Object(h.name, playerObjectPath).GetProperty(playerPositionProperty); err != nil {
		failures++
	} else if position, ok := variant.Value().(int64); ok {
		state.Position = position
	} else {
		failures++
	}

	if failures == 3 {
		return PlayerState{}, errors.New("mpris: player is unreachable, none of PlaybackStatus, Metadata, Position could be read")
	}
	return state, nil
}

func cloneMetadata(in Metadata) Metadata {
	if in == nil {
		return nil
	}
	out := make(Metadata, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
