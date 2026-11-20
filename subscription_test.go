package mpris

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDBusConn is a controllable D-Bus mock for the subscription tests. It stores
// property values, fans emitted signals out like dbus.Conn does (including closing
// every registered channel on disconnect) and simulates player disappearance.
type fakeDBusConn struct {
	mu sync.Mutex

	properties map[string]dbus.Variant
	// getPropertyErr, when non-nil and the property is not explicitly stored, makes
	// GetProperty fail — used to simulate a vanished player.
	getPropertyErr error
	signalChans    map[chan<- *dbus.Signal]struct{}

	// owner maps the watched player name to its current bus owner. An empty owner
	// means the name is unowned (player gone).
	owner map[string]string

	closed        bool
	removeSignals []chan<- *dbus.Signal
	matchRules    [][]dbus.MatchOption

	// failAddMatch makes AddMatchSignal return an error, simulating a bus that
	// rejects a match rule at subscription start.
	failAddMatch bool
}

func newFakeDBusConn(state PlayerState) *fakeDBusConn {
	return &fakeDBusConn{
		properties: map[string]dbus.Variant{
			playerPlaybackStatusProperty: dbus.MakeVariant(string(state.PlaybackStatus)),
			playerMetadataProperty:       dbus.MakeVariant(map[string]dbus.Variant(state.Metadata)),
			playerPositionProperty:       dbus.MakeVariant(state.Position),
		},
		signalChans: make(map[chan<- *dbus.Signal]struct{}),
		owner:       map[string]string{},
	}
}

func (f *fakeDBusConn) Object(_ string, _ dbus.ObjectPath) dbusBusObject {
	return fakeBusObject{conn: f}
}

func (f *fakeDBusConn) AddMatchSignal(options ...dbus.MatchOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAddMatch {
		return errors.New("match rule rejected by bus")
	}
	f.matchRules = append(f.matchRules, options)
	return nil
}

func (f *fakeDBusConn) Signal(ch chan<- *dbus.Signal) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signalChans[ch] = struct{}{}
}

func (f *fakeDBusConn) RemoveSignal(ch chan<- *dbus.Signal) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.signalChans, ch)
	f.removeSignals = append(f.removeSignals, ch)
}

func (f *fakeDBusConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	for ch := range f.signalChans {
		close(ch)
	}
	f.signalChans = map[chan<- *dbus.Signal]struct{}{}
	return nil
}

// emit broadcasts a signal to every registered channel, mirroring dbus.Conn. The
// mock uses generous buffering so test injections never block on hub processing.
func (f *fakeDBusConn) emit(sig *dbus.Signal) {
	f.mu.Lock()
	chans := make([]chan<- *dbus.Signal, 0, len(f.signalChans))
	for ch := range f.signalChans {
		chans = append(chans, ch)
	}
	f.mu.Unlock()
	for _, ch := range chans {
		ch <- sig
	}
}

// setProperty updates the mock property store.
func (f *fakeDBusConn) setProperty(key string, value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.properties[key] = dbus.MakeVariant(value)
}

func (f *fakeDBusConn) getProperty(key string) (dbus.Variant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// An unreachable player fails every property read even while the old values are
	// still stored, mirroring a name without an owner on the real bus.
	if f.getPropertyErr != nil {
		return dbus.Variant{}, f.getPropertyErr
	}
	if variant, ok := f.properties[key]; ok {
		return variant, nil
	}
	return dbus.Variant{}, errors.New("no such property: " + key)
}

// emitPlayerGone fires a NameOwnerChanged with an empty new owner.
func (f *fakeDBusConn) emitPlayerGone(name string) {
	f.mu.Lock()
	oldOwner := f.owner[name]
	f.owner[name] = ""
	f.mu.Unlock()
	f.emit(&dbus.Signal{
		Name: signalNameNameOwnerChanged,
		Body: []interface{}{name, oldOwner, ""},
	})
}

// emitPlayerBack fires a NameOwnerChanged announcing a new owner.
func (f *fakeDBusConn) emitPlayerBack(name string) {
	newOwner := ":1.99"
	f.mu.Lock()
	oldOwner := f.owner[name]
	f.owner[name] = newOwner
	f.mu.Unlock()
	f.emit(&dbus.Signal{
		Name: signalNameNameOwnerChanged,
		Body: []interface{}{name, oldOwner, newOwner},
	})
}

type fakeBusObject struct {
	conn *fakeDBusConn
}

func (o fakeBusObject) Call(method string, _ dbus.Flags, args ...interface{}) dbusCall {
	if method == "org.freedesktop.DBus.GetNameOwner" {
		name, _ := args[0].(string)
		return fakeCall{conn: o.conn, name: name}
	}
	return nil
}

func (o fakeBusObject) GetProperty(p string) (dbus.Variant, error) {
	return o.conn.getProperty(p)
}
func (o fakeBusObject) SetProperty(_ string, _ interface{}) error { return nil }

// fakeCall answers org.freedesktop.DBus.GetNameOwner against the mock owner table,
// returning an error when the name is unowned.
type fakeCall struct {
	conn *fakeDBusConn
	name string
}

func (c fakeCall) Store(targets ...interface{}) error {
	c.conn.mu.Lock()
	owner, ok := c.conn.owner[c.name]
	c.conn.mu.Unlock()
	if !ok || owner == "" {
		return errors.New("org.freedesktop.DBus.Error.NameHasNoOwner")
	}
	if ptr, ok := targets[0].(*string); ok {
		*ptr = owner
	}
	return nil
}

func propertiesChangedSignal(changed map[string]dbus.Variant, invalidated []string) *dbus.Signal {
	return &dbus.Signal{
		Name: signalNamePropertiesChanged,
		Body: []interface{}{playerInterface, changed, invalidated},
	}
}

func seekedSignal(position int64) *dbus.Signal {
	return &dbus.Signal{
		Name: signalNameSeeked,
		Body: []interface{}{position},
	}
}

func metadataFor(trackID, title string) Metadata {
	return Metadata{
		"mpris:trackid": dbus.MakeVariant(dbus.ObjectPath("/tracks/" + trackID)),
		"xesam:title":   dbus.MakeVariant(title),
	}
}

// collect drains a subscription until it closes or the timeout elapses.
func collect(t *testing.T, sub *Subscription, timeout time.Duration) []PlayerStateEvent {
	t.Helper()
	var events []PlayerStateEvent
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-sub.Events:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-deadline:
			return events
		}
	}
}

func waitForEvent(t *testing.T, sub *Subscription, timeout time.Duration) PlayerStateEvent {
	t.Helper()
	select {
	case ev, ok := <-sub.Events:
		require.True(t, ok, "subscription channel closed unexpectedly")
		return ev
	case <-time.After(timeout):
		t.Fatal("timed out waiting for subscription event")
		return PlayerStateEvent{}
	}
}

func waitForClosed(t *testing.T, sub *Subscription, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-sub.Events:
		require.False(t, ok, "expected subscription channel to be closed")
	case <-time.After(timeout):
		t.Fatal("timed out waiting for subscription channel close")
	}
}

func newSubscribedPlayer(t *testing.T, conn *fakeDBusConn, name string, options ...SubscribeOption) (*Player, *Subscription, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	player := &Player{name: name, connection: conn}
	sub, err := player.Subscribe(ctx, options...)
	require.NoError(t, err)
	return player, sub, cancel
}

func TestSubscribe_InitialSnapshot(t *testing.T) {
	initial := PlayerState{
		PlaybackStatus: PlaybackStatusPlaying,
		Metadata:       metadataFor("1", "Song A"),
		Position:       1234,
	}
	conn := newFakeDBusConn(initial)

	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()

	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventInitial, ev.Kind)
	assert.Equal(t, initial, ev.state())
	assert.NoError(t, ev.Err)
	assert.False(t, ev.At.IsZero())

	// Exactly three match rules were registered: PropertiesChanged, Seeked,
	// NameOwnerChanged.
	assert.Len(t, conn.matchRules, 3)
}

func TestSubscribe_StartFailureWhenPlayerUnreachable(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{})
	conn.getPropertyErr = errors.New("player gone")
	delete(conn.properties, playerPlaybackStatusProperty)
	delete(conn.properties, playerMetadataProperty)
	delete(conn.properties, playerPositionProperty)

	player := &Player{name: "org.mpris.MediaPlayer2.gone", connection: conn}
	_, err := player.Subscribe(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "failed to start state subscription"), "unexpected error: %v", err)

	// No goroutine/hub left behind, no signal registration happened.
	assert.Empty(t, conn.signalChans)
}

func TestSubscribe_AggregatedSequenceTrackSwitchAndSeek(t *testing.T) {
	initial := PlayerState{
		PlaybackStatus: PlaybackStatusPlaying,
		Metadata:       metadataFor("1", "Song A"),
		Position:       1000,
	}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()

	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// 1. Seek while track 1 plays.
	conn.emit(seekedSignal(5000))
	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventSeeked, ev.Kind)
	assert.Equal(t, int64(5000), ev.State.Position)
	assert.Equal(t, "Song A", mustTitle(t, ev.State.Metadata))

	// 2. Track switch via PropertiesChanged: progress must reset to 0.
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerMetadataProperty: dbus.MakeVariant(map[string]dbus.Variant(metadataFor("2", "Song B"))),
	}, nil))
	ev = waitForEvent(t, sub, time.Second)
	require.Equal(t, EventPropertiesChanged, ev.Kind)
	assert.Equal(t, int64(0), ev.State.Position, "track switch must reset position")
	assert.Equal(t, "Song B", mustTitle(t, ev.State.Metadata))
	assert.Equal(t, PlaybackStatusPlaying, ev.State.PlaybackStatus, "unrelated fields must be retained")

	// 3. Playback status change on track 2.
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerPlaybackStatusProperty: dbus.MakeVariant(string(PlaybackStatusPaused)),
	}, nil))
	ev = waitForEvent(t, sub, time.Second)
	require.Equal(t, EventPropertiesChanged, ev.Kind)
	assert.Equal(t, PlaybackStatusPaused, ev.State.PlaybackStatus)
	assert.Equal(t, "Song B", mustTitle(t, ev.State.Metadata), "metadata must be retained")

	// 4. Another seek.
	conn.emit(seekedSignal(9000))
	ev = waitForEvent(t, sub, time.Second)
	require.Equal(t, EventSeeked, ev.Kind)
	assert.Equal(t, int64(9000), ev.State.Position)
	assert.Equal(t, "Song B", mustTitle(t, ev.State.Metadata))

	// 5. Track switch carrying an explicit Position in the same signal wins over the
	// reset-to-zero rule.
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerMetadataProperty: dbus.MakeVariant(map[string]dbus.Variant(metadataFor("3", "Song C"))),
		playerPositionProperty: dbus.MakeVariant(int64(4242)),
	}, nil))
	ev = waitForEvent(t, sub, time.Second)
	require.Equal(t, EventPropertiesChanged, ev.Kind)
	assert.Equal(t, "Song C", mustTitle(t, ev.State.Metadata))
	assert.Equal(t, int64(4242), ev.State.Position)
}

func TestSubscribe_FastTrackSwitchesAreNotMissed(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying, Metadata: metadataFor("0", "T0")}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test",
		WithEventBufferSize(32))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Burst: three immediate track switches followed by a seek, all before draining.
	for i := 1; i <= 3; i++ {
		conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
			playerMetadataProperty: dbus.MakeVariant(map[string]dbus.Variant(metadataFor(
				string(rune('0'+i)), "T"+string(rune('0'+i))))),
		}, nil))
	}
	conn.emit(seekedSignal(777))

	events := collect(t, sub, 500*time.Millisecond)
	require.Len(t, events, 4, "every signal must produce an event, none may be missed")
	assert.Equal(t, EventPropertiesChanged, events[0].Kind)
	assert.Equal(t, "T1", mustTitle(t, events[0].State.Metadata))
	assert.Equal(t, "T2", mustTitle(t, events[1].State.Metadata))
	assert.Equal(t, "T3", mustTitle(t, events[2].State.Metadata))
	assert.Equal(t, EventSeeked, events[3].Kind)
	assert.Equal(t, int64(777), events[3].State.Position)
}

func TestSubscribe_MissingAndInvalidPropertiesAreTolerated(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying, Metadata: metadataFor("1", "A")}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Wrongly typed status and a broken Seeked signal must be ignored, not panic.
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerPlaybackStatusProperty: dbus.MakeVariant(42),
	}, nil))
	conn.emit(&dbus.Signal{Name: signalNameSeeked, Body: []interface{}{"not-an-int"}})
	conn.emit(&dbus.Signal{Name: signalNameSeeked, Body: []interface{}{}})
	conn.emit(&dbus.Signal{Name: signalNamePropertiesChanged, Body: []interface{}{}})

	// A valid signal afterwards still works.
	conn.emit(seekedSignal(333))
	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventSeeked, ev.Kind)
	assert.Equal(t, int64(333), ev.State.Position)
	assert.Equal(t, PlaybackStatusPlaying, ev.State.PlaybackStatus, "invalid change must not corrupt state")
}

func TestSubscribe_DuplicateSignalsAreSuppressed(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying, Position: 10}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Two redundant status signals with identical value.
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerPlaybackStatusProperty: dbus.MakeVariant(string(PlaybackStatusPlaying)),
	}, nil))
	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerPlaybackStatusProperty: dbus.MakeVariant(string(PlaybackStatusPlaying)),
	}, nil))
	// Seek to the very same position already cached.
	conn.emit(seekedSignal(10))

	assert.Empty(t, collect(t, sub, 200*time.Millisecond))

	// A real change gets through.
	conn.emit(seekedSignal(11))
	ev := waitForEvent(t, sub, time.Second)
	assert.Equal(t, int64(11), ev.State.Position)
}

func TestSubscribe_MultipleSubscribersAreIndependent(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying, Metadata: metadataFor("1", "A")}
	conn := newFakeDBusConn(initial)
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	sub1, err := player.Subscribe(ctx1)
	require.NoError(t, err)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2, err := player.Subscribe(ctx2)
	require.NoError(t, err)

	// Shared hub: one signal registration, three match rules.
	assert.Len(t, conn.signalChans, 1)
	assert.Len(t, conn.matchRules, 3)

	require.Equal(t, EventInitial, waitForEvent(t, sub1, time.Second).Kind)
	require.Equal(t, EventInitial, waitForEvent(t, sub2, time.Second).Kind)

	// sub2 stops draining to simulate a slow consumer; sub1 must keep flowing.
	conn.emit(seekedSignal(100))
	ev1 := waitForEvent(t, sub1, time.Second)
	assert.Equal(t, int64(100), ev1.State.Position)

	// The same event is independently available on sub2.
	ev2 := waitForEvent(t, sub2, time.Second)
	assert.Equal(t, int64(100), ev2.State.Position)

	// Unsubscribing sub1 must not affect sub2.
	cancel1()
	waitForClosed(t, sub1, time.Second)
	conn.emit(seekedSignal(200))
	ev2 = waitForEvent(t, sub2, time.Second)
	assert.Equal(t, int64(200), ev2.State.Position)
}

func TestSubscribe_SlowConsumerDropOldest(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test",
		WithEventBufferSize(2), WithSlowConsumerPolicy(SlowConsumerDropOldest))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Flood the buffer; hub processing must never block.
	for i := int64(0); i < 20; i++ {
		conn.emit(seekedSignal(i))
	}

	// Drain whatever survived: newest values must be present, ordering of delivered
	// events stays monotonic.
	events := collect(t, sub, 500*time.Millisecond)
	require.NotEmpty(t, events)
	for i := 1; i < len(events); i++ {
		assert.Less(t, events[i-1].State.Position, events[i].State.Position,
			"delivered events must stay in order")
	}
	assert.Equal(t, int64(19), events[len(events)-1].State.Position, "latest state must be retained")
}

func TestSubscribe_SlowConsumerTerminate(t *testing.T) {
	initial := PlayerState{PlaybackStatus: PlaybackStatusPlaying}
	conn := newFakeDBusConn(initial)
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test",
		WithEventBufferSize(1), WithSlowConsumerPolicy(SlowConsumerTerminate))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Fill the single slot and keep flooding until the hub terminates the
	// subscription.
	for i := int64(0); i < 50; i++ {
		conn.emit(seekedSignal(i))
	}

	var terminal PlayerStateEvent
	deadline := time.After(time.Second)
	for {
		select {
		case ev, ok := <-sub.Events:
			if !ok {
				require.ErrorIs(t, terminal.Err, ErrSlowConsumer)
				return
			}
			if ev.Err != nil {
				terminal = ev
			}
		case <-deadline:
			t.Fatal("slow consumer subscription was not terminated")
		}
	}
}

func TestSubscribe_ContextCancellationClosesCleanly(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	sub, err := player.Subscribe(ctx)
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	cancel()
	waitForClosed(t, sub, time.Second)

	// Unsubscribe after cancel is a safe no-op.
	sub.Unsubscribe()
}

func TestSubscribe_UnsubscribeIsIdempotentAndConcurrentSafe(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := player.Subscribe(ctx)
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.Unsubscribe()
		}()
	}
	wg.Wait()
	waitForClosed(t, sub, time.Second)
}

func TestSubscribe_PlayerDisconnectTerminatePolicy(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.vanish"
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player, sub, cancel := newSubscribedPlayer(t, conn, name,
		WithDisconnectPolicy(DisconnectTerminate))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.emitPlayerGone(name)
	waitTerminalErr(t, sub, ErrPlayerDisconnected)

	// Hub detached its signal registration before exiting.
	require.Eventually(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.removeSignals) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestSubscribe_PlayerDisconnectReconnectPolicy(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.vanish"
	conn := newFakeDBusConn(PlayerState{
		PlaybackStatus: PlaybackStatusPlaying,
		Metadata:       metadataFor("1", "Before"),
		Position:       100,
	})
	conn.owner[name] = ":1.42"
	player, sub, cancel := newSubscribedPlayer(t, conn, name,
		WithDisconnectPolicy(DisconnectReconnect),
		WithReconnectInterval(10*time.Millisecond))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	// Mark the player unreachable first; only then announce the disappearance, so no
	// poll can succeed against the old state before the disconnect is observed.
	conn.setPlayerUnreachable(true)
	conn.emitPlayerGone(name)
	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventDisconnected, ev.Kind)
	assert.Equal(t, "Before", mustTitle(t, ev.State.Metadata))

	// While gone, property reads fail; the hub must keep retrying instead of
	// terminating.
	time.Sleep(30 * time.Millisecond)

	// Player returns with a fresh state; a NameOwnerChanged plus readable properties.
	conn.setPlayerUnreachable(false)
	conn.setProperty(playerPlaybackStatusProperty, string(PlaybackStatusPaused))
	conn.setProperty(playerMetadataProperty, map[string]dbus.Variant(metadataFor("2", "After")))
	conn.setProperty(playerPositionProperty, int64(0))
	conn.emitPlayerBack(name)

	ev = waitForEvent(t, sub, 2*time.Second)
	require.Equal(t, EventReconnected, ev.Kind)
	assert.Equal(t, "After", mustTitle(t, ev.State.Metadata))
	assert.Equal(t, PlaybackStatusPaused, ev.State.PlaybackStatus)

	// Signals after reconnect keep flowing.
	conn.emit(seekedSignal(55))
	ev = waitForEvent(t, sub, time.Second)
	assert.Equal(t, EventSeeked, ev.Kind)
	assert.Equal(t, int64(55), ev.State.Position)
}

func TestSubscribe_ConnectionLossTerminatesAllSubscribers(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub1, err := player.Subscribe(ctx)
	require.NoError(t, err)
	sub2, err := player.Subscribe(ctx)
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub1, time.Second).Kind)
	require.Equal(t, EventInitial, waitForEvent(t, sub2, time.Second).Kind)

	// Closing the connection closes every registered signal channel, as godbus does.
	require.NoError(t, conn.Close())

	for _, sub := range []*Subscription{sub1, sub2} {
		waitTerminalErr(t, sub, ErrConnectionClosed)
	}
}

func TestPlayer_CloseTerminatesSubscriptions(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}

	sub, err := player.Subscribe(context.Background())
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	require.NoError(t, player.Close())

	var terminal PlayerStateEvent
	deadline := time.After(time.Second)
	for {
		select {
		case ev, ok := <-sub.Events:
			if !ok {
				require.ErrorIs(t, terminal.Err, ErrSubscriptionClosed)
				// Signal channel was deregistered before the hub exited.
				conn.mu.Lock()
				removed := len(conn.removeSignals)
				conn.mu.Unlock()
				assert.Equal(t, 1, removed)
				return
			}
			if ev.Err != nil {
				terminal = ev
			}
		case <-deadline:
			t.Fatal("subscription was not terminated on Player.Close")
		}
	}
}

func TestSubscribe_InvalidatedMetadataResetsPosition(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{
		PlaybackStatus: PlaybackStatusPlaying,
		Metadata:       metadataFor("1", "A"),
		Position:       900,
	})
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.emit(propertiesChangedSignal(nil, []string{"Metadata"}))
	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventPropertiesChanged, ev.Kind)
	assert.Nil(t, ev.State.Metadata)
	assert.Equal(t, int64(0), ev.State.Position)
}

func TestSubscribe_ResubscribeAfterTerminateCreatesFreshHub(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.vanish"
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: name, connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx1, cancel1 := context.WithCancel(context.Background())
	sub1, err := player.Subscribe(ctx1, WithDisconnectPolicy(DisconnectTerminate))
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub1, time.Second).Kind)

	conn.emitPlayerGone(name)
	waitTerminalErr(t, sub1, ErrPlayerDisconnected)
	cancel1()

	// Player becomes reachable again. A new subscription on the same Player must
	// establish a fresh hub.
	conn.setProperty(playerPlaybackStatusProperty, string(PlaybackStatusPlaying))
	conn.setProperty(playerMetadataProperty, map[string]dbus.Variant(metadataFor("2", "Back")))
	conn.setProperty(playerPositionProperty, int64(0))

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2, err := player.Subscribe(ctx2)
	require.NoError(t, err)
	ev := waitForEvent(t, sub2, time.Second)
	assert.Equal(t, EventInitial, ev.Kind)
}

func waitTerminalErr(t *testing.T, sub *Subscription, expected error) {
	t.Helper()
	var terminal PlayerStateEvent
	deadline := time.After(time.Second)
	for {
		select {
		case ev, ok := <-sub.Events:
			if !ok {
				require.ErrorIs(t, terminal.Err, expected)
				return
			}
			if ev.Err != nil {
				terminal = ev
			}
		case <-deadline:
			t.Fatal("timed out waiting for terminal error")
		}
	}
}

// state is a small test helper that mirrors the field for readability in assertions.
func (ev PlayerStateEvent) state() PlayerState { return ev.State }

func mustTitle(t *testing.T, md Metadata) string {
	t.Helper()
	title, err := md.XESAMTitle()
	require.NoError(t, err)
	return title
}

// setPlayerUnreachable flips the mock between "all property reads fail" and normal.
func (f *fakeDBusConn) setPlayerUnreachable(gone bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if gone {
		f.getPropertyErr = errors.New("name has no owner")
	} else {
		f.getPropertyErr = nil
	}
}

// countHubGoroutines reports how many live goroutines belong to subscription hubs
// (actor or context watchers). It is used to prove that no termination path leaks.
func countHubGoroutines() int {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	stack := string(buf[:n])
	return strings.Count(stack, "stateHub).runLoop") +
		strings.Count(stack, "stateHub).watchContext")
}

func TestSubscribe_NoGoroutineLeakOnAllTerminationPaths(t *testing.T) {
	require.Eventually(t, func() bool { return countHubGoroutines() == 0 },
		time.Second, 10*time.Millisecond, "precondition: no hub goroutines before test")

	// Path 1: context cancellation.
	func() {
		conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
		player := &Player{name: "org.mpris.MediaPlayer2.a", connection: conn}
		ctx, cancel := context.WithCancel(context.Background())
		sub, err := player.Subscribe(ctx)
		require.NoError(t, err)
		require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)
		cancel()
		waitForClosed(t, sub, time.Second)
		assert.NoError(t, player.Close())
	}()

	// Path 2: explicit Unsubscribe while the hub stays alive, then Close.
	func() {
		conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
		player := &Player{name: "org.mpris.MediaPlayer2.b", connection: conn}
		sub, err := player.Subscribe(context.Background())
		require.NoError(t, err)
		require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)
		sub.Unsubscribe()
		waitForClosed(t, sub, time.Second)
		assert.NoError(t, player.Close())
	}()

	// Path 3: player disappearance with DisconnectTerminate.
	func() {
		const name = "org.mpris.MediaPlayer2.c"
		conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
		conn.owner[name] = ":1.1"
		player := &Player{name: name, connection: conn}
		sub, err := player.Subscribe(context.Background(), WithDisconnectPolicy(DisconnectTerminate))
		require.NoError(t, err)
		require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)
		conn.emitPlayerGone(name)
		waitTerminalErr(t, sub, ErrPlayerDisconnected)
		assert.NoError(t, player.Close())
	}()

	// Path 4: connection loss.
	func() {
		conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
		player := &Player{name: "org.mpris.MediaPlayer2.d", connection: conn}
		sub, err := player.Subscribe(context.Background())
		require.NoError(t, err)
		require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)
		require.NoError(t, conn.Close())
		waitTerminalErr(t, sub, ErrConnectionClosed)
		assert.NoError(t, player.Close())
	}()

	require.Eventually(t, func() bool { return countHubGoroutines() == 0 },
		2*time.Second, 10*time.Millisecond,
		"hub or watcher goroutines leaked after all termination paths")
}

func TestSubscribe_HubWidePolicyIsFixedByFirstSubscriber(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.policy"
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	conn.owner[name] = ":1.7"
	player := &Player{name: name, connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	// First subscriber pins DisconnectTerminate on the shared hub.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	sub1, err := player.Subscribe(ctx1, WithDisconnectPolicy(DisconnectTerminate))
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub1, time.Second).Kind)

	// Second subscriber asks for Reconnect, but the hub must keep its pinned policy.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2, err := player.Subscribe(ctx2, WithDisconnectPolicy(DisconnectReconnect))
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, sub2, time.Second).Kind)

	conn.emitPlayerGone(name)
	waitTerminalErr(t, sub1, ErrPlayerDisconnected)
	waitTerminalErr(t, sub2, ErrPlayerDisconnected)
}

func TestSubscribe_SlowPeerDoesNotBlockOtherSubscriber(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.peer", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	// A deliberately tiny buffer that is never drained.
	ctxSlow, cancelSlow := context.WithCancel(context.Background())
	defer cancelSlow()
	slow, err := player.Subscribe(ctxSlow, WithEventBufferSize(1), WithSlowConsumerPolicy(SlowConsumerDropOldest))
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, slow, time.Second).Kind)

	// A healthy subscriber with a generous buffer.
	ctxFast, cancelFast := context.WithCancel(context.Background())
	defer cancelFast()
	fast, err := player.Subscribe(ctxFast, WithEventBufferSize(64))
	require.NoError(t, err)
	require.Equal(t, EventInitial, waitForEvent(t, fast, time.Second).Kind)

	// Far more events than the slow buffer can hold. Start at 1 because a Seeked to
	// the initial position 0 is a no-op snapshot and is correctly deduplicated.
	for i := int64(1); i <= 30; i++ {
		conn.emit(seekedSignal(i))
	}

	// The fast subscriber must observe every position in order, proving the slow
	// peer neither blocks the hub nor consumes shared events.
	events := collect(t, fast, time.Second)
	require.Len(t, events, 30)
	for i, ev := range events {
		assert.Equal(t, int64(i+1), ev.State.Position)
		assert.Equal(t, EventSeeked, ev.Kind)
	}
}

func TestSubscribe_MatchRuleRegistrationFailureLeavesNothingBehind(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	conn.failAddMatch = true

	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	_, err := player.Subscribe(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start state subscription")

	// No hub goroutine, no signal registration, and Close still works.
	assert.NoError(t, player.Close())
	assert.Empty(t, conn.signalChans)
}

func TestSubscribe_AlreadyCanceledContextIsRejected(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player := &Player{name: "org.mpris.MediaPlayer2.test", connection: conn}
	defer func() { assert.NoError(t, player.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := player.Subscribe(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSubscribe_ReconnectByPollingWithoutOwnershipSignal(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.poll"
	conn := newFakeDBusConn(PlayerState{
		PlaybackStatus: PlaybackStatusPlaying,
		Metadata:       metadataFor("1", "Before"),
	})
	conn.owner[name] = ":1.10"
	player, sub, cancel := newSubscribedPlayer(t, conn, name,
		WithDisconnectPolicy(DisconnectReconnect),
		WithReconnectInterval(10*time.Millisecond))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.setPlayerUnreachable(true)
	conn.emitPlayerGone(name)
	require.Equal(t, EventDisconnected, waitForEvent(t, sub, time.Second).Kind)

	// Reappear WITHOUT emitting the NameOwnerChanged signal: only update the owner
	// table and properties, so the polling fallback (GetNameOwner guard + state
	// fetch) must drive the reconnect.
	conn.mu.Lock()
	conn.owner[name] = ":1.11"
	conn.getPropertyErr = nil
	conn.properties[playerMetadataProperty] = dbus.MakeVariant(
		map[string]dbus.Variant(metadataFor("2", "After")))
	conn.mu.Unlock()

	ev := waitForEvent(t, sub, 2*time.Second)
	require.Equal(t, EventReconnected, ev.Kind)
	assert.Equal(t, "After", mustTitle(t, ev.State.Metadata))
}

func TestSubscribe_UnsubscribeDuringDisconnectWait(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.waiting"
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	conn.owner[name] = ":1.20"
	player, sub, cancel := newSubscribedPlayer(t, conn, name,
		WithReconnectInterval(50*time.Millisecond))
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.setPlayerUnreachable(true)
	conn.emitPlayerGone(name)
	require.Equal(t, EventDisconnected, waitForEvent(t, sub, time.Second).Kind)

	// Canceling while the actor is inside the reconnect wait must still close the
	// channel promptly.
	cancel()
	waitForClosed(t, sub, time.Second)
}

func TestSubscribe_ReconnectOwnerSignalBeforePropertiesAreReadable(t *testing.T) {
	const name = "org.mpris.MediaPlayer2.lagging"
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	conn.owner[name] = ":1.30"
	player, sub, cancel := newSubscribedPlayer(t, conn, name,
		WithReconnectInterval(10*time.Millisecond))
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.setPlayerUnreachable(true)
	conn.emitPlayerGone(name)
	require.Equal(t, EventDisconnected, waitForEvent(t, sub, time.Second).Kind)

	// Announce the new owner while properties are still unreadable; the hub must
	// retry instead of giving up.
	conn.emitPlayerBack(name)
	time.Sleep(20 * time.Millisecond)
	conn.setPlayerUnreachable(false)
	conn.setProperty(playerPlaybackStatusProperty, string(PlaybackStatusPaused))

	ev := waitForEvent(t, sub, 2*time.Second)
	require.Equal(t, EventReconnected, ev.Kind)
	assert.Equal(t, PlaybackStatusPaused, ev.State.PlaybackStatus)
}

func TestSubscribe_PositionPropertyChangeUpdatesSnapshot(t *testing.T) {
	conn := newFakeDBusConn(PlayerState{PlaybackStatus: PlaybackStatusPlaying})
	player, sub, cancel := newSubscribedPlayer(t, conn, "org.mpris.MediaPlayer2.test")
	defer cancel()
	defer func() { assert.NoError(t, player.Close()) }()
	require.Equal(t, EventInitial, waitForEvent(t, sub, time.Second).Kind)

	conn.emit(propertiesChangedSignal(map[string]dbus.Variant{
		playerPositionProperty: dbus.MakeVariant(int64(7777)),
	}, nil))
	ev := waitForEvent(t, sub, time.Second)
	require.Equal(t, EventPropertiesChanged, ev.Kind)
	assert.Equal(t, int64(7777), ev.State.Position)
}
