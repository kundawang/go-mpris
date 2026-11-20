// Command subscribe demonstrates mpris.Player.Subscribe: it prints one consistent
// player-state snapshot per D-Bus signal change, survives the player disappearing and
// reappearing, and stops cleanly on Ctrl-C.
//
// Run with:
//
//	go run ./examples/subscribe org.mpris.MediaPlayer2.vlc
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/leberKleber/go-mpris"
)

func main() {
	playerName := "org.mpris.MediaPlayer2.vlc"
	if len(os.Args) > 1 {
		playerName = os.Args[1]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	player, err := mpris.NewPlayer(playerName)
	if err != nil {
		log.Fatalf("connect to %s: %s", playerName, err)
	}
	defer func() { _ = player.Close() }()

	subscription, err := player.Subscribe(ctx,
		mpris.WithEventBufferSize(64),
		mpris.WithDisconnectPolicy(mpris.DisconnectReconnect),
		mpris.WithSlowConsumerPolicy(mpris.SlowConsumerDropOldest),
	)
	if err != nil {
		log.Fatalf("subscribe: %s", err)
	}

	for event := range subscription.Events {
		if event.Err != nil {
			// Terminal event: the channel is closed right after it.
			log.Fatalf("subscription ended: %s", event.Err)
		}

		title, _ := event.State.Metadata.XESAMTitle()
		log.Printf("%-19s status=%-7s position=%10dµs title=%q",
			event.Kind, event.State.PlaybackStatus, event.State.Position, title)
	}
}
