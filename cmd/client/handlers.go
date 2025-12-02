package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return publishGameLog(publishCh, gs.GetUsername(), fmt.Sprintf("%s won a war against %s", winner, loser))
		case gamelogic.WarOutcomeYouWon:
			return publishGameLog(publishCh, gs.GetUsername(), fmt.Sprintf("%s won a war against %s", winner, loser))
		case gamelogic.WarOutcomeDraw:
			return publishGameLog(publishCh, gs.GetUsername(), fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser))
		}
		fmt.Println("error: unknown war outcome")
		return pubsub.NackDiscard
	}
}

func publishGameLog(publishCh *amqp.Channel, usr, msg string) pubsub.Acktype {
	err := pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+usr,
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    usr,
		},
	)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
