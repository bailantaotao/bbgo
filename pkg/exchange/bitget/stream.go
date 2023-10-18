package bitget

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/c9s/bbgo/pkg/exchange/bitget/bitgetapi"
	"github.com/c9s/bbgo/pkg/types"
)

const (
	// spotArgsLimit can input up to 10 args for each subscription request sent to one connection.
	spotArgsLimit = 10
)

//go:generate callbackgen -type Stream
type Stream struct {
	types.StandardStream

	booksEventCallbacks []func(o BookEvent)
}

func NewStream() *Stream {
	stream := &Stream{
		StandardStream: types.NewStandardStream(),
	}

	stream.SetEndpointCreator(stream.createEndpoint)
	stream.SetParser(stream.parseWebSocketEvent)
	stream.SetDispatcher(stream.dispatchEvent)
	stream.OnConnect(stream.handlerConnect)

	stream.OnBooksEvent(stream.handleBooksEvent)
	return stream
}

func (s *Stream) syncSubscriptions(opType WsEventType) error {
	if opType != WsEventUnsubscribe && opType != WsEventSubscribe {
		return fmt.Errorf("unexpected subscription type: %v", opType)
	}

	logger := log.WithField("opType", opType)
	lens := len(s.Subscriptions)
	for begin := 0; begin < lens; begin += spotArgsLimit {
		end := begin + spotArgsLimit
		if end > lens {
			end = lens
		}

		topics := []WebsocketArg{}
		for _, subscription := range s.Subscriptions[begin:end] {
			topic, err := s.convertSubscription(subscription)
			if err != nil {
				logger.WithError(err).Errorf("convert error, subscription: %+v", subscription)
				return err
			}

			topics = append(topics, topic)
		}

		logger.Infof("%s channels: %+v", opType, topics)
		if err := s.Conn.WriteJSON(WsOp{
			Op:   opType,
			Args: topics,
		}); err != nil {
			logger.WithError(err).Error("failed to send request")
			return err
		}
	}

	return nil
}

func (s *Stream) Unsubscribe() {
	// errors are handled in the syncSubscriptions, so they are skipped here.
	_ = s.syncSubscriptions(WsEventUnsubscribe)
	s.Resubscribe(func(old []types.Subscription) (new []types.Subscription, err error) {
		// clear the subscriptions
		return []types.Subscription{}, nil
	})
}

func (s *Stream) createEndpoint(_ context.Context) (string, error) {
	var url string
	if s.PublicOnly {
		url = bitgetapi.PublicWebSocketURL
	} else {
		url = bitgetapi.PrivateWebSocketURL
	}
	return url, nil
}

func (s *Stream) dispatchEvent(event interface{}) {
	switch e := event.(type) {
	case *WsOpEvent:
		if err := e.IsValid(); err != nil {
			log.Errorf("invalid event: %v", err)
		}

	case *BookEvent:
		s.EmitBooksEvent(*e)
	}
}

func (s *Stream) parseWebSocketEvent(in []byte) (interface{}, error) {
	var e WsEvent

	err := json.Unmarshal(in, &e)
	if err != nil {
		return nil, err
	}

	switch {
	case e.IsOp():
		return e.WsOpEvent, nil

	case e.IsChannel():
		switch e.Arg.Channel {
		case ChannelOrderBook, ChannelOrderBook5, ChannelOrderBook15:
			var books BookEvent
			err = json.Unmarshal(e.WsChannelEvent.Data, &books.Events)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal data into BookEvent: %+v, err: %w", string(e.WsChannelEvent.Data), err)
			}

			books.Type = e.WsChannelEvent.Action
			books.instId = e.Arg.InstId
			return &books, nil
		}
	}

	return nil, fmt.Errorf("unhandled websocket event: %+v", string(in))
}

func (s *Stream) handlerConnect() {
	if s.PublicOnly {
		// errors are handled in the syncSubscriptions, so they are skipped here.
		_ = s.syncSubscriptions(WsEventSubscribe)
	} else {
		log.Error("*** PRIVATE API NOT IMPLEMENTED ***")
	}
}

func (s *Stream) convertSubscription(sub types.Subscription) (WebsocketArg, error) {
	w := WebsocketArg{
		// support spot only
		InstType: instSp,
		Channel:  "",
		InstId:   sub.Symbol,
	}

	switch sub.Channel {
	case types.BookChannel:
		w.Channel = ChannelOrderBook5

		switch sub.Options.Depth {
		case types.DepthLevel15:
			w.Channel = ChannelOrderBook15
		case types.DepthLevel200:
			log.Info("*** The subscription events for the order book may return fewer than 200 bids/asks at a depth of 200. ***")
			w.Channel = ChannelOrderBook
		}
		return w, nil
	}

	return w, fmt.Errorf("unsupported stream channel: %s", sub.Channel)
}

func (s *Stream) handleBooksEvent(o BookEvent) {
	for _, book := range o.OrderBooks() {
		switch {
		case o.Type == ActionTypeSnapshot:
			s.EmitBookSnapshot(book)

		case o.Type == ActionTypeUpdate:
			s.EmitBookUpdate(book)
		}
	}
}
