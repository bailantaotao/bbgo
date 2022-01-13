package interact

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/scanner"

	log "github.com/sirupsen/logrus"
)

type Reply interface {
	Message(message string)
	AddButton(text string)
	RemoveKeyboard()
}

// Responder defines the logic of responding the message
type Responder func(message string, reply Reply, ctxObjects ...interface{}) error

type CustomInteraction interface {
	Commands(interact *Interact)
}

type State string

const (
	StatePublic        State = "public"
	StateAuthenticated State = "authenticated"
)

type TextMessageResponder interface {
	SetTextMessageResponder(responder Responder)
}

type CommandResponder interface {
	AddCommand(command string, responder Responder)
}

type Messenger interface {
	TextMessageResponder
	CommandResponder
	Start(ctx context.Context)
}

// Interact implements the interaction between bot and message software.
type Interact struct {
	// commands is the default public command map
	commands map[string]*Command

	// privateCommands is the private command map, need auth
	privateCommands map[string]*Command

	states     map[State]State
	statesFunc map[State]interface{}

	originState, currentState State

	messenger Messenger
}

func New() *Interact {
	return &Interact{
		commands:     make(map[string]*Command),
		originState:  StatePublic,
		currentState: StatePublic,
		states:       make(map[State]State),
		statesFunc:   make(map[State]interface{}),
	}
}

func (it *Interact) SetOriginState(s State) {
	it.originState = s
}

func (it *Interact) AddCustomInteraction(custom CustomInteraction) {
	custom.Commands(it)
}

func (it *Interact) PrivateCommand(command string, f interface{}) *Command {
	cmd := NewCommand(command, f)
	it.privateCommands[command] = cmd
	return cmd
}

func (it *Interact) Command(command string, f interface{}) *Command {
	cmd := NewCommand(command, f)
	it.commands[command] = cmd
	return cmd
}

func (it *Interact) getNextState(currentState State) (nextState State, final bool) {
	var ok bool
	final = false
	nextState, ok = it.states[currentState]
	if ok {
		// check if it's the final state
		if _, hasTransition := it.statesFunc[nextState]; !hasTransition {
			final = true
		}

		return nextState, final
	}

	// state not found, return to the origin state
	return it.originState, final
}

func (it *Interact) setState(s State) {
	log.Infof("[interact] transiting state from %s -> %s", it.currentState, s)
	it.currentState = s
}

func (it *Interact) handleResponse(text string, ctxObjects ...interface{}) error {
	args := parseCommand(text)

	f, ok := it.statesFunc[it.currentState]
	if !ok {
		return fmt.Errorf("state function of %s is not defined", it.currentState)
	}

	_, err := parseFuncArgsAndCall(f, args, ctxObjects...)
	if err != nil {
		return err
	}

	nextState, end := it.getNextState(it.currentState)
	if end {
		it.setState(it.originState)
		return nil
	}

	it.setState(nextState)
	return nil
}

func (it *Interact) getCommand(command string) (*Command, error) {
	switch it.currentState {
	case StateAuthenticated:
		if cmd, ok := it.privateCommands[command]; ok {
			return cmd, nil
		}

	case StatePublic:
		if _, ok := it.privateCommands[command]; ok {
			return nil, fmt.Errorf("private command can not be executed in the public mode")
		}

	}

	if cmd, ok := it.commands[command]; ok {
		return cmd, nil
	}

	return nil, fmt.Errorf("command %s not found", command)
}

func (it *Interact) runCommand(command string, args []string, ctxObjects ...interface{}) error {
	cmd, err := it.getCommand(command)
	if err != nil {
		return err
	}

	it.setState(cmd.initState)
	if _, err := parseFuncArgsAndCall(cmd.F, args, ctxObjects...); err != nil {
		return err
	}

	// if we can successfully execute the command, then we can go to the next state.
	nextState, end := it.getNextState(it.currentState)
	if end {
		it.setState(it.originState)
		return nil
	}

	it.setState(nextState)
	return nil
}

func (it *Interact) SetMessenger(messenger Messenger) {
	// pass Responder function
	messenger.SetTextMessageResponder(func(message string, reply Reply, ctxObjects ...interface{}) error {
		return it.handleResponse(message, append(ctxObjects, reply)...)
	})
	it.messenger = messenger
}

// builtin initializes the built-in commands
func (it *Interact) builtin() error {
	it.Command("/uptime", func(reply Reply) error {
		reply.Message("uptime")
		return nil
	})

	return nil
}

func (it *Interact) init() error {
	if err := it.builtin(); err != nil {
		return err
	}

	for n, cmd := range it.commands {
		for s1, s2 := range cmd.states {
			if _, exist := it.states[s1]; exist {
				return fmt.Errorf("state %s already exists", s1)
			}

			it.states[s1] = s2
		}
		for s, f := range cmd.statesFunc {
			it.statesFunc[s] = f
		}

		// register commands to the service
		if it.messenger == nil {
			return fmt.Errorf("messenger is not set")
		}

		commandName := n
		it.messenger.AddCommand(commandName, func(message string, reply Reply, ctxObjects ...interface{}) error {
			args := parseCommand(message)
			return it.runCommand(commandName, args, append(ctxObjects, reply)...)
		})
	}

	return nil
}

func (it *Interact) Start(ctx context.Context) error {
	if err := it.init(); err != nil {
		return err
	}

	// TODO: use go routine and context
	it.messenger.Start(ctx)
	return nil
}

func parseCommand(src string) (args []string) {
	var s scanner.Scanner
	s.Init(strings.NewReader(src))
	s.Filename = "command"
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		text := s.TokenText()
		if text[0] == '"' && text[len(text)-1] == '"' {
			text, _ = strconv.Unquote(text)
		}
		args = append(args, text)
	}

	return args
}

func parseFuncArgsAndCall(f interface{}, args []string, objects ...interface{}) (State, error) {
	fv := reflect.ValueOf(f)
	ft := reflect.TypeOf(f)

	argIndex := 0

	var rArgs []reflect.Value
	for i := 0; i < ft.NumIn(); i++ {
		at := ft.In(i)

		switch k := at.Kind(); k {

		case reflect.Interface:
			found := false

			for oi := 0; oi < len(objects); oi++ {
				obj := objects[oi]
				objT := reflect.TypeOf(obj)
				objV := reflect.ValueOf(obj)

				fmt.Println(
					at.PkgPath(),
					at.Name(),
					objT, "implements", at, "=", objT.Implements(at),
				)

				if objT.Implements(at) {
					found = true
					rArgs = append(rArgs, objV)
					break
				}
			}

			if !found {
				v := reflect.Zero(at)
				rArgs = append(rArgs, v)
			}

		case reflect.String:
			av := reflect.ValueOf(args[argIndex])
			rArgs = append(rArgs, av)
			argIndex++

		case reflect.Bool:
			bv, err := strconv.ParseBool(args[argIndex])
			if err != nil {
				return "", err
			}
			av := reflect.ValueOf(bv)
			rArgs = append(rArgs, av)
			argIndex++

		case reflect.Int64:
			nf, err := strconv.ParseInt(args[argIndex], 10, 64)
			if err != nil {
				return "", err
			}

			av := reflect.ValueOf(nf)
			rArgs = append(rArgs, av)
			argIndex++

		case reflect.Float64:
			nf, err := strconv.ParseFloat(args[argIndex], 64)
			if err != nil {
				return "", err
			}

			av := reflect.ValueOf(nf)
			rArgs = append(rArgs, av)
			argIndex++
		}
	}

	out := fv.Call(rArgs)
	if ft.NumOut() == 0 {
		return "", nil
	}

	// try to get the error object from the return value
	var state State
	var err error
	for i := 0; i < ft.NumOut(); i++ {
		outType := ft.Out(i)
		switch outType.Kind() {
		case reflect.String:
			if outType.Name() == "State" {
				state = State(out[i].String())
			}

		case reflect.Interface:
			o := out[i].Interface()
			switch ov := o.(type) {
			case error:
				err = ov

			}

		}
	}
	return state, err
}