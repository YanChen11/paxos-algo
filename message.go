package main

type Message struct {
	From        string
	To          string
	Type        int
	Id          int64
	Value       string
}

const (
	Ping    = iota
	Pong
	Prepare
	Promise
	Nack
	Accept
	Accepted
)