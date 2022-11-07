package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

type ConnectionInfo struct {
	SignatureScheme string `json:"signature_scheme"`
	Transport       string `json:"transport"`
	StdinPort       int    `json:"stdin_port"`
	ControlPort     int    `json:"control_port"`
	IOPubPort       int    `json:"iopub_port"`
	HBPort          int    `json:"hb_port"`
	ShellPort       int    `json:"shell_port"`
	Key             string `json:"key"`
	IP              string `json:"ip"`
}

type Sockets struct {
	ShellSocket   *zmq4.Socket
	ControlSocket *zmq4.Socket
	StdinSocket   *zmq4.Socket
	IOPubSocket   *zmq4.Socket
	HBSocket      *zmq4.Socket
	Key           []byte
}

func prepareSockets(connInfo ConnectionInfo) (Sockets, error) {
	// Initialize the socket group.
	var sockets Sockets

	var err error
	var ctx *zmq4.Context

	ctx, err = zmq4.NewContext()
	if err != nil {
		return sockets, err
	}
	
	// Create the shell socket, a request-reply socket that may receive messages from multiple frontend for
	// code execution, introspection, auto-completion, etc.

	sockets.ShellSocket, err = ctx.NewSocket(zmq4.ROUTER)
	if err != nil {
		return sockets, err
	}

	// Create the control socket. This socket is a duplicate of the shell socket where messages on this channel
	// should jump ahead of queued messages on the shell socket.
	sockets.ControlSocket, err = ctx.NewSocket(zmq4.ROUTER)
	if err != nil {
		return sockets, err
	}

	// Create the stdin socket, a request-reply socket used to request user input from a front-end. This is analogous
	// to a standard input stream.
	sockets.StdinSocket, err = ctx.NewSocket(zmq4.ROUTER)
	if err != nil {
		return sockets, err
	}

	// Create the iopub socket, a publisher for broadcasting data like stdout/stderr output, displaying execution
	// results or errors, kernel status, etc. to connected subscribers.
	sockets.IOPubSocket, err = ctx.NewSocket(zmq4.PUB)
	if err != nil {
		return sockets, err
	}

	// Create the heartbeat socket, a request-reply socket that only allows alternating recv-send (request-reply)
	// calls. It should echo the byte strings it receives to let the requester know the kernel is still alive.
	sockets.HBSocket, err = ctx.NewSocket(zmq4.REP)
	if err != nil {
		return sockets, err
	}

	// Bind the sockets.
	address := fmt.Sprintf("%v://%v:%%v", connInfo.Transport, connInfo.IP)
	err = sockets.ShellSocket.Bind(fmt.Sprintf(address, connInfo.ShellPort))
	if err != nil {
		return sockets, fmt.Errorf("could not listen on shell-socket: %w", err)
	}

	err = sockets.ControlSocket.Bind(fmt.Sprintf(address, connInfo.ControlPort))
	if err != nil {
		return sockets, fmt.Errorf("could not listen on control-socket: %w", err)
	}

	err = sockets.StdinSocket.Bind(fmt.Sprintf(address, connInfo.StdinPort))
	if err != nil {
		return sockets, fmt.Errorf("could not listen on stdin-socket: %w", err)
	}

	err = sockets.IOPubSocket.Bind(fmt.Sprintf(address, connInfo.IOPubPort))
	if err != nil {
		return sockets, fmt.Errorf("could not listen on iopub-socket: %w", err)
	}

	err = sockets.HBSocket.Bind(fmt.Sprintf(address, connInfo.HBPort))
	if err != nil {
		return sockets, fmt.Errorf("could not listen on hbeat-socket: %w", err)
	}

	// Set the message signing key.
	sockets.Key = []byte(connInfo.Key)

	return sockets, nil
}

func main() {
  flag.Parse()
  if flag.NArg() < 1 {
		log.Fatalln("Need a command line argument specifying the connection file.")
	}

  var connectionFile = flag.Arg(0)
  connectionData, err := ioutil.ReadFile(connectionFile)
  if err != nil {
    log.Fatal(err)
  }

  var connectionInfo ConnectionInfo
  err = json.Unmarshal(connectionData, &connectionInfo)
  if err != nil {
    log.Fatal(err)
  }

	sockets, err := prepareSockets(connectionInfo)
	if err != nil {
		log.Fatal(err)
	}

	var reactor = zmq4.NewReactor()

	var _/*hbHandler*/ = NewMessageChannels(sockets.HBSocket, reactor)
	var _/*shellHandler*/ = NewMessageChannels(sockets.ShellSocket, reactor)
	var _/*stdinHandler*/ = NewMessageChannels(sockets.StdinSocket, reactor)
	var _/*iopubHandlers*/ = NewMessageChannels(sockets.IOPubSocket, reactor)
	
	reactor.Run(100 * time.Microsecond)
}
