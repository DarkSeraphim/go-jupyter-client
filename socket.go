package main

import (
	"syscall"

	"github.com/pebbe/zmq4"
)

type MessageChannels struct {
	readFrames [][]byte
	writeFrames [][]byte
	writeIndex int
	handler func(zmq4.State) error
	ReadChan <-chan [][]byte
	writeChan chan<- interface{}
}

func (handler *MessageChannels) WriteMessage(message [][]byte) {
	handler.writeChan <- message
}

func (handler *MessageChannels) append(frame []byte) {
	handler.readFrames = append(handler.readFrames, frame)
}

func (handler *MessageChannels) resetFrameBuffer() [][]byte {
  var frames = handler.readFrames
	handler.readFrames = make([][]byte, 0, 10)
	return frames
}

func (handler *MessageChannels) readAvailable(socket *zmq4.Socket, reads chan<- [][]byte) error {
	for {
	  var frame, err = socket.RecvBytes(zmq4.DONTWAIT)
	  if err != nil {
	  	if zmq4.AsErrno(err) == zmq4.Errno(syscall.EAGAIN) {
	  		return nil
	  	}
	  	return err
	  }
	  handler.append(frame)
	  var more bool
	  more, err = socket.GetRcvmore()
	  if err != nil {
	  	return err
	  }
	  if !more {
	  	var frames = handler.resetFrameBuffer()
	  	reads <- frames	
	  }
	}
}

func (handler *MessageChannels) writeAvailable(socket *zmq4.Socket, writes <-chan [][]byte) (bool, error) {
	for {
		if handler.writeIndex >= len(handler.writeFrames) {
			select {
				case nextMessage, ok := <-writes:
					if ok {
						handler.writeFrames = nextMessage
						handler.writeIndex = 0
					} else {
						panic("O no")
					}
				default:	
					return false, nil
			}		
		}

		var b = handler.writeFrames[handler.writeIndex]
		var flag = zmq4.DONTWAIT
		if handler.writeIndex < len(handler.writeFrames) - 1 {
			flag |= zmq4.SNDMORE
		}
		var _, err = socket.SendBytes(b, zmq4.DONTWAIT)
		if err != nil {
			if zmq4.AsErrno(err) == zmq4.Errno(syscall.EAGAIN) {
				return true, nil
			}
			return false, err
		}
		handler.writeIndex++
	}
}

func NewMessageChannels(socket *zmq4.Socket, reactor *zmq4.Reactor) MessageChannels {
	var writes = make(chan interface{})
	var writeQueue = make(chan [][]byte)
	// isWriting is only read/written by a single thread
	// so volatile is not required
	var isWriting bool
	var reads = make(chan [][]byte)
	var channels = MessageChannels {
		writeFrames: make([][]byte, 0, 0),
		writeIndex: 0,
		writeChan: writes,
		ReadChan: reads,
	}
	channels.resetFrameBuffer()
	channels.handler = func(s zmq4.State) error {
		if (s & zmq4.POLLIN != 0) {
			var err = channels.readAvailable(socket, reads)
			if err != nil {
				return err
			}
		}
		if (s & zmq4.POLLOUT != 0) {
			var hasMore, err = channels.writeAvailable(socket, writeQueue)
			if err != nil {
				return err
			}
			if !hasMore {
			  	reactor.AddSocket(socket, zmq4.POLLIN, channels.handler)
					isWriting = false
			}
		}
		return nil
	}
	reactor.AddSocket(socket, zmq4.POLLIN, channels.handler)
	reactor.AddChannel(writes, -1, func(i interface{}) error {
		var bytes, ok = i.([][]byte)
		if !ok {
			panic("Expected [][]byte in the write channel, but got something else")
		}
		writeQueue <- bytes
		if !isWriting {
			reactor.AddSocket(socket, zmq4.POLLIN | zmq4.POLLOUT, channels.handler)
			isWriting = true
		}
		return nil
	})
	return channels
}
