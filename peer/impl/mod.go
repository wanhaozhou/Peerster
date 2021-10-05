package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	newNode := &node{
		config: conf,
		address: conf.Socket.GetAddress(),
		stop: make(chan bool),
		routingTable: make(map[string]string),
	}

	// initialize routingTable
	newNode.AddPeer(conf.Socket.GetAddress())
	// Task 4: Implement a chat messaging mechanism
	newNode.config.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, ExecChatMessage)
	return newNode
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	config peer.Configuration
	address string
	stop chan bool
	routingTable peer.RoutingTable
	routingTableMutex sync.RWMutex
}

// Start implements peer.Service
func (n *node) Start() error {
	go func() {
		for {
			select {
				case <- n.stop:
					close(n.stop)
					return

				default:
					pkt, err := n.config.Socket.Recv(time.Second * 1)
					if errors.Is(err, transport.TimeoutErr(0)) {
						continue
					}

					if pkt.Header.Destination == n.address {
						err = n.ProcessPacket(pkt)
					} else {
						err = n.RelayPacket(pkt)
					}

					if err != nil {
						log.Error().Err(err)
					}
			}
		}
	}()
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.stop <- true
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	// find if the dst is in the routing table
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	if _, present := n.routingTable[dest]; !present {
		return xerrors.Errorf("Cannot find destination: %v", dest)
	}

	// TODO: check the relay address
	header := transport.NewHeader(n.address, n.address, dest, 0)
	packet := transport.Packet{
		Header: &header,
		Msg: &msg,
	}
	err := n.config.Socket.Send(n.routingTable[dest], packet, 0)
	if err != nil {
		// TODO: log the error
		return err
	}
	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addrs ...string) {
	n.routingTableMutex.Lock()
	defer n.routingTableMutex.Unlock()

	for _, addr := range addrs {
		n.routingTable[addr] = addr
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	table := make(map[string]string)
	for k, v := range n.routingTable {
		table[k] = v
	}
	return table
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.routingTableMutex.Lock()
	defer n.routingTableMutex.Unlock()

	if len(relayAddr) == 0 {
		// TODO: potentially lose a neighbour
		delete(n.routingTable, origin)
		return
	}

	// TODO: add a new neighbour
	n.routingTable[origin] = relayAddr
}


func (n *node) ProcessPacket(pkt transport.Packet) error {
	log.Info().Msgf(
		"Receive packet from: %v, relay: %v, to: %v",
		pkt.Header.Source,
		pkt.Header.RelayedBy,
		n.address)
	return n.config.MessageRegistry.ProcessPacket(pkt)
}

func (n *node) RelayPacket(pkt transport.Packet) error {
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()
	if _, present := n.routingTable[pkt.Header.Destination]; !present {
		return xerrors.Errorf("Cannot relay the message: from %v to %v", n.address, pkt.Header.Destination)
	}
	newPktHeader := transport.NewHeader(pkt.Header.Source, n.address, pkt.Header.Destination, pkt.Header.TTL - 1)
	newPktMsg := pkt.Msg.Copy()
	return n.config.Socket.Send(
		n.routingTable[pkt.Header.Destination],
		transport.Packet{
			Header: &newPktHeader,
			Msg: &newPktMsg,
		},
		0)
}

func ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	log.Info().Msgf("ExecChatMessage: receive chat message: %v", chatMsg)
	return nil
}