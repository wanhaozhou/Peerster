package impl

import (
	"errors"
	"fmt"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
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
		stop: false,
		routingTable: make(map[string]string),
	}

	// initialize routingTable
	newNode.AddPeer(conf.Socket.GetAddress())
	return newNode
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	config peer.Configuration
	address string
	stop bool
	routingTable peer.RoutingTable
	routingTableMutex sync.RWMutex
}

// Start implements peer.Service
func (n *node) Start() error {
	go func() {
		for {
			// check if the node receives a stop signal
			if n.stop {
				return
			}

			pkt, err := n.config.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}

			// TODO: handle pkt
			if pkt.Header.Destination == n.address {
				fmt.Println("received packet: ", pkt)
				n.config.MessageRegistry.ProcessPacket(pkt)
			} else {
				fmt.Println("received packet of others: ", pkt)
			}
		}
	}()
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.stop = true
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	// find if the dst is in the routing table
	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	if _, present := n.routingTable[dest]; !present {
		return xerrors.Errorf("cannot find destination: %v", dest)
	}

	header := transport.NewHeader(n.address, n.address, dest, 0)
	packet := transport.Packet{
		Header: &header,
		Msg: &msg,
	}
	err := n.config.Socket.Send(dest, packet, 0)
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