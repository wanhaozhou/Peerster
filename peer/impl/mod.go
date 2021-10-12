package impl

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
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
		sequence: atomicUint{},
		neighbourTable: neighbourTable{values: make(map[string]int)},
		view: view{values: make(map[string]uint)},
		ackRumors: ackRumors{values: make(map[string][]types.Rumor)},
		ackChanMap: ackChanMap{values: make(map[string]chan bool)},
	}

	if conf.AntiEntropyInterval > 0 {
		newNode.antiEntropyTicker = time.NewTicker(conf.AntiEntropyInterval)
	}

	if conf.HeartbeatInterval > 0 {
		newNode.heartbeatTicker = time.NewTicker(conf.HeartbeatInterval)
	}

	// initialize routingTable
	newNode.AddPeer(conf.Socket.GetAddress())
	// Task 4: Implement a chat messaging mechanism
	newNode.config.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, newNode.ExecChatMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, newNode.ExecRumorsMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, newNode.ExecAckMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, newNode.ExecEmptyMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, newNode.ExecStatusMessage)
	return newNode
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	routingTableMutex sync.RWMutex

	config peer.Configuration
	address string
	stop chan bool
	routingTable peer.RoutingTable
	sequence atomicUint
	neighbourTable neighbourTable
	view view
	ackRumors ackRumors
	antiEntropyTicker *time.Ticker
	heartbeatTicker *time.Ticker
	ackChanMap ackChanMap
}

// Start implements peer.Service
func (n *node) Start() error {
	// activate the main service
	go func() {
		for {
			select {
				case <- n.stop:
					return

				default:
					pkt, err := n.config.Socket.Recv(time.Millisecond * 100)
					if errors.Is(err, transport.TimeoutErr(0)) {
						continue
					}

					if pkt.Header.Destination == n.address {
						err = n.processPacket(pkt)
					} else {
						err = n.relayPacket(pkt)
					}

					if err != nil {
						log.Error().Err(err)
					}
			}
		}
	}()

	// activate the anti-entropy mechanism
	go func() {
		for n.antiEntropyTicker != nil {
			select {
			case <- n.stop:
				n.antiEntropyTicker.Stop()
				return
			case <- n.antiEntropyTicker.C:
				n.antiEntropy()
			}
		}
	}()

	// activate heart beat
	go func() {
		for n.heartbeatTicker != nil {
			select {
			case <- n.stop:
				n.heartbeatTicker.Stop()
				return
			case <- n.heartbeatTicker.C:
				n.heartbeat()
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
	return n.config.Socket.Send(n.routingTable[dest], packet, 0)
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addrs ...string) {
	n.routingTableMutex.Lock()
	n.neighbourTable.Lock()
	defer n.neighbourTable.Unlock()
	defer n.routingTableMutex.Unlock()

	for _, addr := range addrs {
		n.routingTable[addr] = addr
		// add this peer to the neighbour table
		if _, present := n.neighbourTable.values[addr]; !present && addr != n.address {
			n.neighbourTable.values[addr] = 1
		}
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
	n.neighbourTable.Lock()
	defer n.neighbourTable.Unlock()
	defer n.routingTableMutex.Unlock()
	log.Info().Msgf("[%v] Setting routing entry: origin=%v, relayAddr=%v", n.address, origin, relayAddr)

	if len(relayAddr) == 0 {
		// TODO: potentially lose a neighbour
		if relay, presentInRouting := n.routingTable[origin]; presentInRouting {
			_, presentInNeighbour := n.neighbourTable.values[relay]
			if presentInNeighbour {
				n.neighbourTable.values[relay]--
				if n.neighbourTable.values[relay] == 0 {
					delete(n.neighbourTable.values, relay)
				}
			}
			delete(n.routingTable, origin)
		}
		return
	}

	// TODO: add a new neighbour
	n.routingTable[origin] = relayAddr
	n.neighbourTable.values[relayAddr]++
}

func (n *node) Broadcast(msg transport.Message) error {
	// send it to a random neighbour
	neighbour, _ := n.getRandomNeighbour()
	return n.broadCast(msg, neighbour, true)
}


// ExecChatMessage TODO: check
func (n *node) ExecChatMessage(msg types.Message, _ transport.Packet) error {
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	log.Info().Msgf("[%v] ExecChatMessage: receive chat message: %v", n.address, chatMsg)
	return nil
}

// ExecEmptyMessage TODO: check
func (n *node) ExecEmptyMessage(msg types.Message, _ transport.Packet) error {
	emptyMsg, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	log.Info().Msgf("[%v] ExecEmptyMessage: receive empty message: %v", n.address, emptyMsg)
	return nil
}

// ExecAckMessage TODO: check
func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}

	log.Info().Msgf("[%v] ExecAckMessage: receive ACK message from: %v, on pkt: %v", n.address, pkt.Header.Source, ackMsg.AckedPacketID)
	n.ackChanMap.RLock()
	ackChan := n.ackChanMap.values[ackMsg.AckedPacketID]
	n.ackChanMap.RUnlock()
	if ackChan != nil {
		go func() {
			select {
				case ackChan <- true:
					log.Info().Msgf("[%v] Syncing ACK for packet %v successful", n.address, ackMsg.AckedPacketID)
				case <-time.After(time.Second):
						log.Info().Msgf("[%v] Timeout syncing ACK for packet %v", n.address, ackMsg.AckedPacketID)
			}
		}()
	}
	// proceed to process the embedded status message
	message, err := n.config.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		return err
	}
	return n.config.MessageRegistry.ProcessPacket(transport.Packet{
		Header: pkt.Header,
		Msg: &message,
	})
}

// ExecRumorsMessage TODO: check
func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	rumorMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	log.Info().Msgf("[%v] ExecRumorsMessage: receives rumors message: %v, from: %v", n.address, rumorMsg, pkt.Header.Source)

	if pkt.Header.Source == n.address {
		// this is a local message from the current node
		// should not happen
		log.Error().Msgf("[%v] Process a local rumor message %v", n.address, rumorMsg)
		return nil
	}

	// process each rumor and update routing table
	expected := false
	for _, rumor := range rumorMsg.Rumors {
		expected = expected || n.handleSingleRumor(rumor, pkt)
	}

	// send ack
	log.Info().Msgf("[%v] Sending ACK to %v", n.address, pkt.Header.Source)
	ackMsg := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status: n.view.getValues(),
	}
	n.sendMessageUnchecked(pkt.Header.Source, ackMsg)
	log.Info().Msgf("[%v] Sent ACK to %v", n.address, pkt.Header.Source)

	if expected {
		// choose a random neighbour to broadcast
		neighbour, err := n.getRandomNeighbourExclude(pkt.Header.RelayedBy)
		if err != nil {
			// we cannot find another neighbour to send the rumor
			// simply abort the action
			log.Info().Err(err)
			return nil
		}

		log.Info().Msgf("[%v] Sending the rumor to: %v", n.address, neighbour)
		pktHeader := transport.NewHeader(n.address, n.address, neighbour, pkt.Header.TTL - 1)
		pktMsg, err := n.config.MessageRegistry.MarshalMessage(rumorMsg)
		if err != nil {
			return err
		}
		pktForward := transport.Packet{
			Header: &pktHeader,
			Msg: &pktMsg,
		}
		return n.config.Socket.Send(neighbour, pktForward, 0)
	}

	return nil
}

// ExecStatusMessage TODO:
func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	log.Info().Msgf("[%v] ExecStatusMessage: receive status message from: %v, content: %v", n.address, pkt.Header.Source, statusMsg)

	myStatus := n.getStatusMessage()
	peerStatus := *statusMsg
	inSync := true

	// check if remote peer has more messages
	for k, v := range peerStatus {
		if myStatus[k] < v {
			n.sendMessageUnchecked(pkt.Header.Source, myStatus)
			inSync = false
			break
		}
	}

	var rumors []types.Rumor

	// check if I have more messages
	for k, v := range myStatus {
		if peerStatus[k] < v {
			n.ackRumors.RLock()
			rumors = append(rumors, n.ackRumors.values[k][peerStatus[k]:v]...)
			n.ackRumors.RUnlock()
		}
	}

	// send the rumors
	if len(rumors) > 0 {
		log.Info().Msgf("[%v] Sending missing messages to: %v, content: %v", n.address, pkt.Header.Source, rumors)
		inSync = false
		n.sendMessageUnchecked(
			pkt.Header.Source,
			types.RumorsMessage{
				Rumors: rumors,
			})
	}

	// ContinueMongering
	if inSync && rand.Float64() < n.config.ContinueMongering {
		neighbour, err := n.getRandomNeighbourExclude(pkt.Header.Source)
		if err == nil {
			log.Info().Msgf("[%v] Continue mongering. Sending message to: %v", n.address, neighbour)
			n.sendMessageUnchecked(neighbour, myStatus)
		}
	}

	return nil
}


func (n *node) broadCast(msg transport.Message, neighbour string, ack bool) error {
	// create a rumor message
	n.ackRumors.Lock()
	rumor := types.Rumor{
		Origin: n.address,
		Sequence: n.sequence.incrementAndGet(),
		Msg: &msg,
	}
	rumorMsg := types.RumorsMessage{
		Rumors: []types.Rumor{rumor},
	}
	n.ackRumors.values[n.address] = append(n.ackRumors.values[n.address], rumor)
	n.ackRumors.Unlock()

	if len(neighbour) <= 0 {
		// we cannot find another neighbour to send the rumor
		// simply abort the action
		return nil
	}

	// marshal the message
	transportMsg, err := n.config.MessageRegistry.MarshalMessage(rumorMsg)
	if err != nil {
		return err
	}

	header := transport.NewHeader(n.address, n.address, neighbour, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg: &transportMsg,
	}
	err = n.config.Socket.Send(neighbour, pkt, 0)
	if err != nil {
		return err
	}

	if ack && n.config.AckTimeout > 0 {
		ackTicker := time.NewTicker(n.config.AckTimeout)
		id := pkt.Header.PacketID
		ackChan := make(chan bool)
		go func() {
			n.ackChanMap.Lock()
			n.ackChanMap.values[id] = ackChan
			n.ackChanMap.Unlock()
			for {
				select {
				case <- ackTicker.C:
					log.Info().Msgf("[%v] Timeout receiving ACK for packet %v", n.address, id)
					nextNeighbour, _ := n.getRandomNeighbourExclude(neighbour)
					if len(nextNeighbour) > 0 {
						broadCastErr := n.broadCast(msg, nextNeighbour, false)
						if broadCastErr != nil {
							log.Error().Err(broadCastErr)
						}
					}
					return
				case <- ackChan:
					return
				}
			}
		}()
	}

	// process the message locally
	header = transport.NewHeader(n.address, n.address, n.address, 0)
	pkt = transport.Packet{
		Header: &header,
		Msg: &msg,
	}
	err = n.config.MessageRegistry.ProcessPacket(pkt)
	return err
}

// processPacket processes the pkt.
func (n *node) processPacket(pkt transport.Packet) error {
	log.Info().Msgf(
		"[%v] Receive packet: id: %v, type: %v, from: %v, relay: %v, to: %v",
		n.address,
		pkt.Header.PacketID,
		pkt.Msg.Type,
		pkt.Header.Source,
		pkt.Header.RelayedBy,
		pkt.Header.Destination)

	return n.config.MessageRegistry.ProcessPacket(pkt)
}

// relayPacket tries to relay the packet based on the routing table.
func (n *node) relayPacket(pkt transport.Packet) error {
	log.Info().Msgf("[%v] Relaying packet #%v from %v to %v", n.address, pkt.Header.PacketID, pkt.Header.Source, pkt.Header.Destination)
	sourceAddress := pkt.Header.Source
	destAddress := pkt.Header.Destination

	n.routingTableMutex.RLock()
	defer n.routingTableMutex.RUnlock()

	if _, present := n.routingTable[destAddress]; !present {
		return xerrors.Errorf("Cannot relay the message: from %v to %v", n.address, destAddress)
	}

	newPktHeader := transport.NewHeader(sourceAddress, n.address, destAddress, pkt.Header.TTL - 1)
	newPktMsg := pkt.Msg.Copy()
	return n.config.Socket.Send(
		n.routingTable[destAddress],
		transport.Packet{
			Header: &newPktHeader,
			Msg: &newPktMsg,
		},
		0)
}

func (n *node) sendMessageUnchecked(dest string, message types.Message) {
	transportMessage, err := n.config.MessageRegistry.MarshalMessage(message)
	if err != nil {
		log.Error().Err(err)
		return
	}
	header := transport.NewHeader(n.address, n.address, dest, 0)
	err = n.config.Socket.Send(dest, transport.Packet{
		Header: &header,
		Msg:    &transportMessage,
	}, 0)

	if err != nil {
		log.Error().Err(err)
		return
	}
	log.Info().Msgf("[%v] Sent %v message to dest %v, content: %v", n.address, message.Name(), dest, message)
}

func (n *node) handleSingleRumor(rumor types.Rumor, pkt transport.Packet) bool {
	err := n.handleMessageInRumor(rumor.Msg, pkt)
	if err != nil {
		log.Error().Err(err)
		return false
	}

	if rumor.Origin == n.address {
		log.Info().Msgf("[%v] rumor %v, packet id: %v, from %v, relay %v, to %v", n.address, rumor, pkt.Header.PacketID, pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination)
	}

	n.view.Lock()
	n.ackRumors.Lock()
	defer n.ackRumors.Unlock()
	defer n.view.Unlock()

	expected := false
	rumorSource := rumor.Origin
	rumorSeq := rumor.Sequence
	if rumorSeq == n.view.values[rumorSource] + 1 {
		expected = true
		n.view.values[rumorSource]++
		rumorMsgCopy := rumor.Msg.Copy()
		n.ackRumors.values[rumorSource] = append(n.ackRumors.values[rumorSource], types.Rumor{
			Origin: rumorSource,
			Sequence: rumorSeq,
			Msg: &rumorMsgCopy,
		})
		n.SetRoutingEntry(rumorSource, pkt.Header.RelayedBy)
	}
	return expected
}

func (n *node) handleMessageInRumor(msg *transport.Message, pkt transport.Packet) error {
	newPkt := transport.Packet{
		Header: pkt.Header,
		Msg: msg,
	}
	return n.config.MessageRegistry.ProcessPacket(newPkt)
}

func (n *node) getRandomNeighbour() (string, error) {
	return n.getRandomNeighbourExclude("")
}

func (n *node) getRandomNeighbourExclude(exclude string) (string, error) {
	err := xerrors.Errorf("There are no neighbours for the peer: %v", n.address)
	n.neighbourTable.RLock()
	defer n.neighbourTable.RUnlock()

	if len(n.neighbourTable.values) <= 0 {
		return "", err
	}

	var neighbours []string
	for k := range n.neighbourTable.values {
		if k == exclude {
			continue
		}
		neighbours = append(neighbours, k)
	}

	if len(neighbours) > 0 {
		return neighbours[rand.Intn(len(neighbours))], nil
	}
	return "", err
}

func (n *node) antiEntropy() {
	neighbour, err := n.getRandomNeighbour()
	if err != nil {
		log.Info().Err(err)
		return
	}

	status := n.getStatusMessage()
	message, err := n.config.MessageRegistry.MarshalMessage(status)
	if err != nil {
		log.Info().Err(err)
		return
	}

	err = n.Unicast(neighbour, message)
	if err != nil {
		log.Info().Err(err)
	}
}

func (n *node) heartbeat() {
	emptyMessage := types.EmptyMessage{}
	message, err := n.config.MessageRegistry.MarshalMessage(emptyMessage)
	log.Info().Msgf("Heartbeat from: %v", n.address)
	if err != nil {
		log.Info().Err(err)
		return
	}
	neighbour, err:= n.getRandomNeighbour()
	if err != nil {
		log.Info().Err(err)
		return
	}

	err = n.broadCast(message, neighbour, false)
	if err != nil {
		log.Info().Err(err)
	}
}

func (n *node) getStatusMessage() types.StatusMessage {
	status := n.view.getValues()
	seq := n.sequence.get()
	if seq > 0 {
		status[n.address] = seq
	}
	return status
}

// atomicUint
type atomicUint struct {
	sync.RWMutex
	num uint
}

func (a *atomicUint) incrementAndGet() uint {
	a.Lock()
	defer a.Unlock()
	a.num++
	return a.num
}

func (a *atomicUint) get() uint {
	a.RLock()
	defer a.RUnlock()
	return a.num
}

// neighbourTable
type neighbourTable struct {
	sync.RWMutex
	values map[string]int
}

// view
type view struct {
	sync.RWMutex
	values map[string]uint
}

func (v *view) getValues() map[string]uint {
	v.RLock()
	defer v.RUnlock()
	status := make(map[string]uint)
	for k, vv := range v.values {
		status[k] = vv
	}
	return status
}

// ackRumors
type ackRumors struct {
	sync.RWMutex
	values map[string][]types.Rumor
}

type ackChanMap struct {
	sync.RWMutex
	values map[string]chan bool
}