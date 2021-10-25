package impl

import (
	"errors"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Improve logging
// Snippet taken from: https://github.com/dedis/dela/blob/6aaa2373492e8b5740c0a1eb88cf2bc7aa331ac0/mod.go#L59

const EnvLogLevel = "LLVL"
const defaultLevel = zerolog.NoLevel
func init() {
	lvl := os.Getenv(EnvLogLevel)
	var level zerolog.Level

	switch lvl {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	case "no":
		level = zerolog.NoLevel
	default:
		level = defaultLevel
	}
	Logger = Logger.Level(level)
}

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

// Logger is a globally available logger instance. By default, it only prints
// error level messages but it can be changed through a environment variable.
var Logger = zerolog.New(logout).Level(defaultLevel).
	With().Timestamp().Logger().
	With().Caller().Logger()


// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	newNode := &node{
		config: conf,
		address: conf.Socket.GetAddress(),
		stop: make(chan bool),
		routingTable: routingTable{values:make(map[string]string)},
		neighbourTable: neighbourTable{values: make(map[string]bool)},
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
	newNode.config.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, newNode.ExecPrivateMessage)
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
	routingTable routingTable
	neighbourTable neighbourTable
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
						Logger.Error().Err(err)
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
	n.routingTable.RLock()
	defer n.routingTable.RUnlock()

	if _, present := n.routingTable.values[dest]; !present {
		return xerrors.Errorf("[%v] Cannot find destination: %v", n.address, dest)
	}

	header := transport.NewHeader(n.address, n.address, dest, 0)
	packet := transport.Packet{
		Header: &header,
		Msg: &msg,
	}
	return n.config.Socket.Send(n.routingTable.values[dest], packet, 0)
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addrs ...string) {
	n.routingTable.Lock()
	n.neighbourTable.Lock()
	defer n.neighbourTable.Unlock()
	defer n.routingTable.Unlock()

	for _, addr := range addrs {
		n.routingTable.values[addr] = addr
		if addr != n.address {
			n.neighbourTable.values[addr] = true
		}
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.routingTable.RLock()
	defer n.routingTable.RUnlock()

	table := make(map[string]string)
	for k, v := range n.routingTable.values {
		table[k] = v
	}
	return table
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.routingTable.Lock()
	n.neighbourTable.Lock()
	defer n.neighbourTable.Unlock()
	defer n.routingTable.Unlock()
	Logger.Info().Msgf("[%v] Setting routing entry: origin=%v, relayAddr=%v", n.address, origin, relayAddr)

	if len(relayAddr) == 0 {
		// TODO: potentially lose a neighbour
		Logger.Error().Msgf("[%v] Losing neighbour", n.address)
		delete(n.routingTable.values, origin)
		return
	}

	// TODO: add a new neighbour
	n.routingTable.values[origin] = relayAddr
	n.neighbourTable.values[relayAddr] = true
}

func (n *node) Broadcast(msg transport.Message) error {
	// send it to a random neighbour
	neighbour, _ := n.getRandomNeighbour()
	return n.broadCast(msg, neighbour, true, true)
}


// ExecChatMessage implements the handler for types.ChatMessage
func (n *node) ExecChatMessage(msg types.Message, _ transport.Packet) error {
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecChatMessage: receive chat message: %v", n.address, chatMsg)
	return nil
}

// ExecPrivateMessage implements the handler for types.PrivateMessage
func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	var err error
	if _, present := privateMsg.Recipients[n.address]; present {
		Logger.Info().Msgf("[%v] ExecPrivateMessage: receive private message: %v", n.address, privateMsg)
		err = n.config.MessageRegistry.ProcessPacket(transport.Packet{
			Header: pkt.Header,
			Msg:    privateMsg.Msg,
		})
	}
	return err
}

// ExecEmptyMessage implements the handler for types.EmptyMessage
func (n *node) ExecEmptyMessage(msg types.Message, _ transport.Packet) error {
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecEmptyMessage: receive empty message", n.address)
	return nil
}

// ExecAckMessage implements the handler for types.AckMessage
func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecAckMessage And Check Neighbour: receive ACK message from: %v, on pkt: %v", n.address, pkt.Header.Source, ackMsg.AckedPacketID)

	n.checkNeighbour(pkt.Header.Source)

	n.ackChanMap.RLock()
	ackChan := n.ackChanMap.values[ackMsg.AckedPacketID]
	n.ackChanMap.RUnlock()
	if ackChan != nil {
		go func() {
			select {
				case ackChan <- true:
					Logger.Info().Msgf("[%v] Sending ACK for packet %v successful to channel", n.address, ackMsg.AckedPacketID)
				case <-time.After(time.Second):
					Logger.Info().Msgf("[%v] Timeout syncing ACK for packet %v", n.address, ackMsg.AckedPacketID)
			}
		}()
	}

	Logger.Info().Msgf("[%v] Start to process status message in ACK", n.address)
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

// ExecRumorsMessage implements the handler for types.RumorsMessage
func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	rumorMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecRumorsMessage And Check Neighbour: receives rumors message: %v, from: %v, relayed by: %v",
		n.address,
		rumorMsg,
		pkt.Header.Source,
		pkt.Header.RelayedBy,
	)

	n.checkNeighbour(pkt.Header.Source)

	if pkt.Header.Source == n.address {
		// this is a local message from the current node
		// should not happen
		Logger.Error().Msgf("[%v] Process a local rumor message %v", n.address, rumorMsg)
		return nil
	}

	if pkt.Header.Source != pkt.Header.RelayedBy {
		Logger.Error().Msgf("[%v] Should Not Happen! Received a rumors message from %v, relayed by %v", n.address, pkt.Header.Source, pkt.Header.RelayedBy)
		//return nil
	}

	// process each rumor and update routing table
	expected := false
	for _, rumor := range rumorMsg.Rumors {
		expected = expected || n.handleSingleRumor(rumor, pkt)
	}

	// send ack
	Logger.Info().Msgf("[%v] Sending ACK to %v", n.address, pkt.Header.Source)
	ackMsg := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status: n.getStatusMessage(),
	}
	n.sendMessageUnchecked(pkt.Header.Source, ackMsg)
	Logger.Info().Msgf("[%v] Sent ACK to %v", n.address, pkt.Header.Source)

	if expected {
		// choose a random neighbour to broadcast
		neighbour, err := n.getRandomNeighbourExclude(pkt.Header.RelayedBy)
		//neighbour, err := n.getRandomNeighbourExclude(pkt.Header.Source)
		if err != nil {
			// we cannot find another neighbour to send the rumor, so simply abort the action
			Logger.Info().Err(err)
			return nil
		}

		Logger.Info().Msgf("[%v] Message is expected. Broadcasting the rumor to: %v", n.address, neighbour)
		n.sendMessageUnchecked(neighbour, rumorMsg)
	}

	return nil
}

// ExecStatusMessage implements the handler for types.StatusMessage
func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecStatusMessage And Check Neighbour: receive status message from: %v, content: %v", n.address, pkt.Header.Source, statusMsg)

	n.checkNeighbour(pkt.Header.Source)

	myStatus := n.getStatusMessage()
	peerStatus := *statusMsg
	inSync := true

	// check if remote peer has more messages
	for k, v := range peerStatus {
		if myStatus[k] < v {
			Logger.Info().Msgf("[%v] Is missing information, syncing with: %v", n.address, pkt.Header.Source)
			Logger.Info().Msgf("[%v] local status: %v, remote %v 's status: %v", n.address, myStatus, pkt.Header.Source, peerStatus)
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
		Logger.Info().Msgf("[%v] Sending missing messages to: %v, content: %v", n.address, pkt.Header.Source, rumors)
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
			Logger.Info().Msgf("[%v] Continue mongering. Sending message to: %v", n.address, neighbour)
			n.sendMessageUnchecked(neighbour, myStatus)
		}
	}

	return nil
}


func (n *node) broadCast(msg transport.Message, neighbour string, ack bool, process bool) error {
	// create a rumor message
	n.ackRumors.Lock()

	// process the message locally
	if process {
		header := transport.NewHeader(n.address, n.address, n.address, 0)
		pkt := transport.Packet{
			Header: &header,
			Msg: &msg,
		}
		err := n.config.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			Logger.Error().Err(err)
		}
	}

	currentSequence := uint(len(n.ackRumors.values[n.address]) + 1)
	rumor := types.Rumor{
		Origin: n.address,
		Sequence: currentSequence,
		Msg: &msg,
	}
	rumorMsg := types.RumorsMessage{
		Rumors: []types.Rumor{rumor},
	}

	// update
	n.ackRumors.values[n.address] = append(n.ackRumors.values[n.address], rumor)
	n.ackRumors.Unlock()

	if len(neighbour) <= 0 {
		// we cannot find another neighbour to send the rumor
		// simply abort the action
		return nil
	}

	// send the message
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
	Logger.Info().Msgf("[%v] Initiate a rumor to %v, packet id: %v, requires ack: %v, process locally: %v", n.address, neighbour, pkt.Header.PacketID, ack, process)


	// wait for ack
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
						Logger.Info().Msgf("[%v] Timeout receiving ACK for packet %v", n.address, id)
						nextNeighbour, _ := n.getRandomNeighbourExclude(neighbour)
						if len(nextNeighbour) > 0 {
							// find the rumor in history
							n.ackRumors.RLock()
							resendRumor := n.ackRumors.values[n.address][currentSequence-1]
							n.ackRumors.RUnlock()
							n.sendMessageUnchecked(nextNeighbour, types.RumorsMessage{
								Rumors: []types.Rumor{resendRumor},
							})
						}

						n.ackChanMap.Lock()
						delete(n.ackChanMap.values, id)
						n.ackChanMap.Unlock()
						return
					case <- ackChan:
						Logger.Info().Msgf("[%v] Receiving ACK for packet %v successful on channel", n.address, id)

						n.ackChanMap.Lock()
						delete(n.ackChanMap.values, id)
						n.ackChanMap.Unlock()
						return
				}
			}
		}()
	}

	return err
}

// processPacket processes the pkt.
func (n *node) processPacket(pkt transport.Packet) error {
	Logger.Info().Msgf(
		"[%v] Socket receives [%v] packet, id: %v, from: %v, relay: %v, to: %v",
		n.address,
		pkt.Msg.Type,
		pkt.Header.PacketID,
		pkt.Header.Source,
		pkt.Header.RelayedBy,
		pkt.Header.Destination)

	return n.config.MessageRegistry.ProcessPacket(pkt)
}

// relayPacket tries to relay the packet based on the routing table.
func (n *node) relayPacket(pkt transport.Packet) error {
	Logger.Info().Msgf("[%v] Relaying packet #%v from %v to %v", n.address, pkt.Header.PacketID, pkt.Header.Source, pkt.Header.Destination)
	sourceAddress := pkt.Header.Source
	destAddress := pkt.Header.Destination

	n.routingTable.RLock()
	defer n.routingTable.RUnlock()

	if _, present := n.routingTable.values[destAddress]; !present {
		return xerrors.Errorf("Cannot relay the message: from %v to %v", n.address, destAddress)
	}

	newPktHeader := transport.NewHeader(sourceAddress, n.address, destAddress, pkt.Header.TTL - 1)
	newPktMsg := pkt.Msg.Copy()
	return n.config.Socket.Send(
		n.routingTable.values[destAddress],
		transport.Packet{
			Header: &newPktHeader,
			Msg: &newPktMsg,
		},
		0)
}

func (n *node) sendMessageUnchecked(dest string, message types.Message) {
	transportMessage, err := n.config.MessageRegistry.MarshalMessage(message)
	if err != nil {
		Logger.Error().Err(err)
		return
	}
	header := transport.NewHeader(n.address, n.address, dest, 0)
	err = n.config.Socket.Send(dest, transport.Packet{
		Header: &header,
		Msg:    &transportMessage,
	}, 0)

	if err != nil {
		Logger.Error().Err(err)
		return
	}
	Logger.Info().Msgf("[%v] Send Message Unchecked: type: %v, to: %v, content: %v", n.address, message.Name(), dest, message)
}

func (n *node) handleSingleRumor(rumor types.Rumor, pkt transport.Packet) bool {

	if rumor.Origin == n.address {
		Logger.Info().Msgf("[%v] Received rumor created by me! " +
			"Content: %v, packet id: %v, from %v, relay %v, to %v",
			n.address,
			rumor,
			pkt.Header.PacketID,
			pkt.Header.Source,
			pkt.Header.RelayedBy,
			pkt.Header.Destination,
		)
	}

	n.ackRumors.Lock()
	defer n.ackRumors.Unlock()

	expected := false
	rumorSource := rumor.Origin
	rumorSeq := rumor.Sequence
	if rumorSeq == uint(len(n.ackRumors.values[rumorSource]) + 1) {
		expected = true
		rumorMsgCopy := rumor.Msg.Copy()
		n.ackRumors.values[rumorSource] = append(n.ackRumors.values[rumorSource], types.Rumor{
			Origin: rumorSource,
			Sequence: rumorSeq,
			Msg: &rumorMsgCopy,
		})
		n.SetRoutingEntry(rumorSource, pkt.Header.RelayedBy)
		Logger.Error().Err(n.handleMessageInRumor(rumor.Msg, pkt))
	}
	return expected
}

func (n *node) handleMessageInRumor(msg *transport.Message, pkt transport.Packet) error {
	newPkt := transport.Packet{
		Header: pkt.Header,
		Msg: msg,
	}
	Logger.Info().Msgf("[%v] Process message in rumor", n.address)
	return n.config.MessageRegistry.ProcessPacket(newPkt)
}

func (n *node) antiEntropy() {
	neighbour, err := n.getRandomNeighbour()
	if err != nil {
		Logger.Info().Err(err)
		return
	}

	Logger.Info().Msgf("[%v] Anti-entropy message to %v", n.address, neighbour)
	n.sendMessageUnchecked(neighbour, n.getStatusMessage())
}

func (n *node) heartbeat() {
	emptyMessage := types.EmptyMessage{}
	message, err := n.config.MessageRegistry.MarshalMessage(emptyMessage)
	if err != nil {
		Logger.Info().Err(err)
		return
	}
	neighbour, err:= n.getRandomNeighbour()
	if err != nil {
		Logger.Info().Err(err)
		return
	}

	Logger.Info().Msgf("[%v] Heartbeat message", n.address)
	err = n.broadCast(message, neighbour, false, false)
	if err != nil {
		Logger.Info().Err(err)
	}
}

func (n *node) checkNeighbour(neighbour string) {
	if len(neighbour) > 0 && neighbour != n.address {
		n.neighbourTable.Lock()
		defer n.neighbourTable.Unlock()
		if !n.neighbourTable.values[neighbour] {
			Logger.Info().Msgf("[%v] Is missing neighbour %v", n.address, neighbour)
			n.neighbourTable.values[neighbour] = true
		}
	}
}

func (n *node) getStatusMessage() types.StatusMessage {
	n.ackRumors.RLock()
	defer n.ackRumors.RUnlock()
	status := make(map[string]uint)
	for k, v := range n.ackRumors.values {
		status[k] = uint(len(v))
	}
	return status
}

// neighbourTable
type neighbourTable struct {
	sync.RWMutex
	values map[string]bool
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


type routingTable struct {
	sync.RWMutex
	values map[string]string
}


type ackRumors struct {
	sync.RWMutex
	values map[string][]types.Rumor
}


type ackChanMap struct {
	sync.RWMutex
	values map[string]chan bool
}