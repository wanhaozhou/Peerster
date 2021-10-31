package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strings"
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
		config:                 conf,
		address:                conf.Socket.GetAddress(),
		stop:                   make(chan bool, 1),
		routingTable:           routingTable{values:make(map[string]string)},
		neighbourTable:         neighbourTable{values: make(map[string]bool)},
		ackRumors:              ackRumors{values: make(map[string][]types.Rumor)},
		ackChanMap:             ackChanMap{values: make(map[string]chan bool)},
		catalog:                catalog{values: make(map[string]map[string]struct{}), valuesArray: make(map[string][]string)},
		ackDataRequest:         ackDataRequest{values: make(map[string]chan []byte)},
		ackSearchAllRequest:    ackSearchAllRequest{values: make(map[string]chan []string)},
		ackSearchFirstRequest:  ackSearchFirstRequest{values: make(map[string]chan string)},
		processedSearchRequest: processedSearchRequest{values: make(map[string]bool)},
	}

	if conf.AntiEntropyInterval > 0 {
		newNode.antiEntropyTicker = time.NewTicker(conf.AntiEntropyInterval)
	}

	if conf.HeartbeatInterval > 0 {
		newNode.heartbeatTicker = time.NewTicker(conf.HeartbeatInterval)
	}

	newNode.AddPeer(conf.Socket.GetAddress())
	newNode.config.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, newNode.ExecChatMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, newNode.ExecRumorsMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, newNode.ExecAckMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, newNode.ExecEmptyMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, newNode.ExecStatusMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, newNode.ExecPrivateMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, newNode.ExecDataRequestMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, newNode.ExecDataReplyMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, newNode.ExecSearchRequestMessage)
	newNode.config.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, newNode.ExecSearchReplyMessage)
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
	catalog catalog
	ackDataRequest        ackDataRequest
	ackSearchAllRequest   ackSearchAllRequest
	ackSearchFirstRequest ackSearchFirstRequest
	processedSearchRequest processedSearchRequest
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
					Logger.Info().Msg(err.Error())
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

	// Initial heart beat
	go func() {
		if n.heartbeatTicker != nil {
			n.heartbeat()
		}
	}()

	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	select {
	case n.stop <- true:
		break
	default:
		Logger.Info().Msgf("[%v] Cannot send stop signal. Shutting down ungracefully", n.address)
	}
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	// find if the dst is in the routing table
	n.routingTable.RLock()
	defer n.routingTable.RUnlock()

	if _, present := n.routingTable.values[dest]; !present {
		err := xerrors.Errorf("[%v] Cannot find destination: %v", n.address, dest)
		Logger.Info().Msg(err.Error())
		return err
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
			Logger.Info().Msg(err.Error())
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
			Logger.Error().Msg(err.Error())
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
		Logger.Error().Msg(err.Error())
		return
	}
	header := transport.NewHeader(n.address, n.address, dest, 0)
	err = n.config.Socket.Send(dest, transport.Packet{
		Header: &header,
		Msg:    &transportMessage,
	}, 0)

	if err != nil {
		Logger.Error().Msg(err.Error())
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
		Logger.Info().Msgf(
			"[%v] The rumor is expected. Rumor source=%v, Pkt Source=%v, Pkt Relayed by=%v",
			n.address, rumorSource, pkt.Header.Source, pkt.Header.RelayedBy)
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
		Logger.Info().Msg(err.Error())
		return
	}

	Logger.Info().Msgf("[%v] Anti-entropy message to %v", n.address, neighbour)
	n.sendMessageUnchecked(neighbour, n.getStatusMessage())
}

func (n *node) heartbeat() {
	emptyMessage := types.EmptyMessage{}
	message, err := n.config.MessageRegistry.MarshalMessage(emptyMessage)
	if err != nil {
		Logger.Info().Msg(err.Error())
		return
	}
	neighbour, _:= n.getRandomNeighbour()
	Logger.Info().Msgf("[%v] Heartbeat message to: %v", n.address, neighbour)
	err = n.broadCast(message, neighbour, false, false)
	if err != nil {
		Logger.Info().Msg(err.Error())
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

type ackDataRequest struct {
	sync.RWMutex
	values map[string]chan []byte
}

type ackSearchAllRequest struct {
	sync.RWMutex
	values map[string]chan []string
}

type ackSearchFirstRequest struct {
	sync.RWMutex
	values map[string]chan string
}

type processedSearchRequest struct {
	sync.RWMutex
	values map[string]bool
}

type catalog struct {
	sync.RWMutex
	values map[string]map[string]struct{}
	valuesArray map[string][]string
}


func (n *node) getNeighbours() ([]string, error) {
	return n.getNeighboursExclude("")
}

func (n *node) getNeighboursExclude(exclude string) ([]string, error) {
	err := xerrors.Errorf("There are no neighbours for the peer: %v", n.address)
	n.neighbourTable.RLock()
	defer n.neighbourTable.RUnlock()

	var neighbours []string

	if len(n.neighbourTable.values) <= 0 {
		return neighbours, err
	}

	for k := range n.neighbourTable.values {
		if k == exclude {
			continue
		}
		neighbours = append(neighbours, k)
	}

	if len(neighbours) > 0 {
		rand.Shuffle(
			len(neighbours),
			func(i, j int) {
				neighbours[i], neighbours[j] = neighbours[j], neighbours[i]
			})
		return neighbours, nil
	}
	return neighbours, err
}

func (n *node) Upload(data io.Reader) (string, error) {
	store := n.config.Storage.GetDataBlobStore()
	buf := make([]byte, n.config.ChunkSize)
	var metaHashSlice []byte
	var chunkHashes []string

	for {
		bytes, err := data.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		chunkHash := crypto.SHA256.New()
		_, err = chunkHash.Write(buf[:bytes])
		if err != nil {
			return "", err
		}

		// compute and store the hash for the current chunk
		chunkHashSlice := chunkHash.Sum(nil)
		chunkHashHex := hex.EncodeToString(chunkHashSlice)
		store.Set(chunkHashHex, append([]byte(nil), buf[:bytes]...))
		Logger.Info().Msgf("[%v] Writes a chunk into local storage with hash %v", n.address, chunkHashHex)

		// store it into meta file
		metaHashSlice = append(metaHashSlice, chunkHashSlice...)
		chunkHashes = append(chunkHashes, chunkHashHex)
	}

	metaHash := crypto.SHA256.New()
	_, err := metaHash.Write(metaHashSlice)
	if err != nil {
		return "", err
	}

	metaHashHex := hex.EncodeToString(metaHash.Sum(nil))
	store.Set(metaHashHex, []byte(strings.Join(chunkHashes, peer.MetafileSep)))
	Logger.Info().Msgf("[%v] Writes a metafile into local storage with hash %v", n.address, metaHashHex)
	return metaHashHex, nil
}

func (n *node) GetCatalog() peer.Catalog {
	n.catalog.RLock()
	defer n.catalog.RUnlock()
	catalogCopy := make(map[string]map[string]struct{})

	for fileHash, peers := range n.catalog.values {
		peersCopy := make(map[string]struct{})
		for p := range peers {
			peersCopy[p] = struct{}{}
		}
		catalogCopy[fileHash] = peersCopy
	}

	return catalogCopy
}

func (n *node) UpdateCatalog(key string, peer string) {
	if key == n.address {
		return
	}

	n.catalog.Lock()
	defer n.catalog.Unlock()

	if _, present := n.catalog.values[key]; !present {
		n.catalog.values[key] = make(map[string]struct{})
	}
	if _, present := n.catalog.values[key][peer]; present {
		return
	}

	n.catalog.values[key][peer] = struct{}{}
	n.catalog.valuesArray[key] = append(n.catalog.valuesArray[key], peer)
	Logger.Info().Msgf("[%v] Updates catalog with key=%v, peer=%v", n.address, key, peer)
}

func (n *node) Download(metaHash string) ([]byte, error) {
	store := n.config.Storage.GetDataBlobStore()

	var err error
	metaFile := store.Get(metaHash)
	if metaFile != nil {
		Logger.Info().Msgf("[%v] Find file of hash=%v locally", n.address, metaHash)
	} else {
		Logger.Info().Msgf("[%v] Cannot find file of hash=%v locally", n.address, metaHash)
		metaFile, err = n.downloadFromPeers(metaHash)
	}

	if metaFile == nil || err != nil {
		return nil, err
	}

	chunkHashes := strings.Split(string(metaFile), peer.MetafileSep)
	if len(chunkHashes) <= 0 {
		return nil, xerrors.Errorf("[%v] Fail to parse the meta file of hash=%v", n.address, metaHash)
	}

	var allChunks [][]byte
	var allBytes []byte

	for i, chunkHash := range chunkHashes {
		Logger.Info().Msgf("[%v] Try to fetch chunk[%v] of file %v", n.address, i, metaHash)
		var file []byte
		file = store.Get(chunkHash)
		if file != nil {
			Logger.Info().Msgf("[%v] Find file of hash=%v locally", n.address, metaHash)
		} else {
			Logger.Info().Msgf("[%v] Cannot find file of hash=%v locally", n.address, chunkHash)
			file, err = n.downloadFromPeers(chunkHash)
			if err != nil {
				return nil, err
			}
		}
		allChunks = append(allChunks, append([]byte(nil), file...))
		allBytes = append(allBytes, file...)
		continue
	}

	if len(allChunks) != len(chunkHashes) {
		err = xerrors.Errorf("[%v] Chunk bytes and chunk hash size does not match", n.address)
		Logger.Error().Msg(err.Error())
		return nil, err
	}

	n.config.Storage.GetDataBlobStore().Set(metaHash, metaFile)
	for i, chunkBytes := range allChunks {
		n.config.Storage.GetDataBlobStore().Set(chunkHashes[i], append([]byte(nil), chunkBytes...))
	}
	return allBytes, nil
}


func (n *node) Tag(name string, mh string) error {
	n.config.Storage.GetNamingStore().Set(name, []byte(mh))
	return nil
}


func (n *node) Resolve(name string) string {
	return string(n.config.Storage.GetNamingStore().Get(name))
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	nameSet := make(map[string]bool)
	var names []string

	// First search locally
	store := n.config.Storage.GetNamingStore()
	store.ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) {
			nameSet[key] = true
		}
		return true
	})

	neighbours, err := n.getNeighbours()
	if err != nil || budget <= 0 {
		// The current peer does not have any neighbours,
		// or we do not have budget to search for others
		Logger.Info().Msg(err.Error())
		for k := range nameSet {
			names = append(names, k)
		}
		return names, nil
	}

	assignedBudgets := n.assignBudgets(budget, uint(len(neighbours)))
	requestId := xid.New().String()
	dataChan := make(chan []string, budget)
	n.ackSearchAllRequest.Lock()
	n.ackSearchAllRequest.values[requestId] = dataChan
	n.ackSearchAllRequest.Unlock()

	// Send the request to each neighbour
	for i, assignedBudget := range assignedBudgets {
		neighbour := neighbours[i]
		message := types.SearchRequestMessage{
			RequestID: requestId,
			Origin:    n.address,
			Pattern:   reg.String(),
			Budget:    assignedBudget,
		}
		Logger.Info().Msgf("[%v] Sending search all requests to: %v", n.address, neighbour)
		n.sendMessageUnchecked(neighbour, message)
	}

	// Wait for the reply
	loop:
	for {
		select {
		case <- time.After(timeout):
			break loop
		case receivedNames := <- dataChan:
			if len(receivedNames) > 0 {
				for _, name := range receivedNames {
					nameSet[name] = true
				}
			}
		}
	}

	for k := range nameSet {
		names = append(names, k)
	}
	return names, nil
}

func (n *node) SearchFirst(reg regexp.Regexp, conf peer.ExpandingRing) (string, error) {
	var nameHolder []string

	// First search locally
	store := n.config.Storage.GetNamingStore()
	store.ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) && n.haveAllChunksLocally(string(val)) {
			nameHolder = append(nameHolder, key)
			return false
		}
		return true
	})


	if len(nameHolder) > 0 {
		return nameHolder[0], nil
	}

	neighbours, err := n.getNeighbours()
	if err != nil || conf.Initial <= 0 {
		// The current peer does not have any neighbours,
		// or we do not have budget to search for others
		return "", nil
	}

	loop:
	for attempt := uint(0); attempt < conf.Retry; attempt++ {
		currentBudget := conf.Initial * uint(n.pow(conf.Factor, attempt))
		assignedBudgets := n.assignBudgets(currentBudget, uint(len(neighbours)))

		requestId := xid.New().String()
		dataChan := make(chan string, currentBudget)
		n.ackSearchFirstRequest.Lock()
		n.ackSearchFirstRequest.values[requestId] = dataChan
		n.ackSearchFirstRequest.Unlock()

		for i, assignedBudget := range assignedBudgets {
			neighbour := neighbours[i]
			message := types.SearchRequestMessage{
				RequestID: requestId,
				Origin:    n.address,
				Pattern:   reg.String(),
				Budget:    assignedBudget,
			}
			Logger.Info().Msgf("[%v] Sending search first requests to: %v", n.address, neighbour)
			n.sendMessageUnchecked(neighbour, message)
		}

		select {
		case <- time.After(conf.Timeout):
			continue loop
		case match := <- dataChan:
			nameHolder = append(nameHolder, match)
			Logger.Info().Msgf("[%v] Received search first answer: %v", n.address, match)
			break loop
		}
	}

	if len(nameHolder) > 0 {
		return nameHolder[0], nil
	}
	return "", nil
}

// ExecDataRequestMessage implements the handler for types.DataRequestMessage
func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	dataRequestMsg, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecDataRequestMessage: receive data request message from %v, replayed by %v", n.address, pkt.Header.Source, pkt.Header.RelayedBy)

	value := n.config.Storage.GetDataBlobStore().Get(dataRequestMsg.Key)
	Logger.Info().Msgf("[%v] ExecDataRequestMessage: my value for request %v is: %v", n.address, dataRequestMsg.RequestID, value)

	dataReplyMsg := types.DataReplyMessage{
		RequestID: dataRequestMsg.RequestID,
		Key:       dataRequestMsg.Key,
		Value:     value,
	}

	// The message must be sent back using the routing table.
	err := n.uniCastMessage(pkt.Header.Source, dataReplyMsg)
	if err != nil {
		neighbours, _ := n.getNeighbours()
		Logger.Info().Msgf("[%v] ExecDataRequestMessage: cannot sent to: %v, routing table: %v, neighbours: %v",
			n.address, pkt.Header.Source, n.GetRoutingTable(), neighbours)
	}
	return err
}

// ExecDataReplyMessage implements the handler for types.DataReplyMessage
func (n *node) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	dataReplyMessage, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecDataReplyMessage: receive data reply message from %v, replayed by %v", n.address, pkt.Header.Source, pkt.Header.RelayedBy)
	n.ackDataRequest.Lock()
	n.ackDataRequest.values[dataReplyMessage.RequestID] <- append([]byte(nil), dataReplyMessage.Value...)
	n.ackDataRequest.Unlock()
	Logger.Info().Msgf("[%v] ExecDataReplyMessage: send bytes in to channel id=%v", n.address, dataReplyMessage.RequestID)
	return nil
}

// ExecSearchRequestMessage implements the handler for types.SearchRequestMessage
func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	searchRequestMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecSearchRequestMessage: receive search request message from %v, replayed by %v", n.address, pkt.Header.Source, pkt.Header.RelayedBy)

	// Check if the search request is already processed
	n.processedSearchRequest.Lock()
	processed := n.processedSearchRequest.values[searchRequestMsg.RequestID]
	if !processed {
		// Mark it as processed
		n.processedSearchRequest.values[searchRequestMsg.RequestID] = true
	}
	n.processedSearchRequest.Unlock()
	if processed {
		return nil
	}

	// Process the new search request message
	remainingBudgets := searchRequestMsg.Budget - 1
	if remainingBudgets > 0 {
		// Forward it to neighbours
		go func() {
			neighbours, err := n.getNeighboursExclude(pkt.Header.Source)
			if err != nil {
				return
			}
			// The forwarded request must have all the same attributes of the original request except the budget.
			// The packet's header Origin and RelayedBy must be set to the peer's socket address.
			budgets := n.assignBudgets(remainingBudgets, uint(len(neighbours)))
			for i, budget:= range budgets {
				newSearchRequestMsg := types.SearchRequestMessage{
					RequestID: searchRequestMsg.RequestID,
					Origin: searchRequestMsg.Origin,
					Pattern: searchRequestMsg.Pattern,
					Budget: budget,
				}
				n.sendMessageUnchecked(neighbours[i], newSearchRequestMsg)
			}
		}()
	}

	pattern := regexp.MustCompile(searchRequestMsg.Pattern)
	fileInfos := n.searchLocalStorage(*pattern)
	message := types.SearchReplyMessage{
		RequestID: searchRequestMsg.RequestID,
		Responses: fileInfos,
	}
	transportMessage, err := n.config.MessageRegistry.MarshalMessage(message)
	if err != nil {
		return err
	}

	// The Destination field of the packet’s header must be set to the searchMessage.Origin.
	header := transport.NewHeader(n.address, n.address, searchRequestMsg.Origin, 0)
	// The reply must be directly sent to the packet's source
	err = n.config.Socket.Send(pkt.Header.Source, transport.Packet{
		Header: &header,
		Msg:    &transportMessage,
	}, 0)
	Logger.Info().Msgf("[%v] Send search reply to %v, the final destination is: %v", n.address, pkt.Header.Source, searchRequestMsg.Origin)
	return err
}

// ExecSearchReplyMessage implements the handler for types.SearchReplyMessage
func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	searchReplyMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("Wrong type: %T", msg)
	}
	Logger.Info().Msgf("[%v] ExecSearchReplyMessage: receive search reply message from %v, replayed by %v", n.address, pkt.Header.Source, pkt.Header.RelayedBy)
	fileInfos := searchReplyMsg.Responses
	if fileInfos == nil {
		return nil
	}

	var names []string
	var fullFileNames []string
	for _, fileInfo := range fileInfos {
		n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
		for _, chunkInfo := range fileInfo.Chunks {
			if chunkInfo != nil{
				n.UpdateCatalog(string(chunkInfo), pkt.Header.Source)
			}
		}
		names = append(names, fileInfo.Name)

		// TODO: Check update naming store
		err := n.Tag(fileInfo.Name, fileInfo.Metahash)
		if err != nil {
			Logger.Error().Msg(err.Error())
		}

		if n.haveAllChunksInFileInfo(fileInfo) {
			Logger.Info().Msgf("[%v] Receives a qualified search first reply, filename=%v, from=%v", n.address, fileInfo.Name, pkt.Header.Source)
			fullFileNames = append(fullFileNames, fileInfo.Name)
		}
	}

	n.ackSearchAllRequest.RLock()
	select {
	case n.ackSearchAllRequest.values[searchReplyMsg.RequestID] <- names:
		Logger.Info().Msgf("[%v] Notifies the peer of search all reply, names=%v, from=%v", n.address, names, pkt.Header.Source)
	default:
		Logger.Info().Msgf("[%v] Search all notification channel is full", n.address)
	}
	n.ackSearchAllRequest.RUnlock()

	if len(fullFileNames) > 0 {
		n.ackSearchFirstRequest.RLock()
		select {
		case n.ackSearchFirstRequest.values[searchReplyMsg.RequestID] <- fullFileNames[0]:
			Logger.Info().Msgf("[%v] Notifies the peer of search first reply, filename=%v, from=%v", n.address, fullFileNames[0], pkt.Header.Source)
		default:
			Logger.Info().Msgf("[%v] Search first notification channel is full", n.address)
		}
		n.ackSearchFirstRequest.RUnlock()
	}
	return nil
}


func (n *node) haveAllChunksLocally(metaHash string) bool {
	metaFileBytes := n.config.Storage.GetDataBlobStore().Get(metaHash)
	if len(metaFileBytes) <= 0 {
		return false
	}
	chunkHashes := strings.Split(string(metaFileBytes), peer.MetafileSep)
	for _, chunkHash := range chunkHashes {
		if len(n.config.Storage.GetDataBlobStore().Get(chunkHash)) <= 0 {
			return false
		}
	}
	return true
}

func (n *node) haveAllChunksInFileInfo(fileInfo types.FileInfo) bool {
	for _, chunk := range fileInfo.Chunks {
		if len(chunk) <= 0 {
			return false
		}
	}
	return true
}


func (n *node) searchLocalStorage(pattern regexp.Regexp) []types.FileInfo {
	var fileInfos []types.FileInfo

	blobStore := n.config.Storage.GetDataBlobStore()
	namingStore := n.config.Storage.GetNamingStore()

	namingStore.ForEach(
		func(key string, val []byte) bool {
			if pattern.MatchString(key) {
				metaHash := string(val)
				metaFileBytes := blobStore.Get(metaHash)
				if metaFileBytes != nil {
					chunkHashes := strings.Split(string(metaFileBytes), peer.MetafileSep)
					fileInfo := types.FileInfo{
						Name: key,
						Metahash: metaHash,
						Chunks: make([][]byte, len(chunkHashes)),
					}
					for i, chunkHash := range chunkHashes {
						chunkBytes := blobStore.Get(chunkHash)
						if chunkBytes == nil {
							continue
						}
						fileInfo.Chunks[i] = []byte(chunkHash)
					}
					fileInfos = append(fileInfos, fileInfo)
				}
			}
			return true
		})

	return fileInfos
}


func (n *node) assignBudgets(total uint, numOfNeighbours uint) []uint {
	var budgets []uint
	if total <= numOfNeighbours {
		for i := uint(0); i < total; i++ {
			budgets = append(budgets, uint(1))
		}
		return budgets
	}

	quotient := total / numOfNeighbours
	remainder := total % numOfNeighbours
	for i := uint(0); i < numOfNeighbours; i++ {
		budgets = append(budgets, quotient)
		if i < remainder {
			budgets[i]++
		}
	}
	return budgets
}

func (n *node) downloadFromPeers(hash string) ([]byte, error) {
	n.catalog.RLock()
	peers := n.catalog.values[hash]
	n.catalog.RUnlock()

	if peers == nil || len(peers) <= 0 {
		return nil, xerrors.Errorf("[%v] Cannot find the file of hash=%v in the catalog", n.address, hash)
	}

	n.catalog.RLock()
	randomPeer := n.catalog.valuesArray[hash][rand.Intn(len(peers))]
	n.catalog.RUnlock()

	backoff := n.config.BackoffDataRequest

	requestId := xid.New().String()
	requestMsg := types.DataRequestMessage{
		RequestID: requestId,
		Key:       hash,
	}

	err := n.uniCastMessage(randomPeer, requestMsg)
	if err != nil {
		return nil, err
	}

	Logger.Info().Msgf("[%v] Asking peer=%v for file=%v, request id=%v", n.address, randomPeer, hash, requestId)

	dataChan := make(chan []byte, 1)
	n.ackDataRequest.Lock()
	n.ackDataRequest.values[requestId] = dataChan
	n.ackDataRequest.Unlock()

	var file []byte

	// TODO: attempt < backoff.Retry or attempt <= backoff.Retry
	loop:
	for attempt := uint(0); attempt < backoff.Retry; attempt++ {
		duration := time.Duration(backoff.Initial.Milliseconds() * n.pow(backoff.Factor, attempt)) * time.Millisecond
		select {
		case <- time.After(duration):
			Logger.Info().Msgf("[%v] Retry to send file request, id=%v, hash=%v", n.address, requestId, hash)
			err = n.uniCastMessage(randomPeer, requestMsg)
			if err != nil {
				return nil, err
			}
		case buf := <- dataChan:
			if buf == nil {
				return nil, xerrors.Errorf("[%v] Receive nil file of hash=%v from peer=%v", n.address, hash, randomPeer)
			}
			file = append(file, buf...)
			break loop
		}
	}

	if file == nil {
		neighbours, _ := n.getNeighbours()
		return nil, xerrors.Errorf(
			"[%v] Fail to receive file of hash=%v from %v, the routing table is: %v, neighbours are: %v",
			n.address, hash, randomPeer, n.GetRoutingTable(), neighbours)
	}

	Logger.Info().Msgf("[%v] Received from peer=%v for file=%v, request id=%v", n.address, randomPeer, hash, requestId)
	return file, nil
}

func (n *node) uniCastMessage(dest string, message types.Message) error {
	transportMessage, err := n.config.MessageRegistry.MarshalMessage(message)
	if err != nil {
		return err
	}
	return n.Unicast(dest, transportMessage)
	//err = n.Unicast(dest, transportMessage)
	//if err != nil {
	//	Logger.Error().Msg(err.Error())
	//}
	//n.sendMessageUnchecked(dest, message)
	//return nil
}

func (n *node) pow(x uint, y uint) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
