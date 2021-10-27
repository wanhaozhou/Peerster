package udp

import (
	"math"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000
const protocol = "udp"

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	addr, err := net.ResolveUDPAddr(protocol, address)
	if err != nil {
		return nil, err
	}

	udp, err := net.ListenUDP(protocol, addr)
	if err != nil {
		return nil, err
	}

	return &Socket{
		udp: udp,
		ins: packets{},
		outs: packets{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	udp *net.UDPConn
	ins packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.udp.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	rAddr, err := net.ResolveUDPAddr(protocol, dest)
	if err != nil {
		return err
	}

	bytes, err := pkt.Marshal()
	if err != nil {
		return err
	}

	if timeout == 0 {
		timeout = math.MaxInt64
	}
	
	if err = s.udp.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}

	_, err = s.udp.WriteToUDP(bytes, rAddr)
	switch err := err.(type) {
		case net.Error:
			return transport.TimeoutErr(timeout)
		case nil:
			s.outs.Lock()
			s.outs.data = append(s.outs.data, pkt.Copy())
			s.outs.Unlock()
			return nil
		default:
			return err
	}
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	buffer := make([]byte, bufSize)

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	if err := s.udp.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return transport.Packet{}, err
	}

	n, _, err := s.udp.ReadFromUDP(buffer)
	if os.IsTimeout(err) {
		return transport.Packet{}, transport.TimeoutErr(timeout)
	}
	if err != nil {
		return transport.Packet{}, err
	}

	pkt := transport.Packet{}
	err = pkt.Unmarshal(buffer[:n])
	if err != nil {
		return transport.Packet{}, err
	}
	s.ins.Lock()
	s.ins.data = append(s.ins.data, pkt.Copy())
	s.ins.Unlock()
	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.udp.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type packets struct {
	sync.RWMutex
	data []transport.Packet
}

func (p *packets) getAll() []transport.Packet {
	var data []transport.Packet
	p.RLock()
	defer p.RUnlock()
	for _, pkt := range p.data {
		data = append(data, pkt.Copy())
	}
	return data
}
