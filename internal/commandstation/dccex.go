package commandstation

import (
	"github.com/mavsphere/mavsphere-layout-agent/internal/dccex"
)

// DccExAdapter wraps a *dccex.Client to satisfy the CommandStation interface.
// All methods delegate directly — this is a zero-logic shim.
type DccExAdapter struct {
	c *dccex.Client
}

// NewDccExAdapter wraps an existing DCC-EX client.
func NewDccExAdapter(c *dccex.Client) *DccExAdapter {
	return &DccExAdapter{c: c}
}

func (a *DccExAdapter) SetLocoSpeed(regID, dccAddr, speed, direction int) error {
	return a.c.SetLocoSpeed(regID, dccAddr, speed, direction)
}

func (a *DccExAdapter) StopLoco(dccAddr int) error {
	return a.c.StopLoco(dccAddr)
}

func (a *DccExAdapter) EmergencyStop() error {
	return a.c.EmergencyStop()
}

func (a *DccExAdapter) SetTurnout(turnoutID int, thrown bool) error {
	return a.c.SetTurnout(turnoutID, thrown)
}

func (a *DccExAdapter) SetFunction(dccAddr, fn, state int) error {
	return a.c.SetFunction(dccAddr, fn, state)
}

func (a *DccExAdapter) IsConnected() bool {
	return a.c.IsConnected()
}

func (a *DccExAdapter) Connect() error {
	return a.c.Connect()
}

func (a *DccExAdapter) Close() error {
	return a.c.Close()
}
