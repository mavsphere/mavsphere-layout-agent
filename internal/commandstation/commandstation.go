// Package commandstation defines the interface that all command station backends
// must satisfy. This allows main.go to select DCC-EX or JMRI at startup based
// on config, without the rest of the codebase caring which is in use.
//
// Method semantics match the existing dccex.Client methods so the DCC-EX client
// can satisfy the interface with a thin adapter (see dccex.go in this package).
package commandstation

// CommandStation is the hardware-control interface for a layout's motive power system.
// Implementations: dccex.Client (via adapter), jmri.ThrottleClient.
type CommandStation interface {
	// SetLocoSpeed sets speed and direction for a single DCC address.
	// speed: 0–126 (DCC 128-step scale), direction: 1=forward 0=reverse.
	SetLocoSpeed(regID, dccAddr, speed, direction int) error

	// StopLoco brings a single address to an immediate stop (speed 0).
	StopLoco(dccAddr int) error

	// EmergencyStop cuts all track power / stops all locos immediately.
	EmergencyStop() error

	// SetTurnout sets a turnout/point by DCC accessory address.
	// thrown=true → REVERSE/THROWN, thrown=false → NORMAL/CLOSED.
	SetTurnout(turnoutID int, thrown bool) error

	// SetFunction sets a decoder function F0–F28.
	// state: 1=on, 0=off.
	SetFunction(dccAddr, fn, state int) error

	// IsConnected reports whether the command station connection is live.
	IsConnected() bool

	// Connect (re-)establishes the connection. Safe to call when already connected.
	Connect() error

	// Close shuts down the connection cleanly.
	Close() error
}
