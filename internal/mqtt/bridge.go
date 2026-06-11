package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	mq "github.com/eclipse/paho.mqtt.golang"
)

type SensorEvent struct {
	NodeID   string
	SensorID string
	State    string
	Active   bool
	Payload  map[string]any
}

type GenericEvent struct {
	NodeID  string
	Type    string
	SubType string
	Payload map[string]any
}

type Bridge struct {
	client      mq.Client
	topicPrefix string
	logger      *log.Logger

	onSensor  func(SensorEvent)
	onGeneric func(GenericEvent)
}

func NewBridge(
	brokerURL string,
	username string,
	password string,
	topicPrefix string,
	clientID string,
	onSensor func(SensorEvent),
	onGeneric func(GenericEvent),
	logger *log.Logger,
) *Bridge {
	opts := mq.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetResumeSubs(true).
		SetCleanSession(false)

	if username != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	b := &Bridge{
		topicPrefix: topicPrefix,
		logger:      logger,
		onSensor:    onSensor,
		onGeneric:   onGeneric,
	}

	opts.SetConnectionLostHandler(func(_ mq.Client, err error) {
		b.logger.Printf("[mqtt] connection lost: %v — auto-reconnect active", err)
	})

	opts.SetDefaultPublishHandler(func(_ mq.Client, msg mq.Message) {
		b.handleMessage(msg.Topic(), msg.Payload())
	})

	opts.SetOnConnectHandler(b.subscribeTopics)

	b.client = mq.NewClient(opts)
	return b
}

func (b *Bridge) subscribeTopics(client mq.Client) {
	topics := map[string]byte{
		fmt.Sprintf("%s/node/+/status", b.topicPrefix):         0,
		fmt.Sprintf("%s/node/+/heartbeat", b.topicPrefix):      0,
		fmt.Sprintf("%s/node/+/sensor/+/state", b.topicPrefix): 0,
		fmt.Sprintf("%s/node/+/rfid/+/tag", b.topicPrefix):     0,
		fmt.Sprintf("%s/node/+/reply/+", b.topicPrefix):        0,
	}
	subToken := client.SubscribeMultiple(topics, nil)
	if !subToken.WaitTimeout(10 * time.Second) {
		b.logger.Printf("[mqtt] subscribe timeout on connect")
		return
	}
	if subToken.Error() != nil {
		b.logger.Printf("[mqtt] subscribe error: %v", subToken.Error())
		return
	}
	b.logger.Printf("[mqtt] subscribed to %d topic patterns", len(topics))
}

func (b *Bridge) Connect() error {
	token := b.client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("mqtt connect timeout after 10s")
	}
	return token.Error()
}

func (b *Bridge) Disconnect() {
	if b.client != nil && b.client.IsConnected() {
		b.client.Disconnect(250)
	}
}

func (b *Bridge) IsConnected() bool {
	return b.client != nil && b.client.IsConnected()
}

func (b *Bridge) PublishConfig(nodeID string, cfg any) error {
	topic := fmt.Sprintf("%s/node/%s/cmd/config/set", b.topicPrefix, nodeID)
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	token := b.client.Publish(topic, 0, false, data)
	token.Wait()
	return token.Error()
}

func (b *Bridge) PublishPing(nodeID string) {
	topic := fmt.Sprintf("%s/node/%s/cmd/ping", b.topicPrefix, nodeID)
	b.client.Publish(topic, 0, false, "{}")
}

func (b *Bridge) PublishReboot(nodeID string) {
	topic := fmt.Sprintf("%s/node/%s/cmd/reboot", b.topicPrefix, nodeID)
	b.client.Publish(topic, 0, false, "{}")
}

// PublishStateSync requests a node to republish its current retained sensor
// truth for every configured digital sensor. Older firmware will ignore it;
// updated MAVsphere sensor-node firmware handles both cmd/state/sync and
// cmd/state-sync.
func (b *Bridge) PublishStateSync(nodeID string) {
	if b.client == nil || !b.client.IsConnected() || nodeID == "" {
		return
	}
	topic := fmt.Sprintf("%s/node/%s/cmd/state/sync", b.topicPrefix, nodeID)
	b.client.Publish(topic, 0, false, "{}")
}

// PublishSignalAspect publishes a signal aspect to MQTT for sensor nodes
// that drive physical signal LEDs.
// Topic: {prefix}/signal/{signalId}/aspect
func (b *Bridge) PublishSignalAspect(signalID, aspect string) {
	topic := fmt.Sprintf("%s/signal/%s/aspect", b.topicPrefix, signalID)
	data, _ := json.Marshal(map[string]any{
		"signalId":  signalID,
		"aspect":    aspect,
		"timestamp": time.Now().UnixMilli(),
		"source":    "backend",
	})
	b.client.Publish(topic, 0, true, data) // retained so nodes get latest on reconnect
}

func timeNowMs() int64 {
	return time.Now().UnixMilli()
}

func (b *Bridge) handleMessage(topic string, payload []byte) {
	parts := strings.Split(topic, "/")
	// {prefix}/node/{nodeId}/{kind}/...
	if len(parts) < 4 {
		return
	}

	nodeID := parts[2]
	kind := parts[3]

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		body = map[string]any{}
	}

	switch kind {
	case "sensor":
		if len(parts) < 6 {
			return
		}
		sensorID := parts[4]

		state, _ := body["state"].(string)
		active, _ := body["active"].(bool)

		if b.onSensor != nil {
			b.onSensor(SensorEvent{
				NodeID:   nodeID,
				SensorID: sensorID,
				State:    state,
				Active:   active,
				Payload:  body,
			})
		}

		if b.onGeneric != nil {
			b.onGeneric(GenericEvent{
				NodeID:  nodeID,
				Type:    "sensor",
				SubType: sensorID,
				Payload: body,
			})
		}

	case "status":
		if b.onGeneric != nil {
			b.onGeneric(GenericEvent{
				NodeID:  nodeID,
				Type:    "status",
				Payload: body,
			})
		}

	case "heartbeat":
		if b.onGeneric != nil {
			b.onGeneric(GenericEvent{
				NodeID:  nodeID,
				Type:    "heartbeat",
				Payload: body,
			})
		}

	case "rfid":
		readerID := ""
		if len(parts) >= 5 {
			readerID = parts[4]
		}
		if readerID != "" {
			body["readerId"] = readerID
		}
		if b.onGeneric != nil {
			b.onGeneric(GenericEvent{
				NodeID:  nodeID,
				Type:    "rfid",
				SubType: readerID,
				Payload: body,
			})
		}

	case "reply":
		replyType := ""
		if len(parts) >= 5 {
			replyType = parts[4]
		}
		if b.onGeneric != nil {
			b.onGeneric(GenericEvent{
				NodeID:  nodeID,
				Type:    "reply",
				SubType: replyType,
				Payload: body,
			})
		}
	}
}
