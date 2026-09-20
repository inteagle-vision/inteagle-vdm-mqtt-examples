// Package sdk 封装 VDM MQTT Topic、双格式编解码、连接与 RPC 关联。
package sdk

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	vdmmqttv1 "github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/generated"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const SchemaVersion uint32 = 1

const (
	evidencePackageHeaderLength = 76
	evidenceChunkBytes          = 128 * 1024
	maxEvidencePackageBytes     = 32 * 1024 * 1024
)

type PayloadFormat string

const (
	JSON     PayloadFormat = "json"
	Protobuf PayloadFormat = "protobuf"
)

func ParsePayloadFormat(value string) (PayloadFormat, error) {
	format := PayloadFormat(strings.ToLower(strings.TrimSpace(value)))
	if format != JSON && format != Protobuf {
		return "", fmt.Errorf("不支持的 Payload 格式: %s", value)
	}
	return format, nil
}

type Topics struct{ Base string }

func TopicsForDevice(deviceID string) (Topics, error) {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" || strings.ContainsAny(deviceID, "/+#") {
		return Topics{}, errors.New("deviceID 不能为空且不能包含 MQTT Topic 分隔符或通配符")
	}
	return Topics{Base: "vdm/" + deviceID}, nil
}

func NewTopics(base string) (Topics, error) {
	base = strings.Trim(base, "/")
	if base == "" || strings.ContainsAny(base, "+#") {
		return Topics{}, errors.New("base topic 不能为空且不能包含 MQTT 通配符")
	}
	return Topics{Base: base}, nil
}

func (t Topics) Topic(suffix string) string { return t.Base + "/" + strings.Trim(suffix, "/") }
func (t Topics) Wildcard() string           { return t.Topic("#") }
func (t Topics) RPCRequest() string         { return t.Topic("rpc/req") }
func (t Topics) RPCResponse() string        { return t.Topic("rpc/resp") }

func (t Topics) Suffix(topic string) (string, error) {
	prefix := t.Base + "/"
	if !strings.HasPrefix(topic, prefix) {
		return "", fmt.Errorf("Topic 不属于当前设备: %s", topic)
	}
	return strings.TrimPrefix(topic, prefix), nil
}

type ImageFrame struct {
	Version      uint8
	HeaderLength uint8
	SensorID     uint8
	ImageType    uint8
	TimestampS   uint32
	JPEG         []byte
}

type EvidencePackageChunk struct {
	MessageType, HeaderLength, PackageFormat, EvidenceKind uint8
	EventID, PackageLength                                 uint64
	PackageSHA256                                          [32]byte
	ChunkIndex, ChunkCount                                 uint32
	ChunkOffset                                            uint64
	Chunk                                                  []byte
}

type DecodedPayload struct {
	Topic  string
	Suffix string
	Raw    []byte
	Value  any
}

// AsMap 返回完整的 JSON 兼容字段视图；Value 仍保留 Protobuf 强类型消息。
// 图片只返回 Header 字段和 JPEG 长度，避免为了日志再次复制图片内容。
func (d *DecodedPayload) AsMap() (map[string]any, error) {
	switch value := d.Value.(type) {
	case map[string]any:
		return value, nil
	case *ImageFrame:
		return map[string]any{
			"version": value.Version, "headerLength": value.HeaderLength,
			"sensorId": value.SensorID, "imageType": value.ImageType,
			"timestampS": value.TimestampS, "jpegBytes": len(value.JPEG),
		}, nil
	case *EvidencePackageChunk:
		return map[string]any{
			"messageType": value.MessageType, "headerLength": value.HeaderLength,
			"packageFormat": value.PackageFormat, "evidenceKind": value.EvidenceKind,
			"eventId": fmt.Sprintf("%d", value.EventID), "packageLength": value.PackageLength,
			"packageSha256": hex.EncodeToString(value.PackageSHA256[:]),
			"chunkIndex":    value.ChunkIndex, "chunkCount": value.ChunkCount,
			"chunkOffset": value.ChunkOffset, "chunkBytes": len(value.Chunk),
		}, nil
	case proto.Message:
		payload, err := (protojson.MarshalOptions{UseProtoNames: false}).Marshal(value)
		if err != nil {
			return nil, err
		}
		var result map[string]any
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, err
		}
		return result, nil
	default:
		return nil, fmt.Errorf("未知解码结果类型: %T", d.Value)
	}
}

type Codec struct{ Format PayloadFormat }

func NewCodec(value string) (Codec, error) {
	format, err := ParsePayloadFormat(value)
	return Codec{Format: format}, err
}

func decodeImage(payload []byte) (any, error) {
	if len(payload) > 0 && payload[0] == 2 {
		return decodeEvidencePackageChunk(payload)
	}
	if len(payload) < 10 {
		return nil, errors.New("图片 Payload 小于 VDM Header 与 JPEG 最小长度")
	}
	headerLength := int(payload[1])
	if headerLength < 8 || headerLength > len(payload) {
		return nil, fmt.Errorf("非法图片 Header 长度: %d", headerLength)
	}
	jpeg := payload[headerLength:]
	if len(jpeg) < 2 || jpeg[0] != 0xff || jpeg[1] != 0xd8 {
		return nil, errors.New("图片数据不是 JPEG")
	}
	return &ImageFrame{
		Version:      payload[0],
		HeaderLength: payload[1],
		SensorID:     payload[2],
		ImageType:    payload[3],
		TimestampS:   binary.BigEndian.Uint32(payload[4:8]),
		JPEG:         append([]byte(nil), jpeg...),
	}, nil
}

func decodeEvidencePackageChunk(payload []byte) (*EvidencePackageChunk, error) {
	if len(payload) < evidencePackageHeaderLength {
		return nil, errors.New("告警抓拍图像包 Payload 小于 76 字节固定 Header")
	}
	if payload[0] != 2 || payload[1] != evidencePackageHeaderLength || payload[2] != 1 || payload[3] != 1 {
		return nil, errors.New("当前只支持 USTAR SNAPSHOT 抓拍图像包")
	}
	eventID := binary.BigEndian.Uint64(payload[4:12])
	packageLength := binary.BigEndian.Uint64(payload[12:20])
	chunkIndex := binary.BigEndian.Uint32(payload[52:56])
	chunkCount := binary.BigEndian.Uint32(payload[56:60])
	chunkOffset := binary.BigEndian.Uint64(payload[60:68])
	chunkLength := binary.BigEndian.Uint32(payload[68:72])
	flags := binary.BigEndian.Uint32(payload[72:76])
	if eventID == 0 || packageLength == 0 || packageLength > maxEvidencePackageBytes {
		return nil, errors.New("告警抓拍图像包身份或长度非法")
	}
	expectedCount := uint32((packageLength + evidenceChunkBytes - 1) / evidenceChunkBytes)
	expectedOffset := uint64(chunkIndex) * evidenceChunkBytes
	if expectedOffset >= packageLength {
		return nil, errors.New("告警抓拍图像包 chunkOffset 超出包长度")
	}
	expectedLength := min(uint64(evidenceChunkBytes), packageLength-expectedOffset)
	if chunkCount != expectedCount || chunkIndex >= chunkCount || chunkOffset != expectedOffset ||
		uint64(chunkLength) != expectedLength || len(payload) != evidencePackageHeaderLength+int(chunkLength) || flags != 0 {
		return nil, errors.New("告警抓拍图像包分块范围、长度或 flags 非法")
	}
	chunk := append([]byte(nil), payload[evidencePackageHeaderLength:]...)
	if chunkIndex == 0 && (len(chunk) < 262 || string(chunk[257:262]) != "ustar") {
		return nil, errors.New("告警抓拍图像包首块缺少 USTAR 标识")
	}
	result := &EvidencePackageChunk{
		MessageType: 2, HeaderLength: evidencePackageHeaderLength, PackageFormat: payload[2], EvidenceKind: payload[3],
		EventID: eventID, PackageLength: packageLength,
		ChunkIndex: chunkIndex, ChunkCount: chunkCount, ChunkOffset: chunkOffset, Chunk: chunk,
	}
	copy(result.PackageSHA256[:], payload[20:52])
	return result, nil
}

func protobufMessage(suffix string) (proto.Message, error) {
	switch suffix {
	case "telemetry":
		return &vdmmqttv1.Telemetry{}, nil
	case "attributes":
		return &vdmmqttv1.Attributes{}, nil
	case "event":
		return &vdmmqttv1.Event{}, nil
	case "3A":
		return &vdmmqttv1.Alarm{}, nil
	case "rpc/req":
		return &vdmmqttv1.RpcRequest{}, nil
	case "rpc/resp":
		return &vdmmqttv1.RpcResponse{}, nil
	default:
		return nil, fmt.Errorf("未支持的 Topic: %s", suffix)
	}
}

func (c Codec) Decode(topic string, topics Topics, payload []byte) (*DecodedPayload, error) {
	suffix, err := topics.Suffix(topic)
	if err != nil {
		return nil, err
	}
	raw := append([]byte(nil), payload...)
	var value any
	if suffix == "image" {
		value, err = decodeImage(raw)
	} else if c.Format == JSON {
		var object map[string]any
		err = json.Unmarshal(raw, &object)
		if err == nil && object == nil {
			err = errors.New("JSON 根节点必须是对象")
		}
		value = object
	} else {
		var message proto.Message
		message, err = protobufMessage(suffix)
		if err == nil {
			err = proto.Unmarshal(raw, message)
		}
		if err == nil {
			versioned, ok := message.(interface{ GetSchemaVersion() uint32 })
			if !ok {
				err = errors.New("Protobuf 消息未声明 schema_version")
			} else if version := versioned.GetSchemaVersion(); version != SchemaVersion {
				err = fmt.Errorf("不支持 schema_version=%d", version)
			}
		}
		value = message
	}
	if err != nil {
		return nil, err
	}
	return &DecodedPayload{Topic: topic, Suffix: suffix, Raw: raw, Value: value}, nil
}

var publicRPCFields = map[string]string{
	"getAttr": "get_attr", "setAttr": "set_attr", "reboot": "reboot", "syncTime": "sync_time",
	"initRefTargets": "init_ref_targets", "addTargets": "add_targets", "getTargets": "get_targets",
	"setTargets": "set_targets", "deleteTargets": "delete_targets", "startMeasurement": "start_measurement",
	"stopMeasurement": "stop_measurement", "setLightLevel": "set_light_level", "getLightLevel": "get_light_level",
	"snapshot": "snapshot", "ispCtl": "isp_ctl", "setMotorAngle": "set_motor_angle",
	"getMotorAngle": "get_motor_angle", "setMotorZero": "set_motor_zero", "enableMotor": "enable_motor",
	"disableMotor": "disable_motor", "getCruisePaths": "get_cruise_paths",
	"getEvidenceStatus": "get_evidence_status", "retryEvidence": "retry_evidence",
	"ackEvidencePackage": "ack_evidence_package",
	"getAlarmCaps":       "get_alarm_caps", "listAlarmRules": "list_alarm_rules",
	"applyAlarmRules": "apply_alarm_rules", "getAlarmState": "get_alarm_state",
	"listAlarmHistory": "list_alarm_history",
	"listAlarmEvents":  "list_alarm_events", "syncTelemetry": "sync_telemetry",
	"getSyncStatus": "get_sync_status", "cancelSync": "cancel_sync",
}

func (c Codec) EncodeRPC(method string, params any, reqID int32) ([]byte, string, error) {
	if reqID == 0 {
		return nil, "", errors.New("reqID 必须是非零 signed int32")
	}
	fieldName, ok := publicRPCFields[method]
	if !ok {
		return nil, "", fmt.Errorf("RPC 方法不属于公开 VDM API: %s", method)
	}
	if params == nil {
		params = map[string]any{}
	}
	if c.Format == JSON {
		payload, err := json.Marshal(map[string]any{"reqId": reqID, "method": method, "params": params})
		return payload, fieldName, err
	}
	paramBytes, err := json.Marshal(params)
	if err != nil {
		return nil, "", fmt.Errorf("编码 %s 参数失败: %w", method, err)
	}
	rpc := &vdmmqttv1.RpcRequest{SchemaVersion: SchemaVersion, ReqId: reqID}
	reflection := rpc.ProtoReflect()
	oneof := reflection.Descriptor().Oneofs().ByName("request")
	field := oneof.Fields().ByName(protoreflect.Name(fieldName))
	if field == nil {
		return nil, "", fmt.Errorf("Schema 缺少 RPC request field: %s", fieldName)
	}
	body := reflection.Mutable(field).Message().Interface()
	if err := protojson.Unmarshal(paramBytes, body); err != nil {
		return nil, "", fmt.Errorf("%s Protobuf 参数不合法: %w", method, err)
	}
	payload, err := proto.Marshal(rpc)
	return payload, fieldName, err
}

type Config struct {
	// DurableHandler stores raw business payloads before QoS1 PUBACK. Never call RPC here.
	DurableHandler func(topic string, payload []byte) error
	// PersistentSession requires a stable, exclusive ClientID.
	PersistentSession bool
	// Nil/empty preserves wildcard subscription; entries are public topic suffixes.
	SubscriptionSuffixes []string
	Host                 string
	Port                 int
	Topics               Topics
	PayloadFormat        PayloadFormat
	Username             string
	Password             string
	ClientID             string
	QoS                  byte
	ConnectTimeout       time.Duration
}

type MessageHandler func(*DecodedPayload)
type ErrorHandler func(error)

type pendingRPC struct {
	expected string
	result   chan rpcResult
}
type rpcResult struct {
	payload *DecodedPayload
	err     error
}

type Client struct {
	config     Config
	codec      Codec
	client     mqtt.Client
	handler    MessageHandler
	onError    ErrorHandler
	ready      chan struct{}
	readyError chan error
	readyOnce  sync.Once
	pending    map[int32]*pendingRPC
	mu         sync.Mutex
	nextReqID  atomic.Int32
	subscribed atomic.Bool
}

func subscriptionTopics(config Config) (map[string]byte, error) {
	topics := make(map[string]byte)
	if len(config.SubscriptionSuffixes) == 0 {
		topics[config.Topics.Wildcard()] = config.QoS
		return topics, nil
	}
	for _, suffix := range config.SubscriptionSuffixes {
		switch suffix {
		case "telemetry", "attributes", "3A", "event", "image", "rpc/req", "rpc/resp":
			topics[config.Topics.Topic(suffix)] = config.QoS
		default:
			return nil, fmt.Errorf("unsupported subscription suffix: %q", suffix)
		}
	}
	return topics, nil
}

func validateSubscriptionResults(requested map[string]byte, granted map[string]byte) error {
	for topic := range requested {
		qos, exists := granted[topic]
		if !exists || qos > 2 {
			return fmt.Errorf("MQTT subscription rejected or missing from SUBACK: %s", topic)
		}
	}
	return nil
}

func (c *Client) failReady(err error) {
	c.report(err)
	select {
	case c.readyError <- err:
	default:
	}
}

func NewClient(config Config, handler MessageHandler, onError ErrorHandler) (*Client, error) {
	if config.Host == "" || config.Port < 1 || config.Port > 65535 {
		return nil, errors.New("MQTT host/port 无效")
	}
	if config.QoS > 2 {
		return nil, errors.New("MQTT QoS 必须是 0、1 或 2")
	}
	normalizedTopics, err := NewTopics(config.Topics.Base)
	if err != nil {
		return nil, err
	}
	config.Topics = normalizedTopics
	subscriptions, err := subscriptionTopics(config)
	if err != nil {
		return nil, err
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.ClientID == "" {
		config.ClientID = fmt.Sprintf("vdm-sdk-go-%d", time.Now().UnixNano())
	}
	codec := Codec{Format: config.PayloadFormat}
	if _, err := ParsePayloadFormat(string(config.PayloadFormat)); err != nil {
		return nil, err
	}
	result := &Client{
		config: config, codec: codec, handler: handler, onError: onError,
		ready: make(chan struct{}), readyError: make(chan error, 1), pending: make(map[int32]*pendingRPC),
	}
	// Responses share one device topic across clients. Avoid every fresh
	// receiver using request ID 1 for its first concurrent call.
	var requestSeed [4]byte
	if _, err := rand.Read(requestSeed[:]); err != nil {
		return nil, fmt.Errorf("initialize request IDs: %w", err)
	}
	result.nextReqID.Store(int32(binary.BigEndian.Uint32(requestSeed[:]) & 0x7fffffff))
	options := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", config.Host, config.Port)).
		SetClientID(config.ClientID).
		SetCleanSession(!config.PersistentSession).
		SetAutoAckDisabled(config.DurableHandler != nil).
		SetDefaultPublishHandler(result.onMessage).
		SetAutoReconnect(true).
		SetConnectTimeout(2 * time.Second).
		SetOnConnectHandler(func(client mqtt.Client) {
			token := client.SubscribeMultiple(subscriptions, result.onMessage)
			if !token.WaitTimeout(5 * time.Second) {
				result.failReady(errors.New("订阅 VDM Topic ACK 超时"))
				return
			}
			if token.Error() != nil {
				result.failReady(fmt.Errorf("订阅 VDM Topic 失败: %w", token.Error()))
				return
			}
			subscriptionToken, ok := token.(*mqtt.SubscribeToken)
			if !ok {
				result.failReady(errors.New("MQTT subscribe token has no SUBACK results"))
				return
			}
			if err := validateSubscriptionResults(subscriptions, subscriptionToken.Result()); err != nil {
				result.failReady(err)
				return
			}
			result.subscribed.Store(true)
			result.readyOnce.Do(func() { close(result.ready) })
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			result.subscribed.Store(false)
			result.failPending(&TransportError{Err: fmt.Errorf("MQTT 连接断开: %w", err)})
			result.report(err)
		})
	if config.Username != "" {
		options.SetUsername(config.Username).SetPassword(config.Password)
	}
	result.client = mqtt.NewClient(options)
	return result, nil
}

func (c *Client) Start(ctx context.Context) error {
	deadline := time.Now().Add(c.config.ConnectTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		token := c.client.Connect()
		if token.WaitTimeout(3*time.Second) && token.Error() == nil {
			break
		}
		lastErr = token.Error()
		time.Sleep(200 * time.Millisecond)
	}
	if !c.client.IsConnected() {
		return fmt.Errorf("连接 MQTT Broker 超时: %w", lastErr)
	}
	select {
	case <-c.ready:
		return nil
	case err := <-c.readyError:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.config.ConnectTimeout):
		return errors.New("订阅 VDM Topic 超时")
	}
}

func (c *Client) Connected() bool {
	return c.client != nil && c.client.IsConnectionOpen() && c.subscribed.Load()
}

func (c *Client) Close() {
	c.subscribed.Store(false)
	c.failPending(errors.New("VDM MQTT client stopped"))
	c.client.Disconnect(1000)
}

func (c *Client) PublishRaw(topic string, payload any, retained bool) error {
	token := c.client.Publish(topic, c.config.QoS, retained, payload)
	if !token.WaitTimeout(5 * time.Second) {
		return &TransportError{Err: fmt.Errorf("MQTT publish ACK 超时: %s: %w", topic, context.DeadlineExceeded)}
	}
	if err := token.Error(); err != nil {
		return &TransportError{Err: err}
	}
	return nil
}

// TransportError identifies a failed MQTT transmission or a lost connection.
// Callers may retry idempotent operations after reconnecting.
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string { return e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

type RPCError struct {
	ReqID   int32
	Code    int32
	Message string
}

func rpcCodeMessage(code int32) string {
	messages := map[int32]string{
		0: "success", 1: "RPC request failed", 2: "invalid RPC request",
		3: "unsupported RPC method", 4: "RPC request rate limited",
		5: "RPC request timed out", 6: "resource state changed",
		100: "resource not found", 102: "reference target initialization failed",
		104: "target lost", 200: "measurement not started",
		201: "measurement already running", 300: "motor unavailable",
		302: "motor moving", 303: "motor limit reached",
		310: "vertical motor unavailable", 400: "cruise unavailable",
		403: "cruise already running",
	}
	if message, ok := messages[code]; ok {
		return message
	}
	return fmt.Sprintf("RPC request failed (code=%d)", code)
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("RPC req_id=%d code=%d: %s", e.ReqID, e.Code, e.Message)
}

func (c *Client) Call(ctx context.Context, method string, params any, reqID int32, allowError bool) (*DecodedPayload, error) {
	if reqID == 0 {
		for reqID == 0 {
			reqID = c.nextReqID.Add(1)
		}
	}
	payload, expected, err := c.codec.EncodeRPC(method, params, reqID)
	if err != nil {
		return nil, err
	}
	pending := &pendingRPC{expected: expected, result: make(chan rpcResult, 1)}
	c.mu.Lock()
	if _, exists := c.pending[reqID]; exists {
		c.mu.Unlock()
		return nil, fmt.Errorf("reqID 已在当前连接等待响应: %d", reqID)
	}
	c.pending[reqID] = pending
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pending[reqID] == pending {
			delete(c.pending, reqID)
		}
		c.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token := c.client.Publish(c.config.Topics.RPCRequest(), c.config.QoS, false, payload)
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return nil, &TransportError{Err: err}
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-pending.result:
		if result.err != nil {
			return nil, result.err
		}
		responseReqID, code, message, responseField, err := responseInfo(result.payload.Value)
		if err != nil {
			return nil, err
		}
		if responseReqID != reqID {
			return nil, fmt.Errorf("RPC reqID 不匹配: request=%d response=%d", reqID, responseReqID)
		}
		if code != 0 && !allowError {
			return nil, &RPCError{ReqID: reqID, Code: code, Message: message}
		}
		if code == 0 && c.config.PayloadFormat == Protobuf && responseField != expected {
			return nil, fmt.Errorf("RPC response oneof 不匹配: expected=%s actual=%s", expected, responseField)
		}
		return result.payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) snapshotKind() string {
	if c.config.PayloadFormat == Protobuf {
		return "EVIDENCE_KIND_SNAPSHOT"
	}
	return "SNAPSHOT"
}

func (c *Client) GetEvidenceStatus(ctx context.Context, eventID uint64) (*DecodedPayload, error) {
	if eventID == 0 {
		return nil, errors.New("eventID 必须是非零整数")
	}
	return c.Call(ctx, "getEvidenceStatus", map[string]any{
		"eventId": strconv.FormatUint(eventID, 10), "kind": c.snapshotKind(),
	}, 0, false)
}

func (c *Client) RetryEvidence(ctx context.Context, eventID uint64) (*DecodedPayload, error) {
	if eventID == 0 {
		return nil, errors.New("eventID 必须是非零整数")
	}
	return c.Call(ctx, "retryEvidence", map[string]any{
		"eventId": strconv.FormatUint(eventID, 10), "kind": c.snapshotKind(),
	}, 0, false)
}

func (c *Client) AckEvidencePackage(ctx context.Context, eventID uint64, packageSHA256 string) (*DecodedPayload, error) {
	if eventID == 0 {
		return nil, errors.New("eventID 必须是非零整数")
	}
	if len(packageSHA256) != 64 || strings.ToLower(packageSHA256) != packageSHA256 {
		return nil, errors.New("packageSHA256 必须是 64 个小写十六进制字符")
	}
	if _, err := hex.DecodeString(packageSHA256); err != nil {
		return nil, errors.New("packageSHA256 必须是 64 个小写十六进制字符")
	}
	return c.Call(ctx, "ackEvidencePackage", map[string]any{
		"eventId": strconv.FormatUint(eventID, 10), "kind": c.snapshotKind(),
		"packageSha256": packageSHA256,
	}, 0, false)
}

func responseInfo(value any) (int32, int32, string, string, error) {
	switch response := value.(type) {
	case *vdmmqttv1.RpcResponse:
		reflection := response.ProtoReflect()
		selected := reflection.WhichOneof(reflection.Descriptor().Oneofs().ByName("response"))
		field := ""
		if selected != nil {
			field = string(selected.Name())
		}
		return response.GetReqId(), response.GetCode(), rpcCodeMessage(response.GetCode()), field, nil
	case map[string]any:
		rawID, ok := response["reqId"]
		if !ok {
			rawID = response["req_id"]
		}
		reqFloat, ok := rawID.(float64)
		if !ok {
			return 0, 0, "", "", errors.New("JSON RPC 响应缺少 reqId")
		}
		code := int32(1)
		switch value := response["code"].(type) {
		case float64:
			code = int32(value)
		case string:
			if value == "0" {
				code = 0
			}
		}
		return int32(reqFloat), code, rpcCodeMessage(code), "", nil
	default:
		return 0, 0, "", "", errors.New("RPC 响应类型错误")
	}
}

func (c *Client) onMessage(client mqtt.Client, message mqtt.Message) {
	if c.config.DurableHandler != nil && message.Topic() != c.config.Topics.RPCResponse() {
		if err := c.config.DurableHandler(message.Topic(), message.Payload()); err != nil {
			c.report(err)
			c.subscribed.Store(false)
			// Disconnect outside Paho's ordered callback; supervisor reconnects for redelivery.
			if client != nil {
				go client.Disconnect(0)
			}
			return
		}
		message.Ack()
		return
	}
	if c.config.DurableHandler != nil {
		defer message.Ack()
	}
	decoded, err := c.codec.Decode(message.Topic(), c.config.Topics, message.Payload())
	if err != nil {
		c.report(err)
		return
	}
	if decoded.Suffix == "rpc/resp" {
		reqID, _, _, _, infoErr := responseInfo(decoded.Value)
		if infoErr == nil {
			c.mu.Lock()
			pending := c.pending[reqID]
			c.mu.Unlock()
			if pending != nil {
				select {
				case pending.result <- rpcResult{payload: decoded}:
				default:
				}
			}
		}
	}
	if c.handler != nil {
		c.handler(decoded)
	}
}

func (c *Client) failPending(err error) {
	c.mu.Lock()
	pending := make([]*pendingRPC, 0, len(c.pending))
	for _, call := range c.pending {
		pending = append(pending, call)
	}
	c.pending = make(map[int32]*pendingRPC)
	c.mu.Unlock()
	for _, call := range pending {
		select {
		case call.result <- rpcResult{err: err}:
		default:
		}
	}
}

func (c *Client) report(err error) {
	if err != nil && c.onError != nil {
		c.onError(err)
	}
}
