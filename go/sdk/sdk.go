// Package sdk 封装 VDM MQTT Topic、双格式编解码、连接与 RPC 关联。
package sdk

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	evidenceImageHeaderLength = 112
	evidenceChunkBytes        = 128 * 1024
	maxEvidenceImageBytes     = 2 * 1024 * 1024
	maxEvidenceImages         = 64
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

type EvidenceImageChunk struct {
	MessageType, HeaderLength, CameraID, TriggerType uint8
	CapturedAtMS, EventID                            uint64
	ImageIndex, ImageCount                           uint16
	ActualOffsetMS                                   int32
	JPEGLength                                       uint32
	JPEGSHA256, ManifestSHA256                       [32]byte
	ChunkIndex, ChunkCount                           uint16
	ChunkOffset                                      uint32
	Chunk                                            []byte
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
	case *EvidenceImageChunk:
		return map[string]any{
			"messageType": value.MessageType, "headerLength": value.HeaderLength,
			"cameraId": value.CameraID, "triggerType": value.TriggerType,
			"capturedAtMs": value.CapturedAtMS, "eventId": fmt.Sprintf("%d", value.EventID),
			"imageIndex": value.ImageIndex, "imageCount": value.ImageCount,
			"actualOffsetMs": value.ActualOffsetMS, "jpegLength": value.JPEGLength,
			"jpegSha256":     hex.EncodeToString(value.JPEGSHA256[:]),
			"manifestSha256": hex.EncodeToString(value.ManifestSHA256[:]),
			"chunkIndex":     value.ChunkIndex, "chunkCount": value.ChunkCount,
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
		return decodeEvidenceImageChunk(payload)
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

func decodeEvidenceImageChunk(payload []byte) (*EvidenceImageChunk, error) {
	if len(payload) < evidenceImageHeaderLength {
		return nil, errors.New("告警证据图片 Payload 小于 112 字节固定 Header")
	}
	if payload[0] != 2 || payload[1] != evidenceImageHeaderLength || payload[3] != 1 {
		return nil, errors.New("告警证据图片 messageType/headerLen/triggerType 非法")
	}
	capturedAt := binary.BigEndian.Uint64(payload[4:12])
	eventID := binary.BigEndian.Uint64(payload[12:20])
	imageIndex := binary.BigEndian.Uint16(payload[20:22])
	imageCount := binary.BigEndian.Uint16(payload[22:24])
	jpegLength := binary.BigEndian.Uint32(payload[28:32])
	chunkIndex := binary.BigEndian.Uint16(payload[96:98])
	chunkCount := binary.BigEndian.Uint16(payload[98:100])
	chunkOffset := binary.BigEndian.Uint32(payload[100:104])
	chunkLength := binary.BigEndian.Uint32(payload[104:108])
	flags := binary.BigEndian.Uint32(payload[108:112])
	if capturedAt == 0 || eventID == 0 || imageCount == 0 || imageCount > maxEvidenceImages || imageIndex >= imageCount {
		return nil, errors.New("告警证据图片身份或 imageIndex/imageCount 非法")
	}
	if jpegLength == 0 || jpegLength > maxEvidenceImageBytes {
		return nil, errors.New("告警证据 JPEG 长度超出 1..2 MiB")
	}
	expectedCount := (jpegLength + evidenceChunkBytes - 1) / evidenceChunkBytes
	expectedOffset := uint32(chunkIndex) * evidenceChunkBytes
	if expectedOffset >= jpegLength {
		return nil, errors.New("告警证据图片 chunkOffset 超出 JPEG")
	}
	expectedLength := min(uint32(evidenceChunkBytes), jpegLength-expectedOffset)
	if uint32(chunkCount) != expectedCount || chunkIndex >= chunkCount || chunkOffset != expectedOffset ||
		chunkLength != expectedLength || len(payload) != evidenceImageHeaderLength+int(chunkLength) || flags != 0 {
		return nil, errors.New("告警证据图片分块范围、长度或 flags 非法")
	}
	chunk := append([]byte(nil), payload[evidenceImageHeaderLength:]...)
	if chunkIndex == 0 && (len(chunk) < 2 || chunk[0] != 0xff || chunk[1] != 0xd8) {
		return nil, errors.New("告警证据 JPEG 首块缺少 SOI")
	}
	result := &EvidenceImageChunk{
		MessageType: 2, HeaderLength: evidenceImageHeaderLength, CameraID: payload[2], TriggerType: payload[3],
		CapturedAtMS: capturedAt, EventID: eventID, ImageIndex: imageIndex, ImageCount: imageCount,
		ActualOffsetMS: int32(binary.BigEndian.Uint32(payload[24:28])), JPEGLength: jpegLength,
		ChunkIndex: chunkIndex, ChunkCount: chunkCount, ChunkOffset: chunkOffset, Chunk: chunk,
	}
	copy(result.JPEGSHA256[:], payload[32:64])
	copy(result.ManifestSHA256[:], payload[64:96])
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
	case "evidence":
		return &vdmmqttv1.AlarmEvidence{}, nil
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
		if err == nil && suffix != "evidence" {
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
	"ackEvidenceImages": "ack_evidence_images",
	"getAlarmCaps":      "get_alarm_caps", "listAlarmRules": "list_alarm_rules",
	"applyAlarmRules": "apply_alarm_rules", "getAlarmState": "get_alarm_state",
	"listAlarmHistory": "list_alarm_history",
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
	Host           string
	Port           int
	Topics         Topics
	PayloadFormat  PayloadFormat
	Username       string
	Password       string
	ClientID       string
	QoS            byte
	ConnectTimeout time.Duration
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
	config    Config
	codec     Codec
	client    mqtt.Client
	handler   MessageHandler
	onError   ErrorHandler
	ready     chan struct{}
	readyOnce sync.Once
	pending   map[int32]*pendingRPC
	mu        sync.Mutex
	nextReqID atomic.Int32
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
		ready: make(chan struct{}), pending: make(map[int32]*pendingRPC),
	}
	result.nextReqID.Store(0)
	options := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", config.Host, config.Port)).
		SetClientID(config.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(2 * time.Second).
		SetOnConnectHandler(func(client mqtt.Client) {
			token := client.Subscribe(config.Topics.Wildcard(), config.QoS, result.onMessage)
			if !token.WaitTimeout(5 * time.Second) {
				result.report(errors.New("订阅 VDM Topic ACK 超时"))
				return
			}
			if token.Error() != nil {
				result.report(fmt.Errorf("订阅 VDM Topic 失败: %w", token.Error()))
				return
			}
			result.readyOnce.Do(func() { close(result.ready) })
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			result.failPending(fmt.Errorf("MQTT 连接断开: %w", err))
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
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.config.ConnectTimeout):
		return errors.New("订阅 VDM Topic 超时")
	}
}

func (c *Client) Close() {
	c.failPending(errors.New("VDM MQTT client stopped"))
	c.client.Disconnect(1000)
}

func (c *Client) PublishRaw(topic string, payload any, retained bool) error {
	token := c.client.Publish(topic, c.config.QoS, retained, payload)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("MQTT publish ACK 超时: %s", topic)
	}
	return token.Error()
}

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
	if err := c.PublishRaw(c.config.Topics.RPCRequest(), payload, false); err != nil {
		return nil, err
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

func (c *Client) onMessage(_ mqtt.Client, message mqtt.Message) {
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
