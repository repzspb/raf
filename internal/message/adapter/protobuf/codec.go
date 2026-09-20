package protobuf

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Codec преобразует ProtoJSON и бинарный Protobuf по загруженным дескрипторам.
type Codec struct {
	// files содержит дескрипторы корневых контрактов и всех их импортов.
	files *protoregistry.Files
	// types разрешает динамические типы при преобразовании сообщений, включая поля Any.
	types *dynamicpb.Types
}

// MessageTypes возвращает полные имена сообщений, включая вложенные и импортированные.
// Служебные сообщения для элементов map в список не попадают.
func (c *Codec) MessageTypes() []string {
	names := make([]string, 0)
	var collect func(protoreflect.MessageDescriptors)
	collect = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			if !message.IsMapEntry() {
				names = append(names, string(message.FullName()))
				collect(message.Messages())
			}
		}
	}
	c.files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		collect(file.Messages())
		return true
	})
	sort.Strings(names)
	return names
}

func (c *Codec) message(name string) (*dynamicpb.Message, error) {
	descriptor, err := c.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("unknown protobuf message type %q: %w", name, err)
	}
	message, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a protobuf message", name)
	}
	return dynamicpb.NewMessage(message), nil
}

func (c *Codec) ValidateType(name string) error {
	_, err := c.message(name)
	return err
}

func (c *Codec) Encode(
	name string,
	jsonValue []byte,
) ([]byte, error) {
	message, err := c.message(name)
	if err != nil {
		return nil, err
	}
	if err := (protojson.UnmarshalOptions{Resolver: c.types}).Unmarshal(jsonValue, message); err != nil {
		return nil, fmt.Errorf("invalid %s JSON: %w", name, err)
	}
	value, err := proto.Marshal(message)
	if err == nil && value == nil {
		// Пустое Protobuf-сообщение занимает ноль байт; nil означал бы tombstone.
		value = []byte{}
	}
	return value, err
}

func (c *Codec) Decode(
	name string,
	value []byte,
) ([]byte, error) {
	message, err := c.message(name)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(value, message); err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return (protojson.MarshalOptions{Resolver: c.types}).Marshal(message)
}
