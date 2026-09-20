package protobuf

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	exampleMaxDepth  = 8
	exampleMaxFields = 2048
)

// exampleBuilder ограничивает обход контракта и отслеживает рекурсивные ссылки.
type exampleBuilder struct {
	// ctx позволяет прервать генерацию при отмене HTTP-запроса.
	ctx context.Context
	// codec предоставляет дескрипторы и разрешение типов Protobuf.
	codec *Codec
	// path содержит типы текущей ветви вложенности.
	path map[protoreflect.FullName]bool
	// fields считает обработанные поля для ограничения размера примера.
	fields int
}

// Example возвращает пример ProtoJSON, пригодный для Encode с тем же типом.
// Необязательные рекурсивные ветви пропускаются; невыполнимые обязательные поля дают ошибку.
func (c *Codec) Example(
	ctx context.Context,
	name string,
) ([]byte, error) {
	message, err := c.message(name)
	if err != nil {
		return nil, err
	}
	builder := exampleBuilder{
		ctx:   ctx,
		codec: c,
		path:  make(map[protoreflect.FullName]bool),
	}
	value, err := builder.message(message.Descriptor(), 0)
	if err != nil {
		return nil, err
	}
	return (protojson.MarshalOptions{
		Resolver:          c.types,
		EmitDefaultValues: true,
	}).Marshal(value)
}

func (b *exampleBuilder) message(
	descriptor protoreflect.MessageDescriptor,
	depth int,
) (*dynamicpb.Message, error) {
	if err := b.ctx.Err(); err != nil {
		return nil, err
	}
	if depth >= exampleMaxDepth || b.path[descriptor.FullName()] {
		return nil, nil
	}
	message := dynamicpb.NewMessage(descriptor)
	if value, ok := wellKnownExample(descriptor.FullName()); ok {
		err := (protojson.UnmarshalOptions{Resolver: b.codec.types}).Unmarshal([]byte(value), message)
		return message, err
	}
	b.path[descriptor.FullName()] = true
	defer delete(b.path, descriptor.FullName())
	for i := 0; i < descriptor.Fields().Len(); i++ {
		if err := b.ctx.Err(); err != nil {
			return nil, err
		}
		b.fields++
		if b.fields > exampleMaxFields {
			return nil, fmt.Errorf("example exceeds %d fields", exampleMaxFields)
		}
		field := descriptor.Fields().Get(i)
		if oneof := field.ContainingOneof(); oneof != nil && message.WhichOneof(oneof) != nil {
			continue
		}
		valueField := field
		if field.IsMap() {
			valueField = field.MapValue()
		}
		value, err := b.value(valueField, depth+1)
		if err != nil {
			return nil, err
		}
		if !value.IsValid() {
			if field.Cardinality() == protoreflect.Required {
				return nil, fmt.Errorf("cannot generate required field %s: recursive type or depth limit", field.FullName())
			}
			continue
		}
		switch {
		case field.IsMap():
			key, err := b.value(field.MapKey(), depth+1)
			if err != nil {
				return nil, err
			}
			message.Mutable(field).Map().Set(key.MapKey(), value)
		case field.IsList():
			message.Mutable(field).List().Append(value)
		default:
			message.Set(field, value)
		}
	}
	return message, nil
}

func (b *exampleBuilder) value(
	field protoreflect.FieldDescriptor,
	depth int,
) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		message, err := b.message(field.Message(), depth)
		if err != nil || message == nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(message), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("example"), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("example")), nil
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true), nil
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(field.Enum().Values().Get(0).Number()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field %s", field.FullName())
	}
}

// wellKnownExample задаёт JSON для типов со специальным представлением ProtoJSON.
func wellKnownExample(name protoreflect.FullName) (string, bool) {
	switch name {
	case "google.protobuf.Timestamp":
		return `"2026-01-01T00:00:00Z"`, true
	case "google.protobuf.Duration":
		return `"1s"`, true
	case "google.protobuf.FieldMask":
		return `"exampleField"`, true
	case "google.protobuf.Struct":
		return `{"example":"value"}`, true
	case "google.protobuf.Value", "google.protobuf.StringValue":
		return `"example"`, true
	case "google.protobuf.ListValue":
		return `["example"]`, true
	case "google.protobuf.BytesValue":
		return `"ZXhhbXBsZQ=="`, true
	case "google.protobuf.BoolValue":
		return "true", true
	case "google.protobuf.Int64Value", "google.protobuf.UInt64Value":
		return `"1"`, true
	case "google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int32Value", "google.protobuf.UInt32Value":
		return "1", true
	case "google.protobuf.Any", "google.protobuf.Empty":
		return "{}", true
	default:
		return "", false
	}
}
