package protobuf

import (
	"context"
	"fmt"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Codec struct {
	files *protoregistry.Files
	types *dynamicpb.Types
}

func New(ctx context.Context, files, importPaths []string) (*Codec, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: importPaths}),
	}
	compiled, err := compiler.Compile(ctx, files...)
	if err != nil {
		return nil, fmt.Errorf("compile proto files: %w", err)
	}
	registry := new(protoregistry.Files)
	seen := make(map[string]bool)
	var register func(protoreflect.FileDescriptor) error
	register = func(file protoreflect.FileDescriptor) error {
		if seen[file.Path()] {
			return nil
		}
		for i := 0; i < file.Imports().Len(); i++ {
			if err := register(file.Imports().Get(i).FileDescriptor); err != nil {
				return err
			}
		}
		seen[file.Path()] = true
		return registry.RegisterFile(file)
	}
	for _, file := range compiled {
		if err := register(file); err != nil {
			return nil, fmt.Errorf("register proto file %s: %w", file.Path(), err)
		}
	}
	return &Codec{files: registry, types: dynamicpb.NewTypes(registry)}, nil
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

func (c *Codec) Encode(name string, jsonValue []byte) ([]byte, error) {
	message, err := c.message(name)
	if err != nil {
		return nil, err
	}
	if err := (protojson.UnmarshalOptions{Resolver: c.types}).Unmarshal(jsonValue, message); err != nil {
		return nil, fmt.Errorf("invalid %s JSON: %w", name, err)
	}
	return proto.Marshal(message)
}

func (c *Codec) Decode(name string, value []byte) ([]byte, error) {
	message, err := c.message(name)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(value, message); err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return (protojson.MarshalOptions{Resolver: c.types}).Marshal(message)
}
