package protobuf

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func New(
	ctx context.Context,
	files []string,
	importPaths []string,
) (*Codec, error) {
	if len(importPaths) == 0 {
		importPaths = []string{"."}
	}
	paths := make([]string, len(importPaths))
	for i, path := range importPaths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve proto import path %q: %w", path, err)
		}
		info, err := os.Stat(absolute)
		if err != nil {
			return nil, fmt.Errorf("proto import path %q: %w", absolute, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("proto import path %q is not a directory", absolute)
		}
		paths[i] = absolute
	}
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: paths}),
	}
	compiled, err := compiler.Compile(ctx, files...)
	if err != nil {
		return nil, fmt.Errorf("compile proto files %q (import paths: %q): %w", files, paths, err)
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
	return &Codec{
		files: registry,
		types: dynamicpb.NewTypes(registry),
	}, nil
}
