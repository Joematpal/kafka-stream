

tools:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest


generate:
	buf generate
	buf build -o gen/descriptors.binpb