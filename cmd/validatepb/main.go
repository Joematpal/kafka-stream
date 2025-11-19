package main

import (
	"fmt"
	"log"
	"os"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func main() {
	// Example 1: Load descriptor from binpb file and validate
	fmt.Println("=== Example 1: Loading from .binpb file ===")

	// Step 1: Load the descriptor from the binpb file
	// This gives you the MESSAGE STRUCTURE (fields, types, validation rules)
	fd, err := loadDescriptorFromFile("descriptors.binpb")
	if err != nil {
		log.Printf("Failed to load descriptor (skipping example 1): %v\n", err)
	} else {
		// Step 2: Create a validator instance
		// This validator can validate ANY protobuf message
		validator, err := protovalidate.New()
		if err != nil {
			log.Fatalf("Failed to create validator: %v", err)
		}

		// Step 3: Create a dynamic message from the descriptor
		// This is your actual data structure/instance
		msgDesc := fd.Messages().ByName("User") // Get "User" message type
		if msgDesc == nil {
			log.Fatal("User message not found in descriptor")
		}
		msg := dynamicpb.NewMessage(msgDesc)

		// Step 4: Populate the message with data
		fields := msgDesc.Fields()
		emailField := fields.ByName("email")
		ageField := fields.ByName("age")

		msg.Set(emailField, protoreflect.ValueOfString("invalid-email"))
		msg.Set(ageField, protoreflect.ValueOfInt32(-5))

		// Step 5: Validate the message instance
		// The validator reads the validation rules FROM the message's descriptor
		// which came from the binpb file
		if err := validator.Validate(msg); err != nil {
			fmt.Printf("Validation failed: %v\n", err)
		} else {
			fmt.Println("Validation passed!")
		}
	}

	// Example 2: Using programmatically created descriptor
	fmt.Println("\n=== Example 2: Using programmatically created descriptor ===")
	descriptor := createSampleDescriptor()

	validator, err := protovalidate.New()
	if err != nil {
		log.Fatalf("Failed to create validator: %v", err)
	}

	msgDesc := descriptor.Messages().Get(0)
	msg := dynamicpb.NewMessage(msgDesc)

	fields := msgDesc.Fields()
	emailField := fields.ByName("email")
	ageField := fields.ByName("age")

	msg.Set(emailField, protoreflect.ValueOfString("valid@example.com"))
	msg.Set(ageField, protoreflect.ValueOfInt32(25))

	if err := validator.Validate(msg); err != nil {
		fmt.Printf("Validation failed: %v\n", err)
	} else {
		fmt.Println("Validation passed!")
	}

	// Example 3: Load message from JSON
	fmt.Println("\n=== Example 3: Loading message from JSON ===")

	// First, we need a descriptor (same as before)
	fd2, err := loadDescriptorFromFile("descriptors.binpb")
	if err != nil {
		log.Printf("Failed to load descriptor (skipping JSON example): %v\n", err)
	} else {
		// Load and validate a message from JSON
		err = loadAndValidateFromJSON(fd2, "user.json")
		if err != nil {
			log.Printf("JSON example failed: %v\n", err)
		}
	}
}

// createSampleDescriptor creates a sample file descriptor for demonstration
func createSampleDescriptor() protoreflect.FileDescriptor {
	// This is a simplified example. In practice, you'd load this from your
	// protobuf manifest/descriptor file
	fileDescProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("example.proto"),
		Package: proto.String("example"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("User"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("email"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name:   proto.String("age"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fileDescProto, nil)
	if err != nil {
		log.Fatalf("Failed to create file descriptor: %v", err)
	}

	return fd
}

// Alternative: Load descriptor from a compiled descriptor file
func loadDescriptorFromFile(filename string) (protoreflect.FileDescriptor, error) {
	// Read the descriptor file (.binpb)
	// This file contains the SCHEMA - the blueprint of your messages
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read descriptor file: %w", err)
	}

	// Parse the FileDescriptorSet
	// This deserializes the binary format into a Go structure
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, fds); err != nil {
		return nil, fmt.Errorf("failed to unmarshal descriptor: %w", err)
	}

	// Create file descriptors from the set
	// This converts the raw descriptor proto into a usable FileDescriptor
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, fmt.Errorf("failed to create files: %w", err)
	}

	// Get the first file descriptor (or find by path)
	// To find by path: return files.FindFileByPath("your/proto/file.proto")
	var fd protoreflect.FileDescriptor
	fds.GetFile()[0].GetName() // Get the name
	files.RangeFiles(func(f protoreflect.FileDescriptor) bool {
		fd = f
		return false // stop after first
	})

	if fd == nil {
		return nil, fmt.Errorf("no file descriptors found")
	}

	return fd, nil
}

// loadMessageFromJSON loads a protobuf message from a JSON file
func loadMessageFromJSON(fd protoreflect.FileDescriptor, messageName string, jsonFile string) (proto.Message, error) {
	// Read the JSON file
	jsonData, err := os.ReadFile(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
	}

	// Get the message descriptor
	msgDesc := fd.Messages().ByName(protoreflect.Name(messageName))
	if msgDesc == nil {
		return nil, fmt.Errorf("message %s not found in descriptor", messageName)
	}

	// Create a new dynamic message
	msg := dynamicpb.NewMessage(msgDesc)

	// Unmarshal JSON into the protobuf message
	if err := protojson.Unmarshal(jsonData, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return msg, nil
}

// loadAndValidateFromJSON demonstrates loading a message from JSON and validating it
func loadAndValidateFromJSON(fd protoreflect.FileDescriptor, jsonFile string) error {
	// Load the message from JSON
	msg, err := loadMessageFromJSON(fd, "User", jsonFile)
	if err != nil {
		return fmt.Errorf("failed to load message from JSON: %w", err)
	}

	// Create validator
	validator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	// Validate the message
	if err := validator.Validate(msg); err != nil {
		fmt.Printf("JSON message validation failed: %v\n", err)
		return err
	}

	fmt.Println("JSON message validation passed!")

	// Optional: Print the loaded message
	fmt.Printf("Loaded message: %v\n", msg)

	return nil
}

// createSampleJSONFile creates a sample JSON file for testing
func createSampleJSONFile(filename string) error {
	jsonContent := `{
	 "email": "user@example.com",
	 "age": 30
}`

	return os.WriteFile(filename, []byte(jsonContent), 0644)
}

// HOW IT WORKS TOGETHER:
//
// 1. loadDescriptorFromFile() reads the .binpb and returns a FileDescriptor
//    - The FileDescriptor contains the MESSAGE SCHEMA (structure, fields, validation rules)
//
// 2. protovalidate.New() creates a validator instance
//    - This is a general-purpose validator that can validate ANY protobuf message
//    - It doesn't need to know about your specific schema yet
//
// 3. dynamicpb.NewMessage(msgDesc) creates a MESSAGE INSTANCE
//    - The message instance internally references its descriptor
//    - The descriptor contains the validation rules
//
// 4. validator.Validate(msg) validates the message
//    - The validator looks at the message's descriptor (from step 3)
//    - It reads the validation rules embedded in that descriptor
//    - It applies those rules to the message data
//
// In summary:
// - .binpb file → FileDescriptor (the schema with validation rules)
// - protovalidate.New() → Validator (the validation engine)
// - FileDescriptor → DynamicMessage (data instance with schema reference)
// - Validator.Validate(DynamicMessage) → reads rules from message's descriptor
