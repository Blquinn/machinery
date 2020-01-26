package tasks

import (
	"encoding/json"
	"fmt"

	tasksv1 "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/google/uuid"
)

// Signature is a copy of the v1 signature with a simplified args structure.
type Signature struct {
	tasksv1.Signature

	Args          []interface{}
	OnSuccess     []*Signature
	OnError       []*Signature
	ChordCallback *Signature
}

// NewSignature creates a new task signature
func NewSignature(name string, args ...interface{}) *Signature {
	return &Signature{
		Signature: tasksv1.Signature{
			UUID: fmt.Sprintf("task_%s", uuid.New()),
			Name: name,
		},
		Args: args,
	}
}

// DecodeSignature is used to deserialize the signature from the broker / result store.
// It defers the deserialization of the arguments in order to use the type information
// from the task function arguments to decode the json arguments.
type DecodeSignature struct {
	tasksv1.Signature

	Args          []json.RawMessage
	OnSuccess     []*DecodeSignature
	OnError       []*DecodeSignature
	ChordCallback *DecodeSignature
}
