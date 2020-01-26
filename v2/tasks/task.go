package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	tasksv1 "github.com/RichardKnop/machinery/v1/tasks"
)

var ValidateTask = tasksv1.ValidateTask

type contextKey string

const signatureCtx contextKey = "signature"

// NewWithSignature is the same as New but injects the signature
func NewWithSignature(taskFunc interface{}, signature *Signature) (*tasksv1.Task, error) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, signatureCtx, signature)
	taskFuncValue := reflect.ValueOf(taskFunc)
	task := &tasksv1.Task{
		TaskFunc: taskFuncValue,
		Context:  ctx,
	}

	taskFuncType := taskFuncValue.Type()
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if tasksv1.IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	for _, a := range signature.Args {
		task.Args = append(task.Args, reflect.ValueOf(a))
	}

	return task, nil
}

// TODO: Add logging
func DecodeArgs(taskFuncValue reflect.Value, args []json.RawMessage) ([]interface{}, error) {
	fvt := taskFuncValue.Type()
	if fvt.Kind() != reflect.Func {
		return []interface{}{}, errors.New("fn must be a function")
	}

	if fvt.NumIn() > len(args) {
		return []interface{}{}, fmt.Errorf("fn expected %d args, %d supplied", fvt.NumIn(), len(args))
	}

	in := make([]interface{}, fvt.NumIn())
	for i := 0; i < fvt.NumIn(); i++ {
		fat := fvt.In(i)
		nv := reflect.New(fat)
		if nv.Elem().Type() != fat {
			return []interface{}{}, fmt.Errorf("argument type %s does not match expected type %s", nv.Elem().Type(), fat)
		}

		if err := json.Unmarshal(args[i], nv.Interface()); err != nil {
			return []interface{}{}, fmt.Errorf("error unmarshalling argument json: %w", err)
		}

		in[i] = nv.Elem().Interface()
	}

	return in, nil
}
