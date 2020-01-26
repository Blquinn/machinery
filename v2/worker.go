package machinery

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	tasksv1 "github.com/RichardKnop/machinery/v1/tasks"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/retry"
	tracingv1 "github.com/RichardKnop/machinery/v1/tracing"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/RichardKnop/machinery/v2/tracing"
	"github.com/opentracing/opentracing-go"
)

// Worker represents a single worker process
type Worker struct {
	server          *Server
	ConsumerTag     string
	Concurrency     int
	Queue           string
	errorHandler    func(err error)
	preTaskHandler  func(*tasks.Signature)
	postTaskHandler func(*tasks.Signature)
}

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	errorsChan := make(chan error)

	worker.LaunchAsync(errorsChan)

	return <-errorsChan
}

// LaunchAsync is a non blocking version of Launch
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	// Log some useful information about worker configuration
	log.INFO.Printf("Launching a worker with the following settings:")
	log.INFO.Printf("- Broker: %s", cnf.Broker)
	if worker.Queue == "" {
		log.INFO.Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	} else {
		log.INFO.Printf("- CustomQueue: %s", worker.Queue)
	}
	log.INFO.Printf("- ResultBackend: %s", cnf.ResultBackend)
	if cnf.AMQP != nil {
		log.INFO.Printf("- AMQP: %s", cnf.AMQP.Exchange)
		log.INFO.Printf("  - Exchange: %s", cnf.AMQP.Exchange)
		log.INFO.Printf("  - ExchangeType: %s", cnf.AMQP.ExchangeType)
		log.INFO.Printf("  - BindingKey: %s", cnf.AMQP.BindingKey)
		log.INFO.Printf("  - PrefetchCount: %d", cnf.AMQP.PrefetchCount)
	}

	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker.Concurrency, worker)

			if retry {
				if worker.errorHandler != nil {
					worker.errorHandler(err)
				} else {
					log.WARNING.Printf("Broker failed with error: %s", err)
				}
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
	if !cnf.NoUnixSignals {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		var signalsReceived uint

		// Goroutine Handle SIGINT and SIGTERM signals
		go func() {
			for {
				select {
				case s := <-sig:
					log.WARNING.Printf("Signal received: %v", s)
					signalsReceived++

					if signalsReceived < 2 {
						// After first Ctrl+C start quitting the worker gracefully
						log.WARNING.Print("Waiting for running tasks to finish before shutting down")
						go func() {
							worker.Quit()
							errorsChan <- errors.New("Worker quit gracefully")
						}()
					} else {
						// Abort the program when user hits Ctrl+C second time in a row
						errorsChan <- errors.New("Worker quit abruptly")
					}
				}
			}
		}()
	}
}

// CustomQueue returns Custom Queue of the running worker process
func (worker *Worker) CustomQueue() string {
	return worker.Queue
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// convertDecodeSignature converts a DecodeSignature and all of it's sub-signatures into
// Signatures. It needs a reference to the server (via the worker) to get the type information
// of the signature's task function.
// TODO: Log errors
func (worker *Worker) convertDecodeSignature(sig *tasks.DecodeSignature) (*tasks.Signature, error) {
	if sig == nil {
		return nil, nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(sig.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get registered task: %w", err)
	}

	args, err := tasks.DecodeArgs(reflect.ValueOf(taskFunc), sig.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to decode task args: %w", err)
	}

	onSuccess := make([]*tasks.Signature, 0, len(sig.OnSuccess))
	for _, s := range sig.OnSuccess {
		cs, err := worker.convertDecodeSignature(s)
		if err != nil {
			return nil, err
		}
		onSuccess = append(onSuccess, cs)
	}
	onError := make([]*tasks.Signature, 0, len(sig.OnError))
	for _, s := range sig.OnError {
		cs, err := worker.convertDecodeSignature(s)
		if err != nil {
			return nil, err
		}
		onError = append(onError, cs)
	}

	ccb, err := worker.convertDecodeSignature(sig.ChordCallback)
	if err != nil {
		return nil, err
	}

	return &tasks.Signature{
		Signature: tasksv1.Signature{
			UUID:                        sig.UUID,
			Name:                        sig.Name,
			RoutingKey:                  sig.RoutingKey,
			ETA:                         sig.ETA,
			GroupUUID:                   sig.GroupUUID,
			GroupTaskCount:              sig.GroupTaskCount,
			Headers:                     sig.Headers,
			Priority:                    sig.Priority,
			Immutable:                   sig.Immutable,
			RetryCount:                  sig.RetryCount,
			RetryTimeout:                sig.RetryTimeout,
			BrokerMessageGroupId:        sig.BrokerMessageGroupId,
			SQSReceiptHandle:            sig.SQSReceiptHandle,
			StopTaskDeletionOnError:     sig.StopTaskDeletionOnError,
			IgnoreWhenTaskNotRegistered: sig.IgnoreWhenTaskNotRegistered,
		},
		Args:          args,
		OnSuccess:     onSuccess,
		OnError:       onError,
		ChordCallback: ccb,
	}, nil
}

// Process handles received tasks and triggers success/error callbacks
// TODO: This needs logging...
func (worker *Worker) Process(decodeSignature *tasks.DecodeSignature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(decodeSignature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(decodeSignature.Name)
	if err != nil {
		return nil
	}

	signature, err := worker.convertDecodeSignature(decodeSignature)
	if err != nil {
		return fmt.Errorf("failed to decode task: %w", err)
	}

	// Update task state to RECEIVED
	if err = worker.server.GetBackend().SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set state to 'received' for task %s returned error: %s", decodeSignature.UUID, err)
	}

	// Prepare task for processing
	task, err := tasks.NewWithSignature(taskFunc, signature)
	// if this failed, it means the task is malformed, probably has invalid
	// decodeSignature, go directly to task failed without checking whether to retry
	if err != nil {
		worker.taskFailed(signature, err)
		return err
	}

	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := tracingv1.StartSpanFromHeaders(decodeSignature.Headers, decodeSignature.Name)
	tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// Update task state to STARTED
	if err = worker.server.GetBackend().SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set state to 'started' for task %s returned error: %s", decodeSignature.UUID, err)
	}

	//Run handler before the task is called
	if worker.preTaskHandler != nil {
		worker.preTaskHandler(signature)
	}

	//Defer run handler for the end of the task
	if worker.postTaskHandler != nil {
		defer worker.postTaskHandler(signature)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		// If a tasks.ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		retriableErr, ok := interface{}(err).(tasksv1.ErrRetryTaskLater)
		if ok {
			return worker.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on decodeSignature.RetryCount
		// and decodeSignature.RetryTimeout values
		if decodeSignature.RetryCount > 0 {
			return worker.taskRetry(signature)
		}

		return worker.taskFailed(signature, err)
	}

	res := make([]interface{}, 0, len(results))
	for _, r := range results {
		res = append(res, r.Value)
	}

	return worker.taskSucceeded(signature, res)
}

// retryTask decrements RetryCount counter and republishes the task to the queue
func (worker *Worker) taskRetry(signature *tasks.Signature) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = retry.FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %d seconds.", signature.UUID, signature.RetryTimeout)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskRetryIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (worker *Worker) retryTaskIn(signature *tasks.Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %.0f seconds.", signature.UUID, retryIn.Seconds())

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (worker *Worker) taskSucceeded(signature *tasks.Signature, taskResults []interface{}) error {
	// Update task state to SUCCESS
	if err := worker.server.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set state to 'success' for task %s returned error: %s", signature.UUID, err)
	}

	// Log human readable results of the processed task
	log.DEBUG.Printf("Processed task %s. Results = %+v", signature.UUID, taskResults)

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				successTask.Args = append(successTask.Args, taskResult)
			}
		}

		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Completed check for group %s returned error: %s", signature.GroupUUID, err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if worker.hasAMQPBackend() {
		defer worker.server.GetBackend().PurgeGroupMeta(signature.GroupUUID)
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.GetBackend().TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Triggering chord for group %s returned error: %s", signature.GroupUUID, err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, taskResult)
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// taskFailed updates the task state and triggers error callbacks
func (worker *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state to 'failure' for task %s returned error: %s", signature.UUID, err)
	}

	if worker.errorHandler != nil {
		worker.errorHandler(taskErr)
	} else {
		log.ERROR.Printf("Failed processing task %s. Error = %v", signature.UUID, taskErr)
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]interface{}{taskErr.Error()}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	if signature.StopTaskDeletionOnError {
		return errs.ErrStopTaskDeletion
	}

	return nil
}

// Returns true if the worker uses AMQP backend
// TODO: Implement v2 amqp backend
func (worker *Worker) hasAMQPBackend() bool {
	//_, ok := worker.server.GetBackend().(*amqp.Backend)
	//return ok
	return false
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (worker *Worker) SetErrorHandler(handler func(err error)) {
	worker.errorHandler = handler
}

//SetPreTaskHandler sets a custom handler func before a job is started
func (worker *Worker) SetPreTaskHandler(handler func(*tasks.Signature)) {
	worker.preTaskHandler = handler
}

//SetPostTaskHandler sets a custom handler for the end of a job
func (worker *Worker) SetPostTaskHandler(handler func(*tasks.Signature)) {
	worker.postTaskHandler = handler
}

//GetServer returns server
func (worker *Worker) GetServer() *Server {
	return worker.server
}
