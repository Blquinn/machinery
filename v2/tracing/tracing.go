package tracing

import (
	"encoding/json"

	tasksv1 "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/opentracing/opentracing-go"
	opentracing_log "github.com/opentracing/opentracing-go/log"
)

func AnnotateSpanWithSignatureInfo(span opentracing.Span, signature *tasks.Signature) {
	// tag the span with some info about the signature
	span.SetTag("signature.name", signature.Name)
	span.SetTag("signature.uuid", signature.UUID)

	if signature.GroupUUID != "" {
		span.SetTag("signature.group.uuid", signature.UUID)
	}

	if signature.ChordCallback != nil {
		span.SetTag("signature.chord.callback.uuid", signature.ChordCallback.UUID)
		span.SetTag("signature.chord.callback.name", signature.ChordCallback.Name)
	}
}

func AnnotateSpanWithGroupInfo(span opentracing.Span, group *tasks.Group, sendConcurrency int) {
	// tag the span with some info about the group
	span.SetTag("group.uuid", group.GroupUUID)
	span.SetTag("group.tasks.length", len(group.Tasks))
	span.SetTag("group.concurrency", sendConcurrency)

	// encode the task uuids to json, if that fails just dump it in
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		span.SetTag("group.tasks", string(taskUUIDs))
	} else {
		span.SetTag("group.tasks", group.GetUUIDs())
	}

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(headers tasksv1.Headers, span opentracing.Span) tasksv1.Headers {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(tasksv1.Headers)
	}

	if err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, headers); err != nil {
		span.LogFields(opentracing_log.Error(err))
	}

	return headers
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(span opentracing.Span, chain *tasks.Chain) {
	// tag the span with some info about the chain
	span.SetTag("chain.tasks.length", len(chain.Tasks))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(span opentracing.Span, chord *tasks.Chord, sendConcurrency int) {
	// tag the span with chord specific info
	span.SetTag("chord.callback.uuid", chord.Callback.UUID)

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(chord.Callback.Headers, span)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(span, chord.Group, sendConcurrency)
}
