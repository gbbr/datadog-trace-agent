package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/DataDog/datadog-trace-agent/info"
	"github.com/DataDog/datadog-trace-agent/model"
	"github.com/DataDog/datadog-trace-agent/model/zipkin"
	"github.com/DataDog/datadog-trace-agent/model/zipkin/zipkinv1"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/cihub/seelog"
)

// tagZipkinHandler is the metrics tag used for the Zipkin span handler.
const tagZipkinHandler = "handler:zipkin"

// handleZipkinV2Spans handles the endpoint accepting Zipkin V2 spans.
func (r *HTTPReceiver) handleZipkinSpansV2(w http.ResponseWriter, req *http.Request) {
	switch v := req.Header.Get("Content-Type"); v {
	case "application/json", "text/json":
		// OK
	default:
		// unsupported
		log.Errorf("/zipkin/v2/spans: unsupported media type %q", v)
		HTTPFormatError([]string{tagZipkinHandler}, w)
		return
	}
	var zipkinSpans []*zipkin.SpanModel
	reader := req.Body
	defer req.Body.Close()
	if enc := req.Header.Get("Content-Encoding"); enc != "" {
		switch enc {
		case "gzip":
			var err error
			reader, err = gzip.NewReader(reader)
			if err != nil {
				log.Errorf("/zipkin/v2/spans: error reading gzipped content")
				HTTPDecodingError(err, []string{tagZipkinHandler}, w)
				return
			}
			defer reader.Close()
		case "identity":
			// OK
		default:
			// unsupported
			log.Errorf("/zipkin/v2/spans: unsupported Content-Encoding: %s", enc)
			HTTPDecodingError(errors.New("unsupported Content-Encoding"), []string{tagZipkinHandler}, w)
			return
		}
	}
	if err := json.NewDecoder(reader).Decode(&zipkinSpans); err != nil {
		log.Errorf("/zipkin/v2/spans: cannot decode traces payload: %v", err)
		HTTPDecodingError(err, []string{tagZipkinHandler}, w)
		return
	}

	traces := tracesFromZipkinSpans(zipkinSpans)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK:%d:%d", len(traces), len(zipkinSpans))

	tags := info.Tags{
		Lang:          "unknown",
		LangVersion:   "unknown",
		Interpreter:   "unknown",
		TracerVersion: "zipkin.v2",
	}
	var size int64
	lr, ok := req.Body.(*model.LimitedReader)
	if ok {
		size = lr.Count
	}
	r.receiveTraces(traces, tags, size)
}

// tracesFromZipkinSpans creates Traces from a set of Zipkin spans.
func tracesFromZipkinSpans(zipkinSpans []*zipkin.SpanModel) model.Traces {
	seen := make(map[zipkin.ID]*zipkin.SpanModel, len(zipkinSpans))
	for _, zspan := range zipkinSpans {
		if dup, ok := seen[zspan.ID]; ok {
			// We have a duplicate ID, this is a case where the Zipkin server
			// normally merges spans together. As an example, this happens when a
			// client initiates a span that finishes on the server. Since Datadog
			// doesn't accept such behaviour, we'll keep the span and instead
			// generate a new ID for it to resolve the collision.
			//
			// This can however still prove problematic when the duplicate span comes
			// in as part of a subsequent payload, in which case we will not be able
			// to detect it. The best way to avoid this behaviour is to configure the
			// client in such a way that duplicate span IDs are not created. This is
			// possible in some languages such as Go (called "WithSharedSpans") or Java
			// (called "supportsJoin").
			zspan.ID = zipkin.ID(rand.Uint64())

			// These spans generally have the same ParentID too, so let's make the older
			// one act as the parent.
			if dup.ParentID == zspan.ParentID && dup.ParentID != nil {
				if zspan.Timestamp.Before(dup.Timestamp) {
					dup.ParentID = &zspan.ID
				} else {
					zspan.ParentID = &dup.ID
				}
			}
			seen[dup.ID] = dup
		}
		seen[zspan.ID] = zspan
	}
	// group by TraceID
	traces := make(model.Traces, 0)
	byID := make(map[uint64][]*model.Span)
	for _, zs := range seen {
		s := zs.Convert()
		byID[s.TraceID] = append(byID[s.TraceID], s)
	}
	for _, t := range byID {
		traces = append(traces, t)
	}
	return traces
}

// tagZipkinHandlerV1 is the metrics tag used for the Zipkin span handler.
const tagZipkinHandlerV1 = "handler:zipkin_v1"

// handleZipkinV1Spans handles the endpoint accepting Zipkin V1 spans.
func (r *HTTPReceiver) handleZipkinSpansV1(w http.ResponseWriter, req *http.Request) {
	if v := req.Header.Get("Content-Type"); v != "application/x-thrift" {
		log.Errorf("/zipkin/v1/spans: unsupported media type %q", v)
		HTTPFormatError([]string{tagZipkinHandlerV1}, w)
		return
	}
	trans := thrift.NewStreamTransportR(req.Body)
	proto := thrift.NewTBinaryProtocolTransport(trans)
	_, size, err := proto.ReadListBegin()
	if err != nil {
		log.Errorf("/zipkin/v1/spans: cannot decode list header: %v", err)
		HTTPDecodingError(err, []string{tagZipkinHandlerV1}, w)
		return
	}
	spans := make([]*zipkinv1.Span, size)
	for i := range spans {
		span := zipkinv1.NewSpan()
		if err := span.Read(proto); err != nil {
			log.Errorf("/zipkin/v1/spans: cannot decode span: %v", err)
			HTTPDecodingError(err, []string{tagZipkinHandlerV1}, w)
			return
		}
		spans[i] = span
	}
	fmt.Printf("\n%#v\n", spans)
}
