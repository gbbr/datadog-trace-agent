package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type Listener interface {
	Init(chan Span)
	Start()
}

type HttpListener struct {
	out chan Span
}

func NewHttpListener() *HttpListener {
	return &HttpListener{}
}

func (l *HttpListener) Init(out chan Span) {
	l.out = out
}

func (l *HttpListener) Start() {
	http.HandleFunc("/span", l.HandleSpan)
	http.HandleFunc("/spans", l.HandleSpans)
	log.Print("HTTP Listener running")
	log.Fatal(http.ListenAndServe(":7777", nil))
}

func (l *HttpListener) HandleSpan(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	var s Span
	err = json.Unmarshal(body, &s)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))

	s.Normalize()
	log.Printf("Span received. TraceID: %d, SpanID: %d, ParentID: %d, Start: %s, Service: %s, Type: %s",
		s.TraceID, s.SpanID, s.ParentID, s.FormatStart(), s.Service, s.Type)

	l.out <- s
}

func (l *HttpListener) HandleSpans(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	var spans []Span
	err = json.Unmarshal(body, &spans)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))

	log.Printf("Set of spans received")

	for _, s := range spans {
		s.Normalize()
		log.Printf("Span received. TraceID: %d, SpanID: %d, ParentID: %d, Start: %s, Service: %s, Type: %s",
			s.TraceID, s.SpanID, s.ParentID, s.FormatStart(), s.Service, s.Type)

		l.out <- s
	}
}