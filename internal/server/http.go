package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer returns a pointer to an http.Server with our routes
func NewHTTPServer(address string) *http.Server {
	httpserver := newHTTPServer()
	r := mux.NewRouter()

	r.HandleFunc("/", httpserver.handleProduce).Methods("POST")
	r.HandleFunc("/", httpserver.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    address,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceRequest is the POST body with a Record
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse is the response object containing the Offset value of the log
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest is the GET request providing the offset of the log
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse is the response object containing the Record given the offset value
type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
