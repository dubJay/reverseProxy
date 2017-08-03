package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

var (
	cert      = flag.String("cert", "", "Concatenation of server's certificate, any intermediates, and the CA's certificate")
	hosts = flag.String("hosts", "", "CSV of all hosts to load balance across")
	key       = flag.String("key", "", "Private key for TLS")
	port  = flag.String("port", ":8080", "Port for server to listen on")
	rootDir   = flag.String("rootDir", "", "Path to webdir structure")
	ssl       = flag.Bool("ssl", false, "Whether to use TLS")
	queue chan *url.URL
)

// Simple round robin queue. ATM all backend comps have equal compute power.
func seedQueue(allHosts []string) error {
	queue = make(chan *url.URL, len(allHosts))
	// Populate queue.
	for _, host := range allHosts {
		// TODO: Validate host can be reached.
		url, err := url.Parse(host)
		if err != nil {
			log.Printf("Error parsing url %s: %v", host, err)
		}
		queue <- url
	}
	if len(allHosts) == 0 {
		return errors.New("No hosts provided for reverse proxy")
	}	
	return nil
}

func getNext() *url.URL {
	if len(queue) == 0 {
		// Rare case that may occur if under heavy load.
		log.Printf("Queue empty at %v", time.Now().Unix())
		return &url.URL{}
	}
	var host = <-queue
	queue <- host
	return host
}

// Copy from https://golang.org/src/net/http/httputil/reverseproxy.go
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

func newMultiHostReverseProxy() *httputil.ReverseProxy {
	// Remnant of SingleHostReverseProxy.
	targetQuery := ""
	director := func(req *http.Request) {
		var target = getNext()
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = singleJoiningSlash(target.Path, req.URL.Path)
		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}
	return &httputil.ReverseProxy{Director: director}
}

func redirect(w http.ResponseWriter, req *http.Request) {
	// remove/add not default ports from req.Host
	target := "https://" + req.Host + req.URL.Path
	if len(req.URL.RawQuery) > 0 {
		target += "?" + req.URL.RawQuery
	}
	http.Redirect(w, req, target, http.StatusTemporaryRedirect)
}

func main() {
	flag.Parse()
	
	err := seedQueue(strings.Split(*hosts, ","))
	if err != nil {
		log.Fatal(err)
	}
	rp := newMultiHostReverseProxy()

	router := mux.NewRouter()
	router.HandleFunc("/", rp.ServeHTTP).Methods("GET")

	if !*ssl {
		log.Fatal(http.ListenAndServe(*port, router))
	} else {
		go http.ListenAndServe(":80", http.HandlerFunc(redirect))
		log.Fatal(http.ListenAndServeTLS(
			*port, filepath.Join(*rootDir, *cert), filepath.Join(*rootDir, *key), router))
	}
}
