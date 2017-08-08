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
)

var (
	cert        = flag.String("cert", "", "Concatenation of server's certificate, any intermediates, and the CA's certificate")
	hosts       = flag.String("hosts", "", "CSV of all hosts to load balance across")
	staticHosts = flag.String("static_hosts", "", "CSV of all static hosts to load balance across")
	key         = flag.String("key", "", "Private key for TLS")
	port        = flag.String("port", ":8080", "Port for server to listen on")
	rootDir     = flag.String("rootDir", "", "Path to webdir structure")
	ssl         = flag.Bool("ssl", false, "Whether to use TLS")
)

type queues struct{
	queue chan *url.URL
	staticQueue chan *url.URL
}

// Simple round robin queue. ATM all backend comps have equal compute power.
func populateQueue(hosts []string, queue *chan *url.URL) error {
	tmp := make(chan *url.URL, len(hosts))
	// Populate queues.
	for _, host := range hosts {
		// TODO: Validate host can be reached.
		url, err := url.Parse(host)
		if err != nil {
			log.Printf("Error parsing url %s: %v", host, err)
		}
		tmp <- url
	}
	if len(hosts) == 0 {
		return errors.New("No hosts provided for reverse proxy")
	}

	*queue = tmp
	return nil
}

func getNext(queue *chan *url.URL) *url.URL {
	if len(*queue) == 0 {
		// Rare case that may occur if under heavy load.
		log.Printf("Queue empty at %v", time.Now().Unix())
		return &url.URL{}
	}
	var host = <-*queue
	*queue <- host
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

func newMultiHostReverseProxy(queue chan *url.URL) *httputil.ReverseProxy {
	// Remnant of SingleHostReverseProxy.
	targetQuery := ""
	director := func(req *http.Request) {
		var target = getNext(&queue)
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

	queues := &queues{}
	err := populateQueue(strings.Split(*hosts, ","), &queues.queue)
	if err != nil {
		log.Fatal(err)
	}
	err = populateQueue(strings.Split(*staticHosts, ","), &queues.staticQueue)
	if err != nil {
		log.Fatal(err)
	}
	
	rp := newMultiHostReverseProxy(queues.queue)
	srp := newMultiHostReverseProxy(queues.staticQueue)

	mux := http.NewServeMux()
	mux.HandleFunc("/static/", srp.ServeHTTP)
	mux.HandleFunc("/images/", srp.ServeHTTP)
	mux.HandleFunc("/", rp.ServeHTTP)

	if !*ssl {
		log.Fatal(http.ListenAndServe(*port, mux))
	} else {
		go http.ListenAndServe(":80", http.HandlerFunc(redirect))
		log.Fatal(http.ListenAndServeTLS(
			*port, filepath.Join(*rootDir, *cert), filepath.Join(*rootDir, *key), mux))
	}
}
