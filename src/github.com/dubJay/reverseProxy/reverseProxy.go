package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

var (
	// Used to determine when to refresh map data.
	mapRefresh int64
	tmpls map[string]*template.Template
	
	cert        = flag.String("cert", "", "Concatenation of server's certificate, any intermediates, and the CA's certificate")
	hosts       = flag.String("hosts", "", "CSV of all hosts to load balance across")
	staticHosts = flag.String("static_hosts", "", "CSV of all static hosts to load balance across")
	key         = flag.String("key", "", "Private key for TLS")
	port        = flag.String("port", ":8080", "Port for server to listen on")
	rootDir     = flag.String("rootDir", "", "Path to webdir structure")
	ssl         = flag.Bool("ssl", false, "Whether to use TLS")
	dataDir     = flag.String("dataDir", "data", "Text file containing ips from ssh attempts")
	templates = flag.String("templates", "templates", "Templates directory")
)

const mapPage = "map.html"

func initTmpls() {
	var err error
	tmpls = make(map[string]*template.Template)

	tmpls[mapPage], err = template.New(
		mapPage).Funcs(template.FuncMap{
			"marshal": func(v interface{}) template.JS {
				a, _ := json.Marshal(v)
				return template.JS(a)
			},
		}).ParseFiles(filepath.Join(*rootDir, *templates, mapPage))
	if err != nil {
		log.Fatalf("error parsing template %s: %v", mapPage, err)
	}
	log.Print("Templates successfully initialized")
}

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
	// Refresh map data trigger. Called everytime a page is requested.
	go refreshMapData()
	
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
	target := "https://" + req.Host + strings.TrimRight(req.URL.Path, "/") + *port
	log.Print(target)
	if len(req.URL.RawQuery) > 0 {
		target += "?" + req.URL.RawQuery
	}
	http.Redirect(w, req, target, http.StatusTemporaryRedirect)
}

type LatLng struct {
	Latitude float64  `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type IPLocation struct {
	IP string       `json:"ip"`
	Coords LatLng   `json:"coords"`
	Attempts int32  `json:"attempts"`
	Timestamp int64 `json:"timestamp"`
}

type IPLocations []IPLocation

// Begin Geocode service structs.
type Geo struct {
	Host string           `json:"host"`
	Ip string             `json:"ip"`
	Rdns string           `json:"rdns"`
	Asn string            `json:"asn"`
	Isp string            `json:"isp"`
	CountryName string    `json:"country_name"`
	CountryCode string    `json:"country_code"`
	Region string         `json:"region"`
	City string           `json:"city"`
	PostalCode string     `json:"postal_code"`
	ContinentCode string  `json:"continent_code"`
	Latitude string       `json:"latitude"`
	Longitude string      `json:"longitude"`
	DmaCode string        `json:"dma_code"`
	AreaCode string       `json:"area_code"`
	TimeZone string       `json:"timezone"`
	DateTime string       `json:"datetime"`
}

type Data struct {
	GeoResponse Geo `json:"geo"`
}

type GeocodeResponse struct {
	Status string `json:"status"`
	Description string `json:"description"`
	DataResponse Data `json:"data"`
}
// End Geocode service structs.

func geocodeIP(geocodeClient http.Client, url string, ip string) (LatLng, error) {
	req, err := http.NewRequest(http.MethodGet, url+ip, nil)
	if err != nil {
		return LatLng{}, err
	}

	resp, err := geocodeClient.Do(req)
	if err != nil {
		return LatLng{}, err
	}
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return LatLng{}, err
	}

	geocodeResp := GeocodeResponse{}
	err = json.Unmarshal(body, &geocodeResp)
	if err != nil {
		return LatLng{}, fmt.Errorf("record: %s. %v", string(body), err)
	}

	if geocodeResp.Status != "success" {
		return LatLng{}, fmt.Errorf("lookup failed for ip %s. Description: %s", ip, geocodeResp.Description)
	}

	latitude, err := strconv.ParseFloat(geocodeResp.DataResponse.GeoResponse.Latitude, 64)
	if err != nil {
		return LatLng{}, fmt.Errorf("failed to convert latitude to float64: %v", err)
	}
	longitude, err := strconv.ParseFloat(geocodeResp.DataResponse.GeoResponse.Longitude, 64)
	if err != nil {
		return LatLng{}, fmt.Errorf("failed to convert longitue to float64: %v", err)
	}
	
	return LatLng{
		Latitude: latitude,
		Longitude: longitude,
	}, nil

}
func geocodeIPs(timestamp int64, ips map[string]int32) IPLocations {
	ctx := context.Background()
	url := "https://tools.keycdn.com/geo.json?host="
	geocodeClient := http.Client{
		Timeout: time.Second * 10,
	}

	var iplocations IPLocations
	
	// One query every two seconds.
	limiter := rate.NewLimiter(2, 1)
	for ip, count := range ips {
		limiter.Wait(ctx)
		latlng, err := geocodeIP(geocodeClient, url, ip)
		if err != nil {
			log.Printf("Error geocoding ip: %s. Continuing: %v", ip, err)
			continue
		}
		iplocations = append(iplocations,
			IPLocation{
				IP: ip,
				Coords: latlng,
				Attempts: count,
				Timestamp: timestamp,
			})
	}

	return iplocations
}

// This could run as an offline job but for maintenance purposes the trigger is in getMap.
func refreshMapData() {
	now := time.Now()
	if mapRefresh >= now.Unix() {
		return
	}

	dataDirectory, iplocations, err := readMapData()
	if err != nil {
		log.Printf("unable to refreshMapData: %v")
		return
	}

	// ips.txt is regenerated via crontab every hour.
	// Command:
	//     awk ' {match($0,/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/); ip = substr($0,RSTART,RLENGTH); logTime=$1" "$2 ; \
	//       wantTime = strftime("%b%e", systime()) ; if (logTime == wantTime && $5 ~ "sshd" && ip != "") \
	//       print ip }' /var/log/auth.log > data/ips.txt
	ipFile, err := os.Open(filepath.Join(dataDirectory, "ips.txt"))
	if err != nil {
		log.Printf("failed to open ipFile. Exiting: %v", err)
		return
	}
	defer ipFile.Close()

	// Read file and build set from ips.
	deduped := make(map[string]int32)
	scanner := bufio.NewScanner(ipFile)
	for scanner.Scan() {
		ip := scanner.Text()
		if _, ok := deduped[ip]; !ok {
			deduped[ip] = 1
		}
		deduped[ip] += 1 
	}
	if err := scanner.Err(); err != nil {
		log.Printf("failed to read ipFile. Exiting: %v", err)
		return
	}

	// Key IPLocations by ip address for easy lookup.
	var iplocationsFinal IPLocations
	iplocationsMap := make(map[string]*IPLocation)
	for _, iplocation := range iplocations {
		// IPlocation is considered stale if not within a day of now.
		// This has two side effects:
		//     1) Even if ips.txt only has today's entries it's possible for map_data.json
		//        to contain a full 24 hour record of all attempts.
		//     2) An edge case is prevented where the Attempts of an ip are counted indefinitely.
		if time.Since(time.Unix(iplocation.Timestamp, 0)) <= time.Hour * 24 { 
			iplocationsMap[iplocation.IP] = &iplocation
			iplocationsFinal = append(iplocationsFinal, iplocation)
		}
	}

	// Determine which ips to geocode. If value is already present in iplocations
	// no need to re-geocode.
	lookupips := make(map[string]int32)
	for ip, count := range deduped {
		if _, ok := iplocationsMap[ip]; !ok {
			lookupips[ip] = count
			continue
		}
		iplocationsMap[ip].Attempts += count
		
	}

	// Geocode ips and append results to iplocations.
	for _, iplocation := range geocodeIPs(now.Unix(), lookupips) {
		iplocationsFinal = append(iplocationsFinal, iplocation)
	}

	// Write iplocations back to file.
	iplocationsJson, err := json.Marshal(iplocationsFinal)
	if err != nil {
		fmt.Printf("failed to marshal iplocationsFinal json. Exiting: %v", err)
		return
	}
	err = ioutil.WriteFile(filepath.Join(dataDirectory, "map_data.json"), iplocationsJson, 0644)
	if err != nil {
		fmt.Printf("failed to write map_data.json. Exiting: %v", err)
		return
	}

	// Set mapRefresh to two hours from now.
	mapRefresh = time.Now().Add(time.Hour * 2).Unix()
}

func readMapData() (string, IPLocations, error) {
	var iplocations IPLocations
	dataDirectory := filepath.Join(*rootDir, *dataDir)

	cached, err := ioutil.ReadFile(filepath.Join(dataDirectory, "map_data.json"))
	if err != nil {
		return dataDirectory, iplocations, fmt.Errorf("unable to read iplocation.json file:  %v")
	}
	err = json.Unmarshal(cached, &iplocations)
	if err != nil {
		return dataDirectory, iplocations, fmt.Errorf("unable to unmarshal iplocation.json: %v")
	}
	return dataDirectory, iplocations, nil
}

func getMap(w http.ResponseWriter, req *http.Request) {
	_, iplocations, err := readMapData()
	if err != nil {
		log.Printf("Unable to build ssh_map: %v", err)
		http.Error(w, "unable to parse map data", http.StatusInternalServerError)
	}

	if err := tmpls[mapPage].Execute(w, iplocations); err != nil {
		log.Printf("error executing template %s: %v", iplocations, err)
	}
}

func main() {
	flag.Parse()

	initTmpls()

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
	mux.HandleFunc("/ssh_map", getMap)
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
