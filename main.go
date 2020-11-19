package main

import (
	"flag"
	"fmt"
	"github.com/deckarep/golang-set"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufferSize = 1024

// zero sized var
var None = struct{}{}

type mirror struct {
	addr   string
	conn   net.Conn
	closed uint32
}

// configs
var (
	printVer                bool
	connectTimeout          time.Duration
	delay                   time.Duration
	listenAddress           string
	mirrorCloseDelay        time.Duration
	downstreamURL           string
	incrementConnectTimeout time.Duration
	writeTimeout            time.Duration
	connectionRetryCount    int
	metricServerAddr        string
	logLevel                string
)

// stores
var msgHashStore *lru.Cache

//var mirrorWakeLock sync.RWMutex
var mirrorWake = sync.Map{}
var mirrorAddressesLock sync.RWMutex
var mirrorAddresses mirrorList

var (
	processedMsgCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "multiplier_processed_message_count",
		Help: "count of processed message",
	}, []string{"status"})
	msgIgnoredCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "multiplier_message_ignored_count",
		Help: "How many duplicated message ignored",
	})
	msgIgnoredSize = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "multiplier_message_ignored_size",
		Help: "Size sum of duplicated message ignored",
	})
	mirrorConnCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "multiplier_mirror_connection_count",
		Help: "count of connection to mirror",
	}, []string{"status", "addr"})
	mirrorConnLatencyMs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "multiplier_mirror_connection_latency_ms",
		Help:    "latency of connection to mirror in milliseconds",
		Buckets: []float64{1, 5, 50, 150},
	}, []string{"addr"})
	bytesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "multiplier_bytes_received",
		Help: "count of bytes of message received",
	}, []string{"addr"})
	bytesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "multiplier_bytes_sent",
		Help: "count of bytes of message sent",
	}, []string{"addr"})
	sentErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "multiplier_sent_error_count",
		Help: "count of errors while sending messages",
	}, []string{"addr"})
	msgHashStoreSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "multiplier_message_hash_store_size",
		Help: "size of lru message hash store",
	})
)

func init() {
	prometheus.MustRegister(
		processedMsgCount,
		msgIgnoredSize,
		mirrorConnCount,
		mirrorConnLatencyMs,
		bytesReceived,
		bytesSent,
		msgIgnoredCount,
		sentErrorCount,
		msgHashStoreSize,
	)
}

func forwardAndCopy(message []byte, mirrors []mirror) {
	wg := sync.WaitGroup{}
	for _, m := range mirrors {
		wg.Add(1)
		go func(m mirror) {
			defer wg.Done()
			_ = m.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			defer m.conn.Close()
			if c, err := m.conn.Write(message); err != nil {
				log.WithError(err).WithField("addr", m.addr).Error("Error while sending message")
				atomic.StoreUint32(&m.closed, 1)
				sentErrorCount.WithLabelValues(m.addr).Inc()
			} else {
				bytesSent.WithLabelValues(m.addr).Add(float64(c))
			}
		}(m)
	}
	wg.Wait()
	log.Info("Done sending message")
}

func closeConnections(mirrors []mirror, mirrorCloseDelay time.Duration) {
	for _, m := range mirrors {
		go func(m mirror) {
			if mirrorCloseDelay > 0 {
				go func() {
					_, _ = io.Copy(ioutil.Discard, m.conn)
				}()
				time.Sleep(mirrorCloseDelay)
			}
			_ = m.conn.Close()
		}(m)
	}
}

type mirrorList []string

func (l *mirrorList) String() string {
	return fmt.Sprint(*l)
}

func (l *mirrorList) Set(value string) error {
	for _, m := range strings.Split(value, ",") {
		*l = append(*l, m)
	}
	return nil
}

func reportDifference(new []string, old []string, oSet mapset.Set) (nSet mapset.Set) {
	nSet = mapset.NewSet()
	for _, n := range new {
		nSet.Add(n)
		if !oSet.Contains(n) {
			log.Infof("mirror address added '%v'", n)
			bytesSent.WithLabelValues(n).Add(0)
			//sentErrorCount.WithLabelValues(n).Add(0)
			//mirrorsConnCount.WithLabelValues("success", n).Add(0)
			//mirrorsConnCount.WithLabelValues("fail", n).Add(0)
		}
	}
	for _, o := range old {
		if !nSet.Contains(o) {
			log.Infof("mirror address removed '%v'", o)
			bytesSent.DeleteLabelValues(o)
			sentErrorCount.DeleteLabelValues(o)
			mirrorConnLatencyMs.DeleteLabelValues(o)
			mirrorConnCount.DeleteLabelValues("success", o)
			mirrorConnCount.DeleteLabelValues("fail", o)
		}
	}
	return
}

func removeEmptyAddr(addresses []string) (newList []string) {
	newList = addresses[:0]
	for _, addr := range addresses {
		if addr != "" {
			newList = append(newList, strings.TrimSpace(addr))
		}
	}
	return
}

func fetchDownstreams() {
	addressStore := mapset.NewSet()
	for {
		response, err := http.Get(downstreamURL)
		if err != nil {
			log.Errorf("error while connecting to downstreamURL: %s", downstreamURL)
		} else {
			func() {
				defer response.Body.Close()
				if response.StatusCode == 200 {
					contents, err := ioutil.ReadAll(response.Body)
					if err != nil {
						log.Fatal(err)
					}
					oldAddresses := mirrorAddresses
					newAddresses := removeEmptyAddr(strings.Split(string(contents), "\n"))
					addressStore = reportDifference(newAddresses, oldAddresses, addressStore)
					mirrorAddressesLock.Lock()
					mirrorAddresses = newAddresses
					mirrorAddressesLock.Unlock()
				} else {
					log.Warnf("DownstreamURL %s may not available at this moment", downstreamURL)
				}
			}()
		}
		time.Sleep(5 * time.Second)
	}
}

func RunMetricServer(addr string) {
	log.Infof("running pprof and metrics server at: %s", addr)
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(addr, nil)
	log.Error(err)
}

func main() {
	flag.BoolVar(&printVer, "V", false, "print version info")
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.StringVar(&metricServerAddr, "metrics-addr", ":9090", "pprof & metrics server listen address (e.g. ':9090')")
	flag.DurationVar(&connectTimeout, "t", 1*time.Second, "mirror connect timeout")
	flag.DurationVar(&delay, "d", 1*time.Second, "delay connecting to mirror after unsuccessful attempt")
	flag.DurationVar(&writeTimeout, "wt", 100*time.Millisecond, "mirror write timeout")
	flag.DurationVar(&mirrorCloseDelay, "mt", 0, "mirror conn close delay")
	flag.StringVar(&downstreamURL, "s", "", "URL for downstream IP list text file (e.g. http://a.com/ip.txt")
	flag.IntVar(&connectionRetryCount, "rc", 3, "mirror conn retry count")
	flag.DurationVar(&incrementConnectTimeout, "pt", 1*time.Second, "increment connect timeout by this duration for every retry")
	flag.StringVar(&logLevel, "log-level", "info", "level of logging (panic, fatal, error, warning, info, debug, trace)")
	flag.Parse()

	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Error("unknown level")
		level = log.InfoLevel
	}
	log.SetLevel(level)

	if printVer {
		printVersion()
		return
	}

	if listenAddress == "" {
		flag.Usage()
		return
	}

	log.Infof("Listening for upstream at %s", listenAddress)
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("Error while listening: %s", err)
	}

	log.Infof("Downstream URL is %s", downstreamURL)
	if downstreamURL == "" {
		flag.Usage()
		return
	}

	// with max size of 2048
	msgHashStore, _ = lru.New(2048)

	// routine that gets the latest updates of mirror address every 10 sec
	// We always replace all existing addresses with new ones read.
	go fetchDownstreams()

	go RunMetricServer(metricServerAddr)

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalf("Error while accepting: %s", err)
		}

		log.Infof("accepted upstream connection (%s <-> %s)", c.RemoteAddr(), c.LocalAddr())

		go func(c net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					log.WithField("recover", r).WithField("rAddr", c.RemoteAddr().String()).Error("Error while processing message")
					fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
					processedMsgCount.WithLabelValues("fail").Inc()
				}
			}()

			defer c.Close()

			msg, err := ioutil.ReadAll(c)
			if err != nil {
				log.WithError(err).Error("Error while receiving message")
				processedMsgCount.WithLabelValues("fail").Inc()
				return
			}

			if len(msg) <= 0 {
				log.Error("Received empty message")
				return
			}
			bytesReceived.WithLabelValues(c.RemoteAddr().(*net.TCPAddr).IP.String()).Add(float64(len(msg)))

			// Get hash of message
			hash := crc32.ChecksumIEEE(msg)
			// Check if hash already existed in hashstore.
			if msgHashStore.Contains(hash) {
				log.Infof("Ignoring duplicate broadcast message - len: %d, hash: %X", len(msg), hash)
				msgIgnoredCount.Inc()
				msgIgnoredSize.Add(float64(len(msg)))
				return
			}
			msgHashStore.Add(hash, None)
			msgHashStoreSize.Set(float64(msgHashStore.Len()))

			log.Infof("Received broadcast message with len: %d, hash: %X", len(msg), hash)

			var mirrors, retryMirrors []mirror
			var localMirrorAddresses mirrorList
			mirrorAddressesLock.RLock()
			localMirrorAddresses = make(mirrorList, len(mirrorAddresses))
			copy(localMirrorAddresses, mirrorAddresses)
			mirrorAddressesLock.RUnlock()

			var retryMirrorAddresses []string
			for _, addr := range localMirrorAddresses {
				if addr == "" {
					continue
				}
				wake, ok := mirrorWake.Load(addr)
				if ok && wake.(time.Time).After(time.Now()) {
					continue
				}
				start := time.Now()
				cn, err := net.DialTimeout("tcp", addr, connectTimeout)
				if err != nil {
					log.Errorf("error while connecting to %s: %s", addr, err)
					mirrorConnCount.WithLabelValues("fail", addr).Inc()
					if strings.Contains(err.Error(), "i/o timeout") {
						retryMirrorAddresses = append(retryMirrorAddresses, addr)
					} else { // connection refused or other error
						mirrorWake.Store(addr, time.Now().Add(delay))
					}
				} else {
					mirrorWake.Delete(addr)
					mirrorConnCount.WithLabelValues("success", addr).Inc()
					mirrorConnLatencyMs.WithLabelValues(addr).Observe(float64(time.Now().Sub(start).Milliseconds()))
					mirrors = append(mirrors, mirror{
						addr:   addr,
						conn:   cn,
						closed: 0,
					})
				}
			}

			if len(mirrors) <= 0 {
				log.Error("empty mirrors list, all mirrors unavailable")
			}
			// write out message to mirrors
			forwardAndCopy(msg, mirrors)
			// close the mirror connection
			closeConnections(mirrors, mirrorCloseDelay)

			log.Infof("len(retryMirrorAddresses)=%d", len(retryMirrorAddresses))
			// retry for prev failures on getting mirror connections
			for _, addr := range retryMirrorAddresses {
				var start time.Time
				var retryCounter = 1
				var progressiveConnTimeout = connectTimeout
				for retryCounter <= connectionRetryCount {
					start = time.Now()
					cn, err := net.DialTimeout("tcp", addr, progressiveConnTimeout)
					if err != nil {
						log.Errorf("[Retry %d ] error while retrying connecting with timeout %s to %s: %s", retryCounter, progressiveConnTimeout.String(), addr, err)
						if strings.Contains(err.Error(), "i/o timeout") {
							retryCounter++
							time.Sleep(500 * time.Millisecond)
							progressiveConnTimeout += incrementConnectTimeout
							continue
						} else { // connection refused or other error
							mirrorWake.Store(addr, time.Now().Add(delay))
							break
						}
					} else {
						mirrorWake.Delete(addr)
						mirrorConnCount.WithLabelValues("success", addr).Inc()
						mirrorConnLatencyMs.WithLabelValues(addr).Observe(float64(time.Now().Sub(start).Milliseconds()))
						retryMirrors = append(retryMirrors, mirror{
							addr:   addr,
							conn:   cn,
							closed: 0,
						})
						break
					}
				}
			}

			if len(retryMirrors) > 0 {
				// write out message to mirrors
				forwardAndCopy(msg, retryMirrors)
				// close the mirror connection
				closeConnections(retryMirrors, mirrorCloseDelay)
			}
			processedMsgCount.WithLabelValues("success").Inc()
		}(c)
	}
}
