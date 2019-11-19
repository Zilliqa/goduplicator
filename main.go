package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize = 1024
	//SPLICE_F_MOVE     = 1
	//SPLICE_F_NONBLOCK = 2
	//SPLICE_F_MORE     = 4
	//SPLICE_F_GIFT     = 8
	//MaxUint           = ^uint(0)
	//MaxInt            = int(MaxUint >> 1)
)

type mirror struct {
	addr   string
	conn   net.Conn
	closed uint32
}

var exists = struct{}{}

type set struct {
	m map[string]struct{}
}

func NewSet() *set {
	s := &set{}
	s.m = make(map[string]struct{})
	return s
}

func (s *set) Add(value string) {
	s.m[value] = exists
}

func (s *set) Remove(value string) {
	delete(s.m, value)
}

func (s *set) Contains(value string) bool {
	_, c := s.m[value]
	return c
}

func (s *set) Clear() {
	s.m = make(map[string]struct{})
}

func (s *set) Deallocate() {
	s.m = nil
}

var writeTimeout time.Duration
var hashStore *set
var lock3 sync.RWMutex

func forwardAndCopy(message []byte, from net.Conn, mirrors []mirror) {
	_ = from // eliminate unused variable warning
	var start, c int
	var err error
	for {
		k := start + defaultBufferSize
		if k > len(message) {
			k = len(message)
		}
		for i := 0; i < len(mirrors); i++ {
			if closed := atomic.LoadUint32(&mirrors[i].closed); closed == 1 {
				continue
			}
			_ = mirrors[i].conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if c, err = mirrors[i].conn.Write(message[start:k]); err != nil {
				log.Println("Some failure")
				_ = mirrors[i].conn.Close()
				atomic.StoreUint32(&mirrors[i].closed, 1)
			}
			// log.Printf("Sent %d bytes", c)
		}
		start += c
		if c == 0 || start >= len(message) {
			log.Printf("Sent all bytes")
			break
		}
	}
}

func connect(message []byte, origin net.Conn, mirrors []mirror) {
	forwardAndCopy(message, origin, mirrors)
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

func reportDifference(new []string, old []string, oSet *set) (nSet *set) {
	nSet = NewSet()
	for _, n := range new {
		nSet.Add(n)
		if !oSet.Contains(n) {
			log.Printf("mirror address added '%v'", n)
		}
	}
	for _, o := range old {
		if !nSet.Contains(o) {
			log.Printf("mirror address removed '%v'", o)
		}
	}
	return
}

func isIPv4(addr string) bool {
	trial := net.ParseIP(addr)
	if trial.To4() == nil {
		return false
	} else {
		return true
	}
}

func lookupAddresses(host string) (addrs []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		log.Printf("fail to lookup host: %v (%v)", host, err.Error())
		return
	}
	for _, ip := range ips {
		if ip.IP.To4() != nil {
			log.Printf("resolved ip %v from %v", ip.String(), host)
			addrs = append(addrs, ip.String())
		}
	}
	return
}

func getAddressList(contents string) (newList []string) {
	lines := strings.Split(contents, "\n")
	newList = []string{}
	for _, addr := range lines {
		addr = strings.TrimSpace(addr)
		// line starts with '#' is a comment
		if addr == "" || strings.HasPrefix(addr, "#") {
			continue
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			log.Printf("invalid address %v, skip", err.Error())
			continue
		}
		if isIPv4(host) {
			newList = append(newList, addr)
		} else {
			fmt.Printf("%v is not an IPv4 address, try to resolve by DNS", host)
			for _, ip := range lookupAddresses(host) {
				newList = append(newList, fmt.Sprintf("%v:%v", ip, port))
			}
		}
	}
	return
}

func main() {
	var (
		connectTimeout          time.Duration
		delay                   time.Duration
		listenAddress           string
		mirrorAddresses         mirrorList
		useZeroCopy             bool
		mirrorCloseDelay        time.Duration
		seedurl                 string
		incrementConnectTimeout time.Duration
		connectionRetryCount    int
	)

	flag.BoolVar(&useZeroCopy, "z", false, "use zero copy")
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.DurationVar(&connectTimeout, "t", 1*time.Second, "mirror connect timeout")
	flag.DurationVar(&delay, "d", 1*time.Second, "delay connecting to mirror after unsuccessful attempt")
	flag.DurationVar(&writeTimeout, "wt", 100*time.Millisecond, "mirror write timeout")
	flag.DurationVar(&mirrorCloseDelay, "mt", 0, "mirror conn close delay")
	flag.StringVar(&seedurl, "s", "", "URL for downstream IP list text file (e.g. http://a.com/ip.txt")
	flag.IntVar(&connectionRetryCount, "rc", 3, "mirror conn retry count")
	flag.DurationVar(&incrementConnectTimeout, "pt", 1*time.Second, "increment connect timeout by this duration for every retry")

	flag.Parse()
	if listenAddress == "" {
		flag.Usage()
		return
	}

	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("error while listening: %s", err)
	}

	log.Println("URL is", seedurl)
	if seedurl == "" {
		flag.Usage()
		return
	}

	var mirrorWakeLock sync.RWMutex
	var mirrorAddressesLock sync.RWMutex
	mirrorWake := make(map[string]time.Time)
	hashStore = NewSet()

	// routine that gets the latest updates of mirror address every 10 sec
	// We always replace all existing addresses with new ones read.
	go func() {
		addressStore := NewSet()

		for {
			func() {
				response, err := http.Get(seedurl)
				if err != nil {
					log.Printf("error while connecting to seedurl: %s", seedurl)
				} else {
					defer func() { _ = response.Body.Close() }()
					if response.StatusCode == 200 {
						contents, err := ioutil.ReadAll(response.Body)
						if err != nil {
							log.Fatal(err)
						}
						oldAddresses := mirrorAddresses
						newAddresses := getAddressList(string(contents))
						oldAddressStore := addressStore
						addressStore = reportDifference(newAddresses, oldAddresses, addressStore)
						oldAddressStore.Deallocate()
						mirrorAddressesLock.Lock()
						mirrorAddresses = newAddresses
						mirrorAddressesLock.Unlock()
					} else {
						log.Printf("May be seedurl: %s is not available at the moment", seedurl)
					}
				}
				time.Sleep(5 * time.Second)
			}()
		}
	}()

	// routine that clears the hash store periodically
	go func() {
		for {
			time.Sleep(300 * time.Second)
			lock3.Lock()
			hashStore.Clear()
			log.Println("Cleared the hash-store")
			lock3.Unlock()
		}
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalf("Error while accepting: %s", err)
		}

		log.Printf("accepted upstream connection (%s <-> %s)", c.RemoteAddr(), c.LocalAddr())

		go func(c net.Conn) {
			defer func() { _ = c.Close() }()

			buf := make([]byte, 0, 4096) // big buffer
			tmp := make([]byte, defaultBufferSize)
			var n int
			var err1 error
			for {
				n, err1 = c.Read(tmp)
				if err1 != nil {
					if err1 != io.EOF {
						log.Println("read error:", err1)
					}
					break
				}
				buf = append(buf, tmp[:n]...)
			}

			if len(buf) <= 0 {
				return
			}

			// Get hash of message
			hasher := md5.New()
			hasher.Write(buf)
			hash := hex.EncodeToString(hasher.Sum(nil))

			// Check if hash already existed in hashstore.
			lock3.Lock()
			if hashStore.Contains(hash) {
				log.Printf("Ignoring duplicate broadcasted message - hash: %s", hash)
				lock3.Unlock()
				return
			}
			hashStore.Add(hash)
			lock3.Unlock()

			log.Printf("len = %d", len(buf))
			log.Printf("Received broadcasted message with hash : %s", hash)

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
				mirrorWakeLock.RLock()
				wake := mirrorWake[addr]
				mirrorWakeLock.RUnlock()
				if wake.After(time.Now()) {
					continue
				}

				cn, err := net.DialTimeout("tcp", addr, connectTimeout)
				if err != nil {
					log.Printf("error while connecting to mirror %s: %s", addr, err)
					if strings.Contains(err.Error(), "i/o timeout") {
						retryMirrorAddresses = append(retryMirrorAddresses, addr)
					} else { // connection refused or other error
						mirrorWakeLock.Lock()
						mirrorWake[addr] = time.Now().Add(delay)
						mirrorWakeLock.Unlock()
					}
				} else {
					mirrors = append(mirrors, mirror{
						addr:   addr,
						conn:   cn,
						closed: 0,
					})
				}
			}

			// write out message to mirrors
			connect(buf, c, mirrors)

			// close the mirror connection
			closeConnections(mirrors, mirrorCloseDelay)

			// retry for prev failures on getting mirror connections
			for _, addr := range retryMirrorAddresses {
				var retryCounter = 1
				var progressiveConnTimeout = connectTimeout
				for retryCounter <= connectionRetryCount {
					cn, err := net.DialTimeout("tcp", addr, progressiveConnTimeout)
					if err != nil {
						log.Printf("[Retry %d ] error while retrying connecting to mirror %s: %s", retryCounter, addr, err)
						if strings.Contains(err.Error(), "i/o timeout") {
							retryCounter++
							time.Sleep(500 * time.Millisecond)
							progressiveConnTimeout += incrementConnectTimeout
							continue
						} else { // connection refused or other error
							mirrorWakeLock.Lock()
							mirrorWake[addr] = time.Now().Add(delay)
							mirrorWakeLock.Unlock()
							break
						}
					} else {
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
				connect(buf, c, retryMirrors)

				// close the mirror connection
				closeConnections(retryMirrors, mirrorCloseDelay)
			}
		}(c)
	}
}
