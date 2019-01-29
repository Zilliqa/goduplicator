package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"net/http"
	"crypto/md5"
	"encoding/hex"
)

const (
	defaultBufferSize = 1024
	SPLICE_F_MOVE     = 1
	SPLICE_F_NONBLOCK = 2
	SPLICE_F_MORE     = 4
	SPLICE_F_GIFT     = 8
	MaxUint           = ^uint(0)
	MaxInt            = int(MaxUint >> 1)
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

var writeTimeout time.Duration
var hashStore *set
var lock3 sync.RWMutex

func forwardAndCopy(message []byte, from net.Conn, mirrors []mirror) {
	var start,c int
	var err error
	for {
		k := start + defaultBufferSize
		if (k > len(message)){
			k = len(message)
		}
		for i := 0; i < len(mirrors); i++ {
			if closed := atomic.LoadUint32(&mirrors[i].closed); closed == 1 {
				continue
			}
			mirrors[i].conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if c, err = mirrors[i].conn.Write(message[start:k]); err != nil {
				log.Println("Some failure")
				mirrors[i].conn.Close()
				atomic.StoreUint32(&mirrors[i].closed, 1)
			}	
			log.Printf("Sent %d bytes", c)
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

func removeEmptyAddr(addresses []string) (newList []string) {
	newList = addresses[:0]
	for _, addr := range addresses {
		if addr != "" {
			newList = append(newList, addr)
		}
	}
	return
}

func main() {
	var (
		connectTimeout   time.Duration
		delay            time.Duration
		listenAddress    string
		mirrorAddresses mirrorList
		useZeroCopy      bool
		mirrorCloseDelay time.Duration
		seedurl          string
	)

	flag.BoolVar(&useZeroCopy, "z", false, "use zero copy")
	flag.StringVar(&listenAddress, "l", "", "listen address (e.g. 'localhost:8080')")
	flag.DurationVar(&connectTimeout, "t", 500*time.Millisecond, "mirror connect timeout")
	flag.DurationVar(&delay, "d", 1*time.Second, "delay connecting to mirror after unsuccessful attempt")
	flag.DurationVar(&writeTimeout, "wt", 20*time.Millisecond, "mirror write timeout")
	flag.DurationVar(&mirrorCloseDelay, "mt", 0, "mirror conn close delay")
	flag.StringVar(&seedurl, "s", "", "URL for downstream IP list text file (e.g. http://a.com/ip.txt")

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

	var lock sync.RWMutex
	var lock2 sync.RWMutex
	mirrorWake := make(map[string]time.Time)
	hashStore = NewSet()

	// routine that gets the latest updates of mirror address every 10 sec
	// We always replace all existing addresses with new ones read.
	go func() {
		addressStore := NewSet()

		for {
			response, err := http.Get(seedurl)
			if err != nil {
				log.Printf("error while connecting to seedurl: %s", seedurl)
			} else {
				defer response.Body.Close()
				if response.StatusCode == 200 {
					contents, err := ioutil.ReadAll(response.Body)
					if err != nil {
							log.Fatal(err)
					}
					oldAddresses := mirrorAddresses
					newAddresses := removeEmptyAddr(strings.Split(string(contents),"\n"))
					addressStore = reportDifference(newAddresses, oldAddresses, addressStore)
					lock2.Lock()
					mirrorAddresses = newAddresses
					lock2.Unlock()
				} else {
					log.Printf("May be seedurl: %s is not available at the moment", seedurl)
				}
			}
			time.Sleep(5*time.Second)
		}
	}()

	// routine that clears the hash store periodically
	go func() {
		for {
			time.Sleep(300*time.Second)
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
			defer c.Close()

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
		
			if(len(buf) <= 0){
				return
			}
			log.Printf("len = %d", len(buf))
		
			// Get hash of message
			hasher := md5.New()
			hasher.Write(buf)
			hash := hex.EncodeToString(hasher.Sum(nil))
		
			// Check if hash already existed in hashstore.
			lock3.Lock()
			if (hashStore.Contains(hash)) { 
				log.Printf("Ignoring duplicate broadcasted message - hash: %s" , hash )
				lock3.Unlock()
				return
			}
			hashStore.Add(hash)
			lock3.Unlock()
		
			log.Printf("Received broadcasted message with hash : %s", hash)
			
			var mirrors []mirror
			var localMirrorAddresses mirrorList			
			lock2.RLock()
			localMirrorAddresses = make(mirrorList, len(mirrorAddresses))
			copy(localMirrorAddresses, mirrorAddresses)
			lock2.RUnlock()
			
			for _, addr := range localMirrorAddresses {
				if addr == "" {
					continue
				}
				lock.RLock()
				wake := mirrorWake[addr]
				lock.RUnlock()
				if wake.After(time.Now()) {
					continue
				}

				cn, err := net.DialTimeout("tcp", addr, connectTimeout)
				if err != nil {
					log.Printf("error while connecting to mirror %s: %s", addr, err)
					lock.Lock()
					mirrorWake[addr] = time.Now().Add(delay)
					lock.Unlock()
				} else {
					mirrors = append(mirrors, mirror{
						addr:   addr,
						conn:   cn,
						closed: 0,
					})
				}
			}

			connect(buf, c, mirrors)

			for _, m := range mirrors {
				go func(m mirror) {
					if mirrorCloseDelay > 0 {
						go func() {
							io.Copy(ioutil.Discard, m.conn)
						}()
						time.Sleep(mirrorCloseDelay)
					}
					m.conn.Close()
				}(m)
			}
		}(c)
	}
}
