package main

import (
	"context"
	"flag"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var peers = set.NewSet()

// limit total connections
var recvSem = semaphore.NewWeighted(128)
var forceClose uint32 = 0

var (
	dupPath      string
	seedCount    uint
	msgLen       uint
	msgCount     uint
	sendInterval time.Duration
)

func runDownstreamServer() string {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	mu := mux.NewRouter()
	mu.HandleFunc("/seeds.txt", func(w http.ResponseWriter, r *http.Request) {
		var peersStr []string
		for p := range peers.Iter() {
			peersStr = append(peersStr, p.(string))
		}
		_, err := w.Write([]byte(strings.Join(peersStr, "\n")))
		if err != nil {
			log.Fatal(err)
		}
	})
	port := l.Addr().(*net.TCPAddr).Port
	log.Infof("seed server listening on: http://0.0.0.0:%d", port)
	go func() {
		log.Fatal(http.Serve(l, mu))
	}()
	return fmt.Sprintf("http://0.0.0.0:%d/seeds.txt", port)
}

func getRandomPorts(n int) (ports []int) {
	for i := 0; i < n; i++ {
		func() {
			l, err := net.Listen("tcp", ":0")
			if err != nil {
				log.Fatal(err)
			}
			defer l.Close()
			ports = append(ports, l.Addr().(*net.TCPAddr).Port)
		}()
	}
	return
}

func runDup(path, seedUrl string) (*exec.Cmd, string, string) {
	path, err := exec.LookPath(path)
	if err != nil {
		log.Fatal("duplicator executable not found")
	}
	ports := getRandomPorts(2)
	pAddr := fmt.Sprintf("0.0.0.0:%d", ports[0])
	mAddr := fmt.Sprintf("0.0.0.0:%d", ports[1])
	dup := exec.Command(path,
		"-l", pAddr,
		"-metrics-addr", mAddr,
		"-s", seedUrl,
		"-log-level", "error",
	)
	dup.Stdout = os.Stdout
	dup.Stderr = os.Stderr
	err = dup.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("running duplicator(PID:%d) with arguments '%v'", dup.Process.Pid, dup.Args)
	runtime.SetFinalizer(dup, func(d *exec.Cmd) {
		log.Info("killing duplicator")
		_ = d.Process.Kill()
		_ = d.Wait()
	})
	return dup, pAddr, mAddr
}

func getMetrics(url string) map[string]*dto.MetricFamily {
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	parser := expfmt.TextParser{}
	metrics, err := parser.TextToMetricFamilies(res.Body)
	if err != nil {
		log.Fatal("error while parsing ")
	}
	return metrics
}

func runTest(name string, test func() string) {
	t1 := time.Now()
	result := test()
	du := time.Now().Sub(t1)
	fmt.Printf(""+
		"====== Test: %s, Took: %s ======\n%s\n"+
		"================================\n", name, du, result)
}

func checkFetchSeed(metricsUrl string) string {
	interval := 5 * time.Second
	peerName := "foo-peer"
	peers.Add(peerName)
	defer peers.Remove(peerName)
	time.Sleep(interval)
	metrics := getMetrics(metricsUrl)
	if bytesSentMetric, ok := metrics["multiplier_bytes_sent"]; ok {
		for _, m := range bytesSentMetric.Metric {
			for _, l := range m.Label {
				if *l.Name == "addr" && *l.Value == peerName {
					return "check seed register success"
				}
			}
		}
	}
	return "check seed register fail"
}

func newSeed(server string) *net.TCPListener {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	return l.(*net.TCPListener)
}

func processSeed(l *net.TCPListener, total *int64, wg *sync.WaitGroup) {
	defer wg.Done()
	defer l.Close()
	for {
		_ = recvSem.Acquire(context.Background(), 1)
		conn, err := l.Accept()
		if err != nil {
			if forceClose == 1 {
				log.Infof("seed exit: %s", l.Addr().String())
			} else {
				log.WithError(err).Errorf("seed exit: %s", l.Addr().String())
			}
			return
		}
		go func() {
			defer recvSem.Release(1)
			defer conn.Close()
			n, err := io.Copy(ioutil.Discard, conn)
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(total, n)
		}()
	}
}

func sendMsg(addr string, size, count int, totalRecv *int64, closeCh chan struct{}) {
	msg := newRingMsg(size)
	for i := 0; i < count; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			log.WithError(err).Error("fail to connect to server")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		func(c net.Conn) {
			defer c.Close()
			n, err := c.Write(msg.next())
			if err != nil {
				log.WithError(err).Error("fail to receive data")
			}
			atomic.AddInt64(totalRecv, int64(n))
		}(c)
		time.Sleep(sendInterval)
	}
	close(closeCh)
}

func main() {
	log.SetReportCaller(true)
	flag.StringVar(&dupPath, "p", "", "path of duplicator executable")
	flag.UintVar(&seedCount, "c", 3, "count of downstream seeds")
	flag.UintVar(&msgLen, "l", 4096, "length of testing message")
	flag.UintVar(&msgCount, "m", 4096, "count of messages to send")
	flag.DurationVar(&sendInterval, "i", 500*time.Microsecond, "the interval time between message sending")
	flag.Parse()
	if dupPath == "" {
		flag.Usage()
		return
	}

	seedUrl := runDownstreamServer()
	dup, pAddr, mAddr := runDup(dupPath, seedUrl)
	log.Infof("duplicator listening on %s", pAddr)
	metricsUrl := fmt.Sprintf("http://%s/metrics", mAddr)

	var seeds []*net.TCPListener
	log.Infof("Adding %d seeds to multiplier", len(seeds))
	for i := 0; i < int(seedCount); i++ {
		s := newSeed(pAddr)
		seeds = append(seeds, s)
		peers.Add(s.Addr().String())
		log.Infof("new seed at %s", s.Addr().String())
	}
	time.Sleep(4 * time.Second)

	var totalSent int64
	var totalRecv int64

	closeCh := make(chan struct{})
	seedWg := &sync.WaitGroup{}
	for _, s := range seeds {
		seedWg.Add(1)
		go processSeed(s, &totalRecv, seedWg)
	}
	start := time.Now()
	fmt.Printf("sending %d x %d bytes msg to %s\n", msgCount, msgLen, pAddr)
	go sendMsg(pAddr, int(msgLen), int(msgCount), &totalSent, closeCh)

	var lastSent int64
	var lastRecv int64

	var interval uint64 = 3
	tick := time.Tick(time.Duration(interval) * time.Second)
Loop:
	for t := range tick {
		fmt.Printf("%s sent: %s %s/s    recv: %s %s/s\n", t.Format(time.RFC3339),
			humanize.Bytes(uint64(totalSent)), humanize.Bytes(uint64(totalSent-lastSent)/interval),
			humanize.Bytes(uint64(totalRecv)), humanize.Bytes(uint64(totalRecv-lastRecv)/interval),
		)
		lastSent = totalSent
		lastRecv = totalRecv
		select {
		case <-closeCh:
			log.Info("send complete")
			time.Sleep(1)
			atomic.StoreUint32(&forceClose, 1)
			for _, s := range seeds {
				_ = s.Close()
			}
			seedWg.Wait()
			break Loop
		default:
		}
	}

	fmt.Printf("total sent: %d, recv: %d, recv/sent: %f\n",
		totalSent, totalRecv, float64(totalRecv)/float64(totalSent),
	)
	cost := time.Now().Sub(start).Seconds()
	fmt.Printf("avg speed: send: %s/s, recv: %s/s\n",
		humanize.Bytes(uint64(float64(totalSent)/cost)),
		humanize.Bytes(uint64(float64(totalRecv)/cost)),
	)
	log.Infof("all done, you can access %s to check metrics", metricsUrl)
	log.Info("press ctrl-c to abort")
	_ = dup.Wait()
}
