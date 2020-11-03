package main

import (
	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"testing"
)

const msgSize = 32
const lruSize = 2048 * 16

func BenchmarkRingMsgNoCollision(b *testing.B) {
	cache, _ := lru.New(lruSize)
	r := newRingMsg(msgSize)
	for i := 0; i < b.N; i++ {
		msg := r.next()
		if cache.Contains(string(msg)) {
			log.WithField("n", i).WithField("msg", msg).Fatal("collision found")
		}
		cache.Add(string(msg), nil)
	}
}

func BenchmarkRingMsg(b *testing.B) {
	r := newRingMsg(64)
	for i := 0; i < b.N; i++ {
		_ = r.next()
	}
}

func BenchmarkRandMsgNoCollision(b *testing.B) {
	cache, _ := lru.New(lruSize)
	msg := make([]byte, msgSize)
	for i := 0; i < b.N; i++ {
		_, _ = rand.Read(msg)
		if cache.Contains(string(msg)) {
			log.WithField("n", i).WithField("msg", msg).Fatal("collision found")
		}
		cache.Add(string(msg), nil)
	}
}

func BenchmarkRandMsg(b *testing.B) {
	msg := make([]byte, 64)
	for i := 0; i < b.N; i++ {
		_, _ = rand.Read(msg)
	}
}
