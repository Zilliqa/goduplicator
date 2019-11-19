package main

import (
	"context"
	"fmt"
	"net"
	"time"
)

func defaultLookup(){
	ips, err := net.LookupIP("seed.bitnodes.io")
	if err != nil {
		fmt.Println(err)
	}
	for _, ip := range ips {
		fmt.Println(ip.String())
	}
}

func lookupWithTimeout(){
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Microsecond)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, "seed.bitnodes.io")
	if err != nil {
		fmt.Println(err)
	}
	for _, ip := range ips {
		fmt.Println(ip.String())
	}
}

func main() {
	defaultLookup()
	lookupWithTimeout()
}
