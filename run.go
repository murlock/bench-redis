package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const ACCOUNT_KEY_PREFIX = "account:"
const BUCKET_KEY_PREFIX = "bucket:"
const BUCKET_LIST_PREFIX = "buckets:"
const CONTAINER_LIST_PREFIX = "containers:"

const EXPIRE_TIME = 60 // seconds

type stats struct {
	sync.Mutex
	rt []time.Duration
}

var global stats

type options struct {
	/* standalone */
	Server string
	/* sentinel */
	MasterName    string
	SentinelAddrs string
}

func parseArgs() options {
	var opt options

	flag.StringVar(&opt.Server, "redis", "", "stand alone redis")
	flag.StringVar(&opt.MasterName, "master", "", "sentinel name")
	flag.StringVar(&opt.SentinelAddrs, "sentinel addr", "", "list of sentinel nodes")

	flag.Parse()
	return opt
}

// LoadScript is a helper to run script
func LoadScript(name string) *redis.Script {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}
	// return string(data[:])
	return redis.NewScript(string(data[:]))
}

func akey(account string) string {
	return ACCOUNT_KEY_PREFIX + account
}

func ckey(account, container string) string {
	return fmt.Sprintf("container:%s:%s", account, container)
}

func clistkey(account string) string {
	return CONTAINER_LIST_PREFIX + account
}

func blistkey(account string) string {
	return BUCKET_LIST_PREFIX + account
}

func run(cnx *redis.Client, script *redis.Script) {
	/*
		ckey = AccountBackend.ckey(account_id, name)
		keys = [self.akey(account_id), ckey, self.clistkey(account_id),
				self._bucket_prefix, self.blistkey(account_id)]
		args = [account_id, name, bucket_name, mtime, dtime,
				object_count, bytes_used, damaged_objects, missing_chunks,
				str(autocreate_account), now, EXPIRE_TIME,
				str(autocreate_container)]
	*/
	account := "murlock"
	container := "my-bucket"
	bucket := "my-bucket"

	start := time.Now()
	current := float64(start.UnixNano()) / 1e9

	/* investigate value send by event and receive by account service */
	mtime := current
	dtime := 0
	now := current

	keys := []string{
		akey(account), ckey(account, container), clistkey(account),
		BUCKET_KEY_PREFIX, blistkey(account)}
	script.Run(context.Background(), cnx, keys,
		account, container, bucket, mtime, dtime,
		10, 1000, 0, 0, "true", now, EXPIRE_TIME, "true")
}

func main() {
	opt := parseArgs()
	/* single instance */
	var rdb *redis.Client
	if len(opt.MasterName) > 0 {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    opt.MasterName,
			SentinelAddrs: strings.Split(opt.SentinelAddrs, ","),
		})
	} else if len(opt.Server) > 0 {
		rdb = redis.NewClient(&redis.Options{
			Addr: opt.Server,
		})

	}
	if rdb == nil {
		fmt.Println("Redis not initialized")
		return
	}

	script := LoadScript("update_container.lua")
	start := time.Now()
	nb := 0
	nbseconds := 5
	len := time.Duration(nbseconds) * time.Second
	for time.Since(start) < len {
		run(rdb, script)
		nb++
	}
	fmt.Printf("total update %d\n", nb)
	fmt.Printf("update per seconds: %5.2f\n", float64(nb)/float64(nbseconds))
	fmt.Printf("update tooks: %5.2f ns\n", float64(len)/float64(nb))
}
