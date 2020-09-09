package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/go-redis/redis/v8"
)

const ACCOUNT_KEY_PREFIX = "account:"
const BUCKET_KEY_PREFIX = "bucket:"
const BUCKET_LIST_PREFIX = "buckets:"
const CONTAINER_LIST_PREFIX = "containers:"

const EXPIRE_TIME = 60 // seconds

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

	current := time.Now()

	/* investigate value send by event and receive by account service */
	mtime := current.Unix()
	dtime := 0
	now := current.Unix()

	keys := []string{
		akey(account), ckey(account, container), clistkey(account),
		BUCKET_KEY_PREFIX, blistkey(account)}
	script.Run(context.Background(), cnx, keys,
		account, container, bucket, mtime, dtime,
		10, 1000, 0, 0, "true", now, EXPIRE_TIME, "true")
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	script := LoadScript("update_container.lua")
	run(rdb, script)
}
