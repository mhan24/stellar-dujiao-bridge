// Enqueue only Dujiao's existing price and inventory reconciliation task.
package main

import (
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/hibiken/asynq"
)

func main() {
	db, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		log.Fatal("invalid REDIS_DB")
	}
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: os.Getenv("REDIS_ADDR"), Password: os.Getenv("REDIS_PASSWORD"), DB: db})
	defer client.Close()
	_, err = client.Enqueue(asynq.NewTask("upstream:sync_stock", nil),
		asynq.Queue("default"), asynq.Unique(55*time.Second), asynq.MaxRetry(1),
		asynq.Timeout(45*time.Second), asynq.Retention(2*time.Minute))
	if errors.Is(err, asynq.ErrDuplicateTask) {
		log.Print("sync already queued")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Print("price and inventory sync queued")
}
