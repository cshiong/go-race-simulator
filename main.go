package main

import (
	"code.comcast.com/VariousArtists/csv-context-go"
	"flag"
	"github.com/pborman/uuid"
	"github.comcast.com/VariousArtists/go-race-simulator/core"
	"os"

	"math/rand"
	"strconv"
	"sync"
	"time"
)

var jobChannel chan core.Job
var pool core.JobPool
var dCount duplicateCount

func main() {

	// =================================================
	// Set up arguments for the app
	// =================================================

	producers := flag.Int("producers", 10, "number of producers to put data into the pool")
	producerTime := flag.Int("pTime", 1, "interval(second) that producers put data into the pool")
	consumers := flag.Int("consumers", 100, "number of consumers to consumer the data in the pool")
	consumerTime := flag.Int("cTime",400, "maximun consumer execution time second")
	last := flag.Int("last", 300, "how many second to run this program")
	//limit := flag.Int("limit", 10000, "how many jobs to be created")
	dublicate := flag.Bool("dublicate", false, "allow dublicate jobs")
	flag.Parse()

	// =================================================
	// Get Flags, Set State, Run Service
	// =================================================
	jobChannel = make(chan core.Job,100)

	dCount = duplicateCount{
		count: 0,
		mu:    &sync.Mutex{},
	}

	if *dublicate == false {
		pool = core.JobPool{
			Pool: make(map[string]string,100),
			Mu:   &sync.Mutex{},
		}
	}

	ctx := context.NewContext(os.Stdout)
	db, err := core.NewDynamoDB(ctx, "local", *producers)
	if err != nil {
		ctx.Log.Error(err.Error())
		os.Exit(2)
	}

	dG := Worker{
		ctx:    ctx.SubContext(),
		number: *last,
		name:   "Generator",
		mu:     &sync.Mutex{},
	}

	prod := Worker{
		ctx:    ctx.SubContext(),
		number: *producers,
		name:   "Producer",
		mu:     &sync.Mutex{},
		count:  0,
	}

	c := Worker{
		ctx:    ctx.SubContext(),
		number: *consumers,
		name:   "Consumer",
		mu:     &sync.Mutex{},
		count:  0,
	}

	//go dG.DataGeneratorBynNum(db, *limit)
	go dG.DataGeneratorByTime(db)
	//time.Sleep(time.Duration(5)*time.Second)
	prod.Producer(db, *producerTime, *dublicate)
	c.Consumer(db, *consumerTime, *dublicate)
	defer db.DeleteTable()
	time.Sleep(time.Duration(*last) * time.Second)
	ctx.Logger().Info("job created", dG.count, "produced job", prod.count, "consumered jobs", c.count, "duplicateFound", dCount.count)
}

type duplicateCount struct {
	count int
	mu    *sync.Mutex
}

func (d *duplicateCount) increaseCount() {
	d.mu.Lock()
	d.count++
	d.mu.Unlock()

}

type Worker struct {
	ctx    *context.Context
	number int
	name   string
	count  int
	mu     *sync.Mutex
}



func (c *Worker) Consumer(d *core.DynamoDB, delay int, dublicate bool) {
	for i := 0; i < c.number; i++ {
		go func(w int) {
			c.ctx.Logger().Info("Worker", strconv.Itoa(w), "system", c.name)
			for {
				job := <-jobChannel
				executionTime := time.Duration(rand.Int31n(int32(delay))) * time.Millisecond
				time.Sleep(executionTime)
				d.Delete(job)
				if !dublicate {
					c.ctx.Logger().Info("op", "deleteFromPool", "job", job.Id)
					pool.DelateFromPool(job)
				}
				c.IncreaseCount()
			}
		}(i)
	}
}
func (c *Worker) Producer(d *core.DynamoDB, interval int, dublicate bool) {

	for i := 0; i < c.number; i++ {
		go func(w int) {
			c.ctx.Logger().Info("Worker", strconv.Itoa(w), "system", c.name)
			for {

				jobs, err := d.Get(strconv.Itoa(w))
				if err == nil {
					for _, job := range jobs {
						c.IncreaseCount()
						if !dublicate {
							write := pool.WriteToPool(job)
							c.ctx.Logger().Info("op", "writeToPool", "job", job.Id, "uniqual", write)

							if !write {
								dCount.increaseCount()
								continue
							}

						}
						c.ctx.Logger().Info("op", "writeToChannel", "job", job.Id)
						jobChannel <- job

					}
				}
				time.Sleep(time.Duration(interval) * time.Second)
			}
		}(i)
	}

}

/*
func (c *Worker) DataGeneratorV1(d *core.DynamoDB) {

	for i := 0; i < c.number; i++ {
		go c.DataGenerator(d)
	}

}
*/

func (c *Worker) DataGeneratorByTime(d *core.DynamoDB) {
	timeout := time.After(time.Duration(c.number) * time.Second)
	// Keep trying until we're timed out
	for {
		select {
		// Got a timeout! fail with a timeout error
		case <-timeout:
			return
		// Got a tick, we should check on doSomething()
		default:

			id := uuid.New()

			//c.ctx.Logger().Info("system", c.name, "newJob", id)
			d.Insert(id)
			c.IncreaseCount()

		}
	}

}

func (c *Worker) DataGeneratorBynNum(d *core.DynamoDB, num int) {

	for i := 0; i < num; i++ {

		id := uuid.New()
		//c.ctx.Logger().Info("system", c.name, "newJob", id)
		d.Insert(id)
		c.IncreaseCount()

	}
}

func (c *Worker) IncreaseCount() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}
