package core

import (
	"sync"
	"fmt"
)

type JobPool struct{

	Pool  map[string]string
	Mu  *sync.Mutex
}


func (p *JobPool)WriteToPool(job Job) bool {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	fmt.Printf("poolSize : %d ",len(p.Pool))
	if p.Pool[job.Id] == "" {
		p.Pool[job.Id] = job.Time
		fmt.Printf("poolSize : %d ",len(p.Pool))
		return true

	}
	return false
}

func (p *JobPool) DelateFromPool(job Job) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	delete(p.Pool, job.Id)
}


