package sync

import "time"

type Semaphore interface {
	acquireForLatest(holderKey string) string
	acquire(holderKey string) bool
	tryAcquire(holderKey string) (bool, string)
	release(key string) bool
	addToQueue(holderKey string, priority int32, creationTime time.Time)
	removeFromQueue(holderKey string)
	getCurrentHolders() []string
	getCurrentPending() []string
	getName() string
	getLimit() int
	resize(n int) bool
	watcherExist() bool
	setWatcher(b bool)
}

type Event struct {
	PipelineRunName string
	RepoName        string
	Namespace       string
	EventTime       time.Time
}
