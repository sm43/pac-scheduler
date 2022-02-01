package sync

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/apis"
)

type (
	GetSyncLimit func(string) (int, error)
	IsSDDeleted  func(string) bool
)

type Manager struct {
	syncLockMap  map[string]Semaphore
	lock         *sync.Mutex
	getSyncLimit GetSyncLimit
	isSDDeleted  IsSDDeleted
}

func NewLockManager(getSyncLimit GetSyncLimit, isSDDeleted IsSDDeleted) *Manager {
	return &Manager{
		syncLockMap:  make(map[string]Semaphore),
		lock:         &sync.Mutex{},
		getSyncLimit: getSyncLimit,
		isSDDeleted:  isSDDeleted,
	}
}

func (m *Manager) Print(e *Event) {

	syncLockName, _ := GetLockName(e)
	lockKey := syncLockName.EncodeName()
	b := m.syncLockMap[lockKey]
	log.Println("*****************************")
	log.Println("----name ", b.getName())
	log.Println("----limit ", b.getLimit())
	log.Println("----holder ", b.getCurrentHolders())
	log.Println("----pending ", b.getCurrentPending())

}

func (m *Manager) Register(e *Event, pipelineCS versioned.Interface) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	syncLockName, err := GetLockName(e)
	if err != nil {
		return false, fmt.Errorf("getlockname: requested configuration is invalid: %w", err)
	}

	lockKey := syncLockName.EncodeName()

	lock, found := m.syncLockMap[lockKey]
	if !found {
		lock, err = m.initializeSemaphore(lockKey)
		if err != nil {
			return false, fmt.Errorf("failed to init semaphore")
		}
		m.syncLockMap[lockKey] = lock
	}

	// if limit changed the update size of semaphore
	err = m.checkAndUpdateSemaphoreSize(lock)
	if err != nil {
		log.Println("failed to update semaphore size")
		return false, err
	}

	holderKey := getHolderKey(e)

	priority := int32(1)
	creationTime := e.EventTime

	// add to the Q
	lock.addToQueue(holderKey, priority, creationTime)

	// start a watcher for the repo

	go checkWatcher(m, lockKey, pipelineCS)

	return false, err
}

func checkWatcher(cm *Manager, lockKey string, pipelineCS versioned.Interface) {

	// check if watcher already exist
	cm.lock.Lock()
	defer cm.lock.Unlock()

	log.Println("watcher : ", cm.syncLockMap[lockKey].watcherExist())
	if !cm.syncLockMap[lockKey].watcherExist() {
		go func() {
			err := startWatcher(cm, lockKey, pipelineCS)

			log.Println("i m waiting u know... ^.^")
			if err != nil {
				// do something
			}
			cm.lock.Lock()
			defer cm.lock.Unlock()
			// mark watcher as false
			cm.syncLockMap[lockKey].setWatcher(false)
		}()
		// mark watcher as true
		cm.syncLockMap[lockKey].setWatcher(true)
	}
}

func startWatcher(cm *Manager, lockKey string, pipelineCS versioned.Interface) error {
	for {
		log.Println("************************************************")
		log.Println("************************************************")
		log.Println("I am a thread for ", lockKey)
		repo := cm.syncLockMap[lockKey]

		r, _ := DecodeLockName(lockKey)
		// check if the holders are complete and if yes then remove from Q
		holders := repo.getCurrentHolders()
		log.Println("first holders : ", holders)

		penders := repo.getCurrentPending()
		log.Println("current penders : ", penders)

		// check if holders are complete and
		// if yes then remove them
		for _, pr := range holders {
			log.Println("-----------------------------------------------change status for : ", pr)

			prname := strings.Split(pr, "/")
			pipelineRun, err := pipelineCS.TektonV1beta1().PipelineRuns(r.Namespace).Get(context.Background(), prname[1], v1.GetOptions{})
			if err != nil {
				log.Printf("failed to get pipelinerun %v : %v", pr, err)
				return err
			}

			// check if is completed, if yes then remove from holder Q
			succeeded := pipelineRun.Status.GetCondition(apis.ConditionSucceeded)
			if succeeded == nil {
				continue
			}
			if succeeded.Status == corev1.ConditionTrue || succeeded.Status == corev1.ConditionFalse {
				// pipelinerun has been succeeded so can be removed from Q
				cm.Release(lockKey, pr)
			}
		}
		// after checking all now check if the limit has been reached or move new pending from Q

		holders = repo.getCurrentHolders()
		log.Println("updated holders : ", holders)

		penders = repo.getCurrentPending()
		log.Println("updated penders : ", penders)

		limit := repo.getLimit()

		log.Println("holders vs limit : ", len(holders), limit)

		if len(repo.getCurrentPending()) == 0 && len(repo.getCurrentHolders()) == 0 {
			// all pipelinerun are completed
			// we can return
			return nil
		}

		for len(repo.getCurrentHolders()) < limit {
			log.Println("limit is not reached so I can start new...")

			// move the top most from Q to running
			ready := repo.acquireForLatest(lockKey)

			log.Println("fetching pipelinerun ..")
			prname := strings.Split(ready, "/")
			// fetch and update pipelinerun by removing pending
			tobeUpdated, err := pipelineCS.TektonV1beta1().PipelineRuns(r.Namespace).Get(context.Background(), prname[1], v1.GetOptions{})
			if err != nil {
				log.Printf("failed to get pipelinerun %v : %v", ready, err)
				return err
			}

			tobeUpdated.Spec.Status = ""

			_, err = pipelineCS.TektonV1beta1().PipelineRuns(r.Namespace).Update(context.Background(), tobeUpdated, v1.UpdateOptions{})
			if err != nil {
				log.Printf("failed to update pipelinerun %v : %v", tobeUpdated.Name, err)
				return err
			}
		}

		log.Println("************************************************")
		log.Println("************************************************")
		time.Sleep(10 * time.Second)
	}
}

func (cm *Manager) TryAcquire(sd *Event) (bool, string, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	// Each Namespace/RepoName has a Queue
	// The limit comes from repo

	syncLockName, err := GetLockName(sd)
	if err != nil {
		return false, "", fmt.Errorf("getlockname: requested configuration is invalid: %w", err)
	}

	// namespace/repoName
	lockKey := syncLockName.EncodeName()

	// lock is the semaphore which manages priority Q for the repo
	lock, found := cm.syncLockMap[lockKey]
	if !found {
		lock, err = cm.initializeSemaphore(lockKey)
		if err != nil {
			return false, "", fmt.Errorf("failed to init semaphore")
		}
		cm.syncLockMap[lockKey] = lock
	}

	// if limit changed the update size of semaphore
	err = cm.checkAndUpdateSemaphoreSize(lock)
	if err != nil {
		log.Println("failed to update semaphore size")
		return false, "", err
	}

	holderKey := getHolderKey(sd)

	//var priority int32
	//if wf.Spec.Priority != nil {
	//	priority = *wf.Spec.Priority
	//} else {
	//	priority = 0
	//}
	// setting priority as const for now, visit again
	priority := int32(1)
	creationTime := sd.EventTime

	// add to the Q
	lock.addToQueue(holderKey, priority, creationTime)

	//currentHolders := cm.getCurrentLockHolders(lockKey)

	acquired, msg := lock.tryAcquire(holderKey)
	if acquired {
		log.Println("Yay! I got it ...")
		return true, "", nil
	}

	return false, msg, nil
}

func (cm *Manager) Release(lockKey, holderKey string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if syncLockHolder, ok := cm.syncLockMap[lockKey]; ok {
		log.Printf("%s sync lock is getting released by %s", lockKey, holderKey)
		syncLockHolder.release(holderKey)
		syncLockHolder.removeFromQueue(holderKey)
		log.Printf("%s sync lock is released by %s", lockKey, holderKey)
	}

	//if sd == nil {
	//	return
	//}
	//
	//cm.lock.Lock()
	//defer cm.lock.Unlock()
	//
	//holderKey := getHolderKey(sd)
	//lockName, err := GetLockName(sd)
	//if err != nil {
	//	return
	//}
	//
	//if syncLockHolder, ok := cm.syncLockMap[lockName.EncodeName()]; ok {
	//	syncLockHolder.release(holderKey)
	//	syncLockHolder.removeFromQueue(holderKey)
	//	log.Printf("%s sync lock is released by %s", lockName.EncodeName(), holderKey)
	//}
}

func getHolderKey(sd *Event) string {
	if sd == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s", sd.Namespace, sd.PipelineRunName)
}

func (cm *Manager) getCurrentLockHolders(lockName string) []string {
	if concurrency, ok := cm.syncLockMap[lockName]; ok {
		return concurrency.getCurrentHolders()
	}
	return nil
}

func (cm *Manager) initializeSemaphore(semaphoreName string) (Semaphore, error) {
	limit, err := cm.getSyncLimit(semaphoreName)
	if err != nil {
		return nil, err
	}
	return NewSemaphore(semaphoreName, limit), nil
}

func (cm *Manager) isSemaphoreSizeChanged(semaphore Semaphore) (bool, int, error) {
	limit, err := cm.getSyncLimit(semaphore.getName())
	if err != nil {
		return false, semaphore.getLimit(), err
	}
	return semaphore.getLimit() != limit, limit, nil
}

func (cm *Manager) checkAndUpdateSemaphoreSize(semaphore Semaphore) error {
	changed, newLimit, err := cm.isSemaphoreSizeChanged(semaphore)
	if err != nil {
		return err
	}
	if changed {
		semaphore.resize(newLimit)
	}
	return nil
}
