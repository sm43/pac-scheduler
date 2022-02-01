package sync

import (
	"fmt"
	"strings"
)

type LockName struct {
	Namespace string
	RepoName  string
}

func NewLockName(namespace, repoName string) *LockName {
	return &LockName{
		Namespace: namespace,
		RepoName:  repoName,
	}
}

func GetLockName(e *Event) (*LockName, error) {
	return NewLockName(e.Namespace, e.RepoName), nil
}

func DecodeLockName(lockName string) (*LockName, error) {
	items := strings.Split(lockName, "/")
	if len(items) < 2 {
		return nil, fmt.Errorf("invalid lock key: unknown format %s", lockName)
	}

	return &LockName{Namespace: items[0], RepoName: items[1]}, nil
}

func (ln *LockName) EncodeName() string {
	return fmt.Sprintf("%s/%s", ln.Namespace, ln.RepoName)
}
