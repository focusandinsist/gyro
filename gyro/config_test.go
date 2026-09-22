package gyro

import (
	"sync"
	"testing"
	"time"
)

func TestConfigManagerConcurrentUpdatesAreSerialized(t *testing.T) {
	initial := DefaultConfig()
	initial.Locator.PartitionCount = 100
	manager := NewConfigManager(initial)

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	manager.AddConfigWatcher(func(_, next *Config) error {
		switch next.Locator.PartitionCount {
		case 101:
			close(firstEntered)
			<-releaseFirst
		case 102:
			close(secondEntered)
			<-releaseSecond
		}
		return nil
	})

	first := *initial
	first.Locator.PartitionCount = 101
	second := *initial
	second.Locator.PartitionCount = 102

	var workers sync.WaitGroup
	workers.Add(2)
	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() {
		defer workers.Done()
		firstErr <- manager.UpdateConfig(&first)
	}()

	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first update did not enter its watcher")
	}

	go func() {
		defer workers.Done()
		secondErr <- manager.UpdateConfig(&second)
	}()

	close(releaseFirst)
	if err := <-firstErr; err != nil {
		t.Fatalf("first update failed: %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second update did not run after the first update committed")
	}
	close(releaseSecond)
	workers.Wait()
	if err := <-secondErr; err != nil {
		t.Fatalf("second update failed: %v", err)
	}
	if got := manager.GetConfig().Locator.PartitionCount; got != 102 {
		t.Fatalf("final partition count = %d, want 102", got)
	}
}
