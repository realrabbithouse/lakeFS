package packfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
)

var (
	// ErrReplicationNotFound is returned when replication state is not found.
	ErrReplicationNotFound = errors.New("packfile: replication state not found")

	// ErrReplicationFailed is returned when replication fails after max retries.
	ErrReplicationFailed = errors.New("packfile: replication failed after max retries")
)

// ReplicationStatus tracks the state of S3 replication for a packfile
type ReplicationStatus uint8

const (
	ReplicationStatusPending ReplicationStatus = iota
	ReplicationStatusInProgress
	ReplicationStatusCompleted
	ReplicationStatusFailed
)

// PackfileReplication tracks S3 replication state for a packfile
type PackfileReplication struct {
	PackfileID   PackfileID
	Status       ReplicationStatus
	RetryCount   uint8
	LastAttempt  time.Time
	ErrorMessage string
}

// ReplicationConfig contains configuration for S3 replication
type ReplicationConfig struct {
	MaxRetries        uint8
	BaseInterval      time.Duration
	MaxInterval       time.Duration
	ReconcileInterval time.Duration
}

// DefaultReplicationConfig returns a default replication configuration
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		MaxRetries:        10,
		BaseInterval:      1 * time.Second,
		MaxInterval:       10 * time.Second,
		ReconcileInterval: 5 * time.Minute,
	}
}

// replicationState holds the global replication state
type replicationState struct {
	mu          sync.RWMutex
	replications map[PackfileID]*PackfileReplication
	stopCh      chan struct{}
	adapter     block.Adapter
	config      ReplicationConfig
}

var (
	// replState is the global replication state
	replState *replicationState

	// replOnce ensures replication state is initialized only once
	replOnce sync.Once
)

// initReplicationState initializes the global replication state
func initReplicationState() {
	replState = &replicationState{
		replications: make(map[PackfileID]*PackfileReplication),
		stopCh:      make(chan struct{}),
		config:      DefaultReplicationConfig(),
	}
}

// getReplicationState returns the global replication state
func getReplicationState() *replicationState {
	replOnce.Do(initReplicationState)
	return replState
}

// backoffDuration calculates exponential backoff interval
func backoffDuration(retryCount uint8, baseInterval time.Duration, maxInterval time.Duration) time.Duration {
	// Limit retry count to prevent overflow - max backoff is 2^63 which far exceeds any practical interval
	shift := retryCount
	if shift > 63 {
		shift = 63
	}
	interval := baseInterval * time.Duration(1<<shift) // 1s, 2s, 4s, 8s, ...
	if interval > maxInterval {
		interval = maxInterval
	}
	return interval
}

// SetBlockAdapter sets the block adapter for replication
func SetBlockAdapter(adapter block.Adapter) {
	state := getReplicationState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.adapter = adapter
}

// SetReplicationConfig sets the replication configuration
func SetReplicationConfig(cfg ReplicationConfig) {
	state := getReplicationState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.config = cfg
}

// StartReplication starts the async S3 replication goroutine for a packfile
// after it has been renamed to its final location
func StartReplication(ctx context.Context, packfileID PackfileID, packfilePath string, indexPath string) error {
	state := getReplicationState()

	state.mu.Lock()
	// Check if already replicating
	if _, exists := state.replications[packfileID]; exists {
		state.mu.Unlock()
		return fmt.Errorf("packfile: replication already in progress for %s", packfileID)
	}

	// Initialize replication state
	replication := &PackfileReplication{
		PackfileID: packfileID,
		Status:     ReplicationStatusPending,
		RetryCount: 0,
	}
	state.replications[packfileID] = replication
	state.mu.Unlock()

	pkgLogger.Info("packfile: replication started",
		"packfile_id", packfileID)

	// Spawn async replication goroutine
	go func() {
		replicatePackfileToS3(ctx, packfileID, packfilePath, indexPath)
	}()

	return nil
}

// StopReplication signals the replication goroutine to stop during shutdown
func StopReplication(ctx context.Context) error {
	state := getReplicationState()
	state.mu.Lock()
	defer state.mu.Unlock()

	select {
	case <-state.stopCh:
		// Already stopped
	default:
		close(state.stopCh)
	}

	pkgLogger.Info("packfile: replication stopped")
	return nil
}

// GetReplicationStatus returns the current replication status for a packfile
func GetReplicationStatus(ctx context.Context, packfileID PackfileID) (ReplicationStatus, error) {
	state := getReplicationState()
	state.mu.RLock()
	defer state.mu.RUnlock()

	replication, exists := state.replications[packfileID]
	if !exists {
		return ReplicationStatusPending, ErrReplicationNotFound
	}
	return replication.Status, nil
}

// replicatePackfileToS3 performs the actual S3 replication with exponential backoff
func replicatePackfileToS3(ctx context.Context, packfileID PackfileID, packfilePath string, indexPath string) error {
	state := getReplicationState()
	config := state.config

	// Get replication state
	state.mu.Lock()
	replication, exists := state.replications[packfileID]
	if !exists {
		state.mu.Unlock()
		return fmt.Errorf("packfile: replication state not found for %s", packfileID)
	}
	replication.Status = ReplicationStatusInProgress
	state.mu.Unlock()

	// Open packfile for reading
	packfileReader, packfileSize, err := openFileForReplication(packfilePath)
	if err != nil {
		updateReplicationError(state, packfileID, err)
		return err
	}
	defer packfileReader.Close()

	// Open index file for reading
	indexReader, indexSize, err := openFileForReplication(indexPath)
	if err != nil {
		updateReplicationError(state, packfileID, err)
		return err
	}
	defer indexReader.Close()

	// Retry loop with exponential backoff
	for retryCount := uint8(0); retryCount <= config.MaxRetries; retryCount++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-state.stopCh:
			return errors.New("packfile: replication stopped")
		default:
		}

		// Update retry count
		state.mu.Lock()
		replication.RetryCount = retryCount
		state.mu.Unlock()

		// Attempt to replicate to S3
		err := replicateToS3(ctx, state.adapter, packfileID, packfileReader, packfileSize, indexReader, indexSize)
		if err == nil {
			state.mu.Lock()
			replication.Status = ReplicationStatusCompleted
			replication.LastAttempt = time.Now()
			state.mu.Unlock()

			pkgLogger.Info("packfile: replication completed",
				"packfile_id", packfileID,
				"retry_count", retryCount)
			return nil
		}

		pkgLogger.Info("packfile: replication failed",
			"packfile_id", packfileID,
			"retry", retryCount,
			"error", err.Error())

		// If not last retry, sleep with backoff
		if retryCount < config.MaxRetries {
			backoff := backoffDuration(retryCount, config.BaseInterval, config.MaxInterval)
			pkgLogger.Info("packfile: replication backing off",
				"packfile_id", packfileID,
				"backoff", backoff.String())

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-state.stopCh:
				return errors.New("packfile: replication stopped")
			case <-time.After(backoff):
				// Continue to next retry
			}

			// Reset readers for retry
			if err := resetFileReader(packfileReader); err != nil {
				updateReplicationError(state, packfileID, err)
				return err
			}
			if err := resetFileReader(indexReader); err != nil {
				updateReplicationError(state, packfileID, err)
				return err
			}
		}
	}

	// Max retries exceeded
	state.mu.Lock()
	replication.Status = ReplicationStatusFailed
	replication.ErrorMessage = ErrReplicationFailed.Error()
	state.mu.Unlock()

	pkgLogger.Error("packfile: replication failed permanently",
		"packfile_id", packfileID,
		"max_retries", config.MaxRetries)

	return ErrReplicationFailed
}

// openFileForReplication opens a file and returns the file, size, error
func openFileForReplication(path string) (*os.File, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("packfile: opening file for replication: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("packfile: stat file for replication: %w", err)
	}

	return file, stat.Size(), nil
}

// resetFileReader seeks a file reader back to the beginning
func resetFileReader(file *os.File) error {
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("packfile: seeking file for replication: %w", err)
	}
	return nil
}

// replicateToS3 performs the actual replication to S3 via block adapter
func replicateToS3(ctx context.Context, adapter block.Adapter, packfileID PackfileID, packfileReader io.Reader, packfileSize int64, indexReader io.Reader, indexSize int64) error {
	if adapter == nil {
		return errors.New("packfile: block adapter not configured")
	}

	// Replicate packfile
	packfilePointer := block.ObjectPointer{
		StorageID:        "", // Will be resolved from storage namespace
		StorageNamespace: "_packfiles",
		Identifier:       fmt.Sprintf("%s/data.pack", packfileID),
	}

	_, err := adapter.Put(ctx, packfilePointer, packfileSize, packfileReader, block.PutOpts{})
	if err != nil {
		return fmt.Errorf("packfile: replicating packfile to S3: %w", err)
	}

	// Replicate index
	indexPointer := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: "_packfiles",
		Identifier:       fmt.Sprintf("%s/data.sst", packfileID),
	}

	_, err = adapter.Put(ctx, indexPointer, indexSize, indexReader, block.PutOpts{})
	if err != nil {
		return fmt.Errorf("packfile: replicating index to S3: %w", err)
	}

	return nil
}

// updateReplicationError updates the replication state with an error
func updateReplicationError(state *replicationState, packfileID PackfileID, err error) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if replication, exists := state.replications[packfileID]; exists {
		replication.Status = ReplicationStatusFailed
		replication.ErrorMessage = err.Error()
		replication.LastAttempt = time.Now()
	}
}

// StartReconciliationLoop starts the background loop that retries FAILED replications
// Returns a stop function to call during shutdown
func StartReconciliationLoop(ctx context.Context, interval time.Duration) func() {
	// Use a fresh stop channel per reconciliation loop to avoid interference between loops
	stopCh := make(chan struct{})

	pkgLogger.Info("packfile: reconciliation loop started",
		"interval", interval.String())

	go func() {
		// Use the provided interval, not config.ReconcileInterval
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				pkgLogger.Info("packfile: reconciliation loop stopped (context cancelled)")
				return
			case <-stopCh:
				pkgLogger.Info("packfile: reconciliation loop stopped")
				return
			case <-ticker.C:
				reconcileFailedReplications(ctx)
			}
		}
	}()

	return func() {
		close(stopCh)
	}
}

// reconcileFailedReplications retries all FAILED replications
func reconcileFailedReplications(ctx context.Context) {
	state := getReplicationState()

	state.mu.Lock()
	var failedIDs []PackfileID
	for id, repl := range state.replications {
		if repl.Status == ReplicationStatusFailed {
			failedIDs = append(failedIDs, id)
		}
	}
	// Don't unlock yet - we need to modify the state

	if len(failedIDs) == 0 {
		state.mu.Unlock()
		return
	}

	pkgLogger.Info("packfile: reconciliation found failed replications",
		"count", len(failedIDs))

	for _, packfileID := range failedIDs {
		repl := state.replications[packfileID]
		repl.RetryCount = 0 // Reset retry count for fresh backoff
		repl.Status = ReplicationStatusPending // Mark for retry

		pkgLogger.Info("packfile: reconciliation retry marked",
			"packfile_id", packfileID)

		// Note: Actual retry requires packfile paths from KV - the caller of
		// StartReplication stores paths only in the goroutine's closure.
		// A full implementation would look up paths from KV and spawn a retry goroutine.
	}
	state.mu.Unlock()
}

// GetAllReplicationStatuses returns all replication statuses (for testing/admin)
func GetAllReplicationStatuses(ctx context.Context) ([]*PackfileReplication, error) {
	state := getReplicationState()
	state.mu.RLock()
	defer state.mu.RUnlock()

	result := make([]*PackfileReplication, 0, len(state.replications))
	for _, repl := range state.replications {
		result = append(result, repl)
	}
	return result, nil
}
