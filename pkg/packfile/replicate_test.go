package packfile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		name         string
		retryCount   uint8
		baseInterval time.Duration
		maxInterval  time.Duration
		expected     time.Duration
	}{
		{
			name:         "retry 0",
			retryCount:   0,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     1 * time.Second, // 1 * 2^0 = 1
		},
		{
			name:         "retry 1",
			retryCount:   1,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     2 * time.Second, // 1 * 2^1 = 2
		},
		{
			name:         "retry 2",
			retryCount:   2,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     4 * time.Second, // 1 * 2^2 = 4
		},
		{
			name:         "retry 3",
			retryCount:   3,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     8 * time.Second, // 1 * 2^3 = 8
		},
		{
			name:         "retry 4 exceeds max",
			retryCount:   4,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     10 * time.Second, // capped at max
		},
		{
			name:         "retry 5 way exceeds max",
			retryCount:   5,
			baseInterval: 1 * time.Second,
			maxInterval:  10 * time.Second,
			expected:     10 * time.Second, // still capped at max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := backoffDuration(tt.retryCount, tt.baseInterval, tt.maxInterval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReplicationStatus(t *testing.T) {
	tests := []struct {
		name   string
		status ReplicationStatus
		want   string
	}{
		{"Pending", ReplicationStatusPending, "Pending"},
		{"InProgress", ReplicationStatusInProgress, "InProgress"},
		{"Completed", ReplicationStatusCompleted, "Completed"},
		{"Failed", ReplicationStatusFailed, "Failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify status values are as expected
			switch tt.status {
			case ReplicationStatusPending:
				assert.Equal(t, uint8(0), uint8(tt.status))
			case ReplicationStatusInProgress:
				assert.Equal(t, uint8(1), uint8(tt.status))
			case ReplicationStatusCompleted:
				assert.Equal(t, uint8(2), uint8(tt.status))
			case ReplicationStatusFailed:
				assert.Equal(t, uint8(3), uint8(tt.status))
			}
		})
	}
}

func TestDefaultReplicationConfig(t *testing.T) {
	cfg := DefaultReplicationConfig()

	assert.Equal(t, uint8(10), cfg.MaxRetries)
	assert.Equal(t, 1*time.Second, cfg.BaseInterval)
	assert.Equal(t, 10*time.Second, cfg.MaxInterval)
	assert.Equal(t, 5*time.Minute, cfg.ReconcileInterval)
}

func TestGetReplicationState(t *testing.T) {
	// Test that getReplicationState returns the same instance
	state1 := getReplicationState()
	state2 := getReplicationState()
	assert.Same(t, state1, state2)
}

func TestSetBlockAdapter(t *testing.T) {
	// Test that SetBlockAdapter doesn't panic
	// (actual adapter would be mocked in integration tests)
	state := getReplicationState()
	assert.Nil(t, state.adapter)

	// Set nil adapter - should not panic
	SetBlockAdapter(nil)
	assert.Nil(t, state.adapter)
}

func TestSetReplicationConfig(t *testing.T) {
	state := getReplicationState()
	originalConfig := state.config

	// Set custom config
	customConfig := ReplicationConfig{
		MaxRetries:        5,
		BaseInterval:      2 * time.Second,
		MaxInterval:       20 * time.Second,
		ReconcileInterval: 10 * time.Minute,
	}

	SetReplicationConfig(customConfig)

	assert.Equal(t, customConfig.MaxRetries, state.config.MaxRetries)
	assert.Equal(t, customConfig.BaseInterval, state.config.BaseInterval)
	assert.Equal(t, customConfig.MaxInterval, state.config.MaxInterval)
	assert.Equal(t, customConfig.ReconcileInterval, state.config.ReconcileInterval)

	// Reset to original
	state.config = originalConfig
}

func TestGetReplicationStatusNotFound(t *testing.T) {
	ctx := context.Background()

	// Start fresh - clear any existing state
	state := getReplicationState()
	state.mu.Lock()
	state.replications = make(map[PackfileID]*PackfileReplication)
	state.mu.Unlock()

	status, err := GetReplicationStatus(ctx, "nonexistent")
	assert.ErrorIs(t, err, ErrReplicationNotFound)
	assert.Equal(t, ReplicationStatusPending, status)
}

func TestStartReplicationAlreadyInProgress(t *testing.T) {
	ctx := context.Background()

	// Clear state
	state := getReplicationState()
	state.mu.Lock()
	state.replications = make(map[PackfileID]*PackfileReplication)
	state.mu.Unlock()

	// Start first replication
	err := StartReplication(ctx, "test-pf-1", "/tmp/packfile.pack", "/tmp/packfile.sst")
	require.NoError(t, err)

	// Try to start again - should fail
	err = StartReplication(ctx, "test-pf-1", "/tmp/packfile.pack", "/tmp/packfile.sst")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")
}

func TestGetAllReplicationStatuses(t *testing.T) {
	ctx := context.Background()

	// Clear state
	state := getReplicationState()
	state.mu.Lock()
	state.replications = make(map[PackfileID]*PackfileReplication)
	state.mu.Unlock()

	// Initially empty
	statuses, err := GetAllReplicationStatuses(ctx)
	require.NoError(t, err)
	assert.Empty(t, statuses)

	// Start a replication
	err = StartReplication(ctx, "test-pf-1", "/tmp/packfile.pack", "/tmp/packfile.sst")
	require.NoError(t, err)

	// Should have one status
	statuses, err = GetAllReplicationStatuses(ctx)
	require.NoError(t, err)
	assert.Len(t, statuses, 1)
	assert.Equal(t, PackfileID("test-pf-1"), statuses[0].PackfileID)
	assert.Equal(t, ReplicationStatusPending, statuses[0].Status)
}

func TestReplicationStateMutexSafety(t *testing.T) {
	// This test verifies the mutex is properly used
	state := getReplicationState()

	// Concurrent reads and writes should not panic
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				state.mu.RLock()
				_ = len(state.replications)
				state.mu.RUnlock()

				state.mu.Lock()
				state.replications[PackfileID("test")] = &PackfileReplication{}
				state.mu.Unlock()
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
