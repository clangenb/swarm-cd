package swarmcd

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

import (
	_ "modernc.org/sqlite"
)

func TestSaveAndLoadLastDeployedRevision(t *testing.T) {
	const stackName = "test-stack"
	setupTestDB(t)
	defer closeSqlDb()

	repoRevision := "abcdefgh"
	stackRevision := "12345678"
	stackContent := []byte("test content")

	version := newStackMetadataFromStackData(repoRevision, stackRevision, stackContent)
	now := time.Now()
	version.deployedAt = now

	err := saveLastDeployedMetadata(stackName, version)
	if err != nil {
		t.Fatalf("Failed to save repoRevision: %v", err)
	}

	loadedVersion, err := loadLastDeployedMetadata(stackName)
	if err != nil {
		t.Fatalf("Failed to load repoRevision: %v", err)
	}

	expectedHash := computeHash(stackContent)

	if loadedVersion.repoRevision != repoRevision {
		t.Errorf("Expected repoRevision %s, got %s", repoRevision, loadedVersion.repoRevision)
	}

	if loadedVersion.deployedStackRevision != stackRevision {
		t.Errorf("Expected repoRevision %s, got %s", repoRevision, loadedVersion.deployedStackRevision)
	}

	if !isRoughlyEqual(loadedVersion.deployedAt, now, 1*time.Microsecond) {
		t.Errorf("Expected time %s, got %s", now, loadedVersion.deployedAt)
	}

	if loadedVersion.hash != expectedHash {
		t.Errorf("Expected hash %s, got %s", expectedHash, loadedVersion.hash)
	}
}

// Test parallel database access
func TestDatabaseParallelAccess(t *testing.T) {
	setupTestDB(t)
	defer closeSqlDb()

	const workers = 10
	const insertsPerWorker = 10
	var wg sync.WaitGroup
	now := time.Now()

	// Parallel inserts
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < insertsPerWorker; j++ {
				stackName := fmt.Sprintf("stack-%d", workerID)
				stackMetadata := newStackMetadata(
					fmt.Sprintf("repoRev-%d-%d", workerID, j),
					fmt.Sprintf("stackRev-%d-%d", workerID, j),
					fmt.Sprintf("hash-%d-%d", workerID, j),
					now,
				)

				err := saveLastDeployedMetadata(stackName, stackMetadata)
				if err != nil {
					t.Errorf("Worker %d: failed to insert: %v", workerID, err)
				}
			}
		}(i)
	}

	wg.Wait() // Wait for all goroutines

	// Verify correct number of unique stack entries
	var count int
	err := globalDB.QueryRow("SELECT COUNT(*) FROM revisions").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	// Expected: At most `workers` unique entries due to `ON CONFLICT`
	if count != workers {
		t.Errorf("Expected %d unique stacks, got %d", workers, count)
	}
}

func setupTestDB(t *testing.T) {
	const dbFile = "file:/foobar?vfs=memdb" // Use in-memory database for tests

	err := initSqlDB(dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	t.Log("Test DB setup complete.")

}

func isRoughlyEqual(t1, t2 time.Time, tolerance time.Duration) bool {
	diff := t2.Sub(t1)
	// Check if the difference is within the tolerance
	if diff < 0 {
		diff = -diff // Handle negative difference
	}
	return diff <= tolerance
}
