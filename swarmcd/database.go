package swarmcd

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
	"sync"
	"time"
)

var globalDB *sql.DB
var dbPath string

var dbMutex sync.Mutex

type stackMetadata struct {
	repoRevision          string
	deployedStackRevision string
	deployedAt            time.Time
	hash                  string
}

func newStackMetadata(repoRevision string, stackRevision string, hash string, time time.Time) *stackMetadata {
	return &stackMetadata{
		repoRevision:          repoRevision,
		deployedStackRevision: stackRevision,
		hash:                  hash,
		deployedAt:            time,
	}
}

func newStackMetadataFromStackData(repoRevision string, stackRevision string, stackData []byte) *stackMetadata {
	return &stackMetadata{
		repoRevision:          repoRevision,
		deployedStackRevision: stackRevision,
		hash:                  computeHash(stackData),
		deployedAt:            time.Now(),
	}
}

func (stackMetadata *stackMetadata) fmtHash() string {
	return fmtHash(stackMetadata.hash)
}

// Ensure database and table exist
func initSqlDB(dbFile string) error {
	dbPath = dbFile
	return initDB()
}

func initDB() error {
	sqlDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Enable Write-Ahead Logging (WAL) mode allowing concurrent read access
	_, err = sqlDB.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Set a busy timeout to prevent SQLITE_BUSY errors
	_, err = sqlDB.Exec(`PRAGMA busy_timeout = 5000;`)
	if err != nil {
		return fmt.Errorf("failed to set busy timeout: %w", err)
	}

	_, err = sqlDB.Exec(`CREATE TABLE IF NOT EXISTS revisions (
		stack TEXT PRIMARY KEY, 
		repo_revision TEXT, 
		deployed_stack_revision TEXT, 
		hash TEXT, 
		deployed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	globalDB = sqlDB
	return nil
}

func ensureDBAliveOrReconnect() error {
	if err := globalDB.Ping(); err != nil {
		logger.Info("Database connection lost, reconnecting...")
		if err := initDB(); err != nil {
			return fmt.Errorf("failed to reconnect to the database: %w", err)
		}
	}

	return nil
}

func closeSqlDb() error {
	if globalDB != nil {
		err := globalDB.Close()
		if err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		globalDB = nil
	} else {
		logger.Info("DB was uninitialized closed")
	}

	return nil
}

// Save last deployed stackMetadata
func saveLastDeployedMetadata(stackName string, stackMetadata *stackMetadata) error {
	if globalDB == nil {
		return fmt.Errorf("DB not initialized")
	}

	// We need this to prevent occasional `SQL_DB_BUSY` errors
	dbMutex.Lock()
	_, err := globalDB.Exec(`
		INSERT INTO revisions (stack, repo_revision, deployed_stack_revision, hash, deployed_at) 
		VALUES (?, ?, ?, ?, ?) 
		ON CONFLICT(stack) DO UPDATE SET 
			repo_revision = excluded.repo_revision, 
			deployed_stack_revision = excluded.deployed_stack_revision, 
			hash = excluded.hash,
			deployed_at = excluded.deployed_at
	`, stackName, stackMetadata.repoRevision, stackMetadata.deployedStackRevision, stackMetadata.hash, stackMetadata.deployedAt)
	dbMutex.Unlock()

	if err != nil {
		return fmt.Errorf("failed to save revision: %w", err)
	}

	return nil
}

// Load a stack's stackMetadata
func loadLastDeployedMetadata(stackName string) (*stackMetadata, error) {
	if globalDB == nil {
		return nil, fmt.Errorf("DB not initialized")
	}

	var repoRevision, deployedStackRevision, hash string
	var deployedAt time.Time

	err := globalDB.QueryRow(`
		SELECT repo_revision, deployed_stack_revision, hash, deployed_at 
		FROM revisions 
		WHERE stack = ?`, stackName).Scan(&repoRevision, &deployedStackRevision, &hash, &deployedAt)

	if err == sql.ErrNoRows {
		return newStackMetadata("", "", "", time.Now()), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query revision: %w", err)
	}

	return &stackMetadata{
		repoRevision:          repoRevision,
		deployedStackRevision: deployedStackRevision,
		hash:                  hash,
		deployedAt:            deployedAt,
	}, nil
}

// Compute a SHA-256 hash of the stack content
func computeHash(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

func fmtHash(hash string) string {
	if len(hash) >= 8 {
		return hash[:8]
	}
	return "<empty-hash>"
}
