package swarmcd_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const swarmManagerImage = "docker:dind"

func TestSwarmCDIntegration(t *testing.T) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()

	// Step 1: Start a Swarm cluster inside a test container
	swarmManager, err := startSwarmManager(ctx)
	if err != nil {
		t.Fatalf("Failed to start Swarm manager: %v", err)
	}
	defer swarmManager.Terminate(ctx)

	// Step 2: Initialize Swarm
	_, err = runCommand("docker", "swarm", "init")
	if err != nil {
		t.Fatalf("Failed to initialize Swarm: %v", err)
	}

	// Step 3: Set up a test Git repository
	gitRepoDir, err := os.MkdirTemp("", "swarmcd-repo")
	if err != nil {
		t.Fatalf("Failed to create temp Git repository: %v", err)
	}
	defer os.RemoveAll(gitRepoDir)

	err = setupFakeGitRepo(gitRepoDir)
	if err != nil {
		t.Fatalf("Failed to set up Git repository: %v", err)
	}

	// Step 4: Build SwarmCD Docker image
	err = buildSwarmCDImage()
	if err != nil {
		t.Fatalf("Failed to build SwarmCD Docker image: %v", err)
	}

	// Step 5: Deploy SwarmCD as a stack
	err = deploySwarmCDStack()
	if err != nil {
		t.Fatalf("Failed to deploy SwarmCD stack: %v", err)
	}

	// Step 6: Wait for services to be deployed
	time.Sleep(10 * time.Second)

	// Step 7: Verify deployment
	out, err := runCommand("docker", "service", "ls")
	if err != nil {
		t.Fatalf("Failed to list services: %v", err)
	}
	assert.Contains(t, out, "swarm-cd", "Expected service 'swarm-cd' to be deployed")

	// (Optional) Verify stack changes by modifying the Git repository
}

// startSwarmManager starts a Docker-in-Docker container for Swarm
func startSwarmManager(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        swarmManagerImage,
		Privileged:   true,
		ExposedPorts: []string{"2377/tcp", "7946/tcp", "4789/udp"},
		WaitingFor:   wait.ForLog("Swarm initialized"),
		Entrypoint:   []string{"dockerd-entrypoint.sh"},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// setupFakeGitRepo creates a Git repository with a sample stack config
func setupFakeGitRepo(dir string) error {
	_, err := runCommand("git", "-C", dir, "init")
	if err != nil {
		return err
	}

	stackConfig := "version: '3.8'\nservices:\n  my-app:\n    image: nginx\n"
	err = os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte(stackConfig), 0644)
	if err != nil {
		return err
	}

	_, err = runCommand("git", "-C", dir, "add", ".")
	if err != nil {
		return err
	}

	_, err = runCommand("git", "-C", dir, "commit", "-m", "Initial commit")
	return err
}

// buildSwarmCDImage builds a Docker image for SwarmCD
func buildSwarmCDImage() error {
	_, err := runCommand("docker", "build", "-t", "swarmcd:test", ".")
	return err
}

// deploySwarmCDStack deploys SwarmCD using a test stack file
func deploySwarmCDStack() error {
	_, err := runCommand("docker", "stack", "deploy", "--compose-file", "test/docker-compose.yaml", "swarm-cd")
	return err
}

// runCommand executes a shell command and returns output
func runCommand(cmd string, args ...string) (string, error) {
	out, err := exec.Command(cmd, args...).CombinedOutput()
	return string(out), err
}
