package swarmcd

import (
	"fmt"
	"github.com/m-adawi/swarm-cd/util"
	"os"
	"strconv"
	"sync"
	"time"
)

var stackStatus = map[string]*StackStatus{}
var stacks = map[string]*swarmStack{}

func getWorkerCount() int {
	const defaultWorkers = 3

	workerStr := os.Getenv("SWARMCD_CONCURRENCY")
	if workerStr == "" {
		return defaultWorkers
	}

	workerCount, err := strconv.Atoi(workerStr)
	if err != nil || workerCount <= 0 {
		logger.Warn(fmt.Sprintf("Invalid SWARMCD_CONCURRENCY value, using default: %v", defaultWorkers))

		return defaultWorkers
	}

	return workerCount
}

func Run() {
	logger.Info("starting SwarmCD")
	for {
		logger.Debug("starting update loop")
		var waitGroup sync.WaitGroup
		stacksChannel := make(chan *swarmStack, len(stacks))

		// Start worker pool
		var workerCount = getWorkerCount()
		logger.Info(fmt.Sprintf("worker count: %v", workerCount))

		for i := 0; i < workerCount; i++ {
			go worker(stacksChannel, &waitGroup)
		}

		// Send stacks to workers
		for _, swarmStack := range stacks {
			logger.Debug(fmt.Sprintf("Queueing stack %v for update", swarmStack.name))
			waitGroup.Add(1)
			stacksChannel <- swarmStack
		}
		close(stacksChannel)

		// Wait for all workers to complete
		waitGroup.Wait()

		logger.Info("waiting for the update interval")
		time.Sleep(time.Duration(config.UpdateInterval) * time.Second)

		logger.Info("checking if new repos or new stacks are in the config")
		updateStackConfigs()
	}
}

func worker(stacks <-chan *swarmStack, waitGroup *sync.WaitGroup) {
	for swarmStack := range stacks {
		updateStackThread(swarmStack)
		waitGroup.Done()
	}
}

func updateStackConfigs() {
	err := util.LoadConfigs()
	if err != nil {
		logger.Info("Error calling loadConfig again: %v", err)
		return
	}

	err = initRepos()
	if err != nil {
		logger.Info("Error calling initRepos again: %v", err)
	}

	err = initStacks()
	if err != nil {
		logger.Info("Error calling initStacks again: %v", err)
	}
}

func updateStackThread(swarmStack *swarmStack) {
	repoLock := swarmStack.repo.lock
	repoLock.Lock()
	defer repoLock.Unlock()

	logger.Debug(fmt.Sprintf("%s checking if stack needs to be updated", swarmStack.name))
	stackMetadata, err := swarmStack.updateStack()
	if err != nil {
		stackStatus[swarmStack.name].Error = err.Error()
		logger.Error(err.Error())
		return
	}

	stackStatus[swarmStack.name].Error = ""
	stackStatus[swarmStack.name].Revision = stackMetadata.repoRevision
	stackStatus[swarmStack.name].DeployedStackRevision = stackMetadata.deployedStackRevision
	stackStatus[swarmStack.name].DeployedAt = stackMetadata.deployedAt.Format(time.RFC3339)
	logger.Debug(fmt.Sprintf("%s updateStackThread done", swarmStack.name))
}

func GetStackStatus() map[string]*StackStatus {
	return stackStatus
}
