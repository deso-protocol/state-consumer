package consumer

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// StateSyncerConsumer provides a simplified interface for state change consumption
// using the new diff-based FileProcessor architecture
type StateSyncerConsumer struct {
	// The FileProcessor handles all state change processing
	FileProcessor *FileProcessor
}

// InitializeAndRun creates and starts a FileProcessor for diff-based state change consumption
func (consumer *StateSyncerConsumer) InitializeAndRun(
	stateChangeDir string, consumerProgressDir string, batchBytes uint64,
	threadLimit int, syncMempool bool, handler StateSyncerDataHandler) error {

	glog.Infof("Starting diff-based state consumer with FileProcessor architecture")

	// Configure the FileProcessor
	config := FileProcessorConfig{
		StateChangeDir:      stateChangeDir,
		ProgressDir:         consumerProgressDir,
		MaxConcurrentChunks: threadLimit,
		BatchSize:           batchBytes / 1000, // Convert bytes to approximate entry count
	}

	// Create the file processor
	fileProcessor, err := NewFileProcessor(config, handler)
	if err != nil {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error creating FileProcessor")
	}

	// Store the file processor for cleanup
	consumer.FileProcessor = fileProcessor

	// Start processing - this will handle hypersync → committed blocks → mempool flow automatically
	if err := fileProcessor.Start(); err != nil {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error starting FileProcessor")
	}

	glog.Infof("FileProcessor started successfully - processing diff-based state changes")

	// Block until we receive a signal to stop
	return consumer.waitForShutdown()
}

// waitForShutdown blocks until a termination signal is received
func (consumer *StateSyncerConsumer) waitForShutdown() error {
	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	glog.Infof("Consumer running - waiting for shutdown signal...")

	// Wait for signal
	sig := <-sigChan
	glog.Infof("Received signal %v - initiating graceful shutdown", sig)

	// Stop the FileProcessor
	consumer.Stop()

	glog.Infof("Consumer shutdown complete")
	return nil
}

// Stop gracefully shuts down the consumer
func (consumer *StateSyncerConsumer) Stop() {
	if consumer.FileProcessor != nil {
		if err := consumer.FileProcessor.Stop(); err != nil {
			glog.Errorf("Error stopping FileProcessor: %v", err)
		}
	}
}
