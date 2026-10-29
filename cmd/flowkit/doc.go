// Command flowkit is the production entry point for the Flowkit voice kernel.
// It loads configuration, wires observability, transports, providers, and the
// JSON-RPC server, then runs until SIGTERM triggers graceful shutdown.
package main
