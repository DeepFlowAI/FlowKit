// Package session models a Call lifecycle (state machine + event bus) and a
// Manager that creates, looks up, and closes calls.
//
// Ownership: Manager holds a root context. Each Call has a per-call context
// cancelled when the call ends (Done is that context's Done channel), derived
// via WithoutCancel so root cancel cannot desync Done from IsEnded. Shutdown
// Hangup-loops first, then cancels the root; watch Hangups on root cancel as
// fallback, then waits are bounded.
package session
