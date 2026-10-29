// Package telecom is the SIP Trunk Catalog: a runtime registry of SIP lines
// (peer endpoints + outbound profiles), maintained either statically from
// env vars (the built-in "system" Provider) or dynamically by external
// Providers via the HTTP register API.
//
// FlowKit deliberately holds NO phone number directory: tenants, DIDs,
// number→trunk binding, and IVR policy live in the CC layer. The Catalog
// only answers two questions:
//
//   - inbound: which trunk_id matches this source IP?
//   - outbound: for this trunk_id, what is the OutboundProfile?
package telecom
