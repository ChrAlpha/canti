# Security policy

## Project status

canti is an experimental skeleton with no stable releases or production support
commitment. The current branch is the development target. There is no concrete
model adapter, SQLite store, execution backend, or sandbox in the repository.
The `exec` protocol is not an authorization or isolation mechanism.

Relevant reports include unsafe decision decoding, effect identity collisions,
recovery that duplicates an external effect, credential exposure, and isolation
or resource-limit failures in future adapters. Identify the exact revision and
any external integration involved.

## Private reporting

When the repository's Security tab offers **Report a vulnerability**, use that
private GitHub reporting channel. Include impact, affected revision, minimal
reproduction, and any mitigation. Remove credentials and unrelated private data.

Private vulnerability reporting is not yet configured for this repository.
Until it is available, request a private contact channel from the maintainer
through an existing trusted channel. If no such channel is available, an issue
may request a private contact without including vulnerability details. Do not
post exploit details, affected private data, or secrets in that issue.

Before public availability, the maintainer should enable GitHub private
vulnerability reporting or publish a monitored private contact here. No
acknowledgement or remediation deadline is currently promised.

## Runtime trust boundary

Future execution adapters must keep host credentials and authoritative run facts
outside the agent's writable workspace. Expose only the required capabilities
and read-only history projections. Preserve uncertainty after a timeout or
interrupted dispatch; neither proves that an external action did not happen.

Security claims about persistence, isolation, cancellation, or recovery require
tests against the concrete backend. The current fixture tests do not establish
those deployment properties.
