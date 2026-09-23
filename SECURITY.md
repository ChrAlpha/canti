# Security policy

## Project status

canti is an experimental single-machine agent with no stable releases or
production support commitment. The current branch is the development target.
It contains an Anthropic Messages adapter, a SQLite journal, and a Docker
executor. The `exec` protocol is not an authorization mechanism: the agent may
run any command inside its container.

Relevant reports include unsafe decision decoding, effect identity collisions,
recovery that duplicates an external effect, credential exposure, and isolation
or resource-limit failures in the adapters. Identify the exact revision and
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

Each command the agent chooses runs in its own Docker container. The container
sees the workspace read-write and earlier job outputs read-only; the journal,
job records, and the model credential stay outside it. It receives no host
environment variables, runs as the invoking user's uid with all capabilities
dropped and `no-new-privileges`, and has no network unless the run is created
with another `-network`. The CLI removes the credential from its environment
before starting any process and sends it only as an HTTP header, over `https`
unless the endpoint is a loopback address.

Known limits:

- Docker is the isolation boundary. Jobs share the host kernel, and anyone who
  can use the Docker daemon controls the host; canti adds no sandbox of its own.
- canti trusts the Docker daemon, anything with access to it, and any process
  of the same user that can modify the state directory; jobs have neither.
  Within that assumption, a container is recognized by its name and labels
  alone, so a principal with daemon access could plant one that canti adopts,
  and docker CLI output and final job records are read without a size cap.
- The workspace is writable by the agent by design, so anything placed in it
  is agent-controlled. Notes in the workspace are never trusted as facts.
- The state directory is protected only by file permissions. A process of the
  same user can read or change the journal and artifacts, or replace the
  workspace lock file and defeat single-writer exclusion.
- A non-default network lets jobs reach any address it can route to.
- Only retained job output is bounded, per job and per run between steps.
  Workspace writes, the journal, and Docker's log of a running job, which only
  the job's run time bounds, are not; there is no filesystem quota.

Preserve uncertainty after a timeout or interrupted dispatch; neither proves
that an external action did not happen. [V1 acceptance](docs/v1-acceptance.md)
records which of these properties the integration tests exercise, and against
which backend versions.
