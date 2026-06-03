<!--
  Adapted from specs/templates/spec-template.md
  Feature: 005-security-hardening
-->

# Feature Specification: Security Hardening

**Created**: 2026-06-02
**Status**: Draft
**Input**: Static analysis findings from automated security tooling — 7 code-level issues and 18 CVEs in the Go standard library

## User Scenarios & Testing *(mandatory)*

### User Story 1 - All random values are generated from a cryptographically secure source (Priority: P1)

An operator deploying this service in a production environment needs confidence that every
random value produced by the service is derived from a source that cannot be predicted or
influenced by an external observer. Using a predictable random number generator anywhere in
the service — even for non-secret purposes such as retry jitter — violates defence-in-depth,
sets a precedent that weakens the overall security posture, and will be flagged as a violation
by automated security scanning tools in the CI pipeline.

**Why this priority**: Predictable randomness is an attacker primitive. Even when the
immediate use appears harmless, a weak RNG anywhere in the service is a P1 blocker for
security review and erodes confidence in the randomness properties of security-sensitive
operations.

**Independent Test**: Run the automated security scanner against the project source. Zero
findings related to weak random number generation should be reported. Also verify that jitter
values observed across at least 1 000 consecutive retry cycles are not a trivially predictable
arithmetic sequence.

**Acceptance Scenarios**:

1. **Given** the service source is scanned by the automated security tool, **When** the
   scanner checks for weak random number generation, **Then** zero findings of that class
   are reported.

2. **Given** the service is running and produces retry jitter values over at least 1 000
   retry cycles, **When** the sequence of jitter values is examined, **Then** it is not a
   predictable arithmetic sequence.

3. **Given** a change is introduced that reintroduces a non-cryptographically-secure
   random number generator anywhere in the service, **When** the automated security scanner
   runs in CI, **Then** the build fails and the regression is surfaced before the change
   can be merged.

---

### User Story 2 - Internal diagnostic endpoints are not exposed on any service port (Priority: P1)

An operator who has deployed the service in a network-accessible environment needs
confidence that the runtime's internal profiling and diagnostic capabilities are not
reachable through the service's HTTP interface. An accessible diagnostic endpoint leaks
memory profiles, execution-context snapshots, CPU traces, and other runtime internals that
an attacker can use to fingerprint the service or prepare a targeted exploit against it.

**Why this priority**: Exposure of diagnostic data through the service's public interface
is a high-severity finding in any production security review. It provides an attacker with
structural knowledge of the service that materially assists memory and timing attacks.

**Independent Test**: Deploy the service with a standard configuration. Attempt to access
the runtime profiling paths from the main service port. Verify that no profiling data is
returned — the response must not be a successful 200 with profiling content.

**Acceptance Scenarios**:

1. **Given** the service is running with a standard production configuration, **When** any
   caller requests the runtime profiling paths on the main service port, **Then** the server
   returns no profiling data; the response is not a 200 OK containing profiling content.

2. **Given** the service source is scanned by the automated security tool, **When** the
   scanner checks for unauthenticated diagnostic endpoint exposure, **Then** zero findings
   of that class are reported.

3. **Given** the service is running, **When** an unauthenticated caller requests any URL
   path associated with runtime profiling or diagnostic data on any service port, **Then**
   every such request returns a non-200 response and no profiling payload is included in
   the body.

---

### User Story 3 - Numeric value conversions do not produce silent overflow (Priority: P2)

A developer relying on the idempotency guarantee and data store access behaviour of the
service needs confidence that numeric values used internally cannot silently truncate or
wrap into incorrect representations. A silent overflow in a deduplication identifier can
cause two distinct operations to be treated as duplicates, breaking the idempotency
guarantee; a silent overflow in a concurrency configuration value can produce an effective
limit that differs from what was configured, degrading throughput or causing errors.

**Why this priority**: Silent numeric overflow breaks both a security-critical invariant
(idempotency) and an operational invariant (concurrency limit) in a way that is invisible
at runtime but trivially detectable and preventable through static analysis.

**Independent Test**: Run the automated security scanner against the source. Zero findings
of unsafe numeric type conversion should be reported. Submit a pair of operations with
distinct identifiers that the idempotency subsystem must treat as independent, and verify
the system treats them as two separate operations.

**Acceptance Scenarios**:

1. **Given** the service source is scanned by the automated security tool, **When** the
   scanner checks for unsafe numeric type conversions, **Then** zero findings of that class
   are reported.

2. **Given** two operations with distinct identifiers that the system must treat as
   independent, **When** both are submitted concurrently, **Then** neither is treated as a
   duplicate of the other, and both operations proceed to completion independently.

3. **Given** the service is configured with a specific maximum concurrency limit for data
   store access, **When** the service starts and the limit takes effect, **Then** the
   effective limit observable during operation matches the configured value.

---

### User Story 4 - The runtime has no known published CVEs (Priority: P2)

An operator performing a security review before a production deployment needs to confirm
that the runtime version declared by the project has no known, publicly disclosed
vulnerabilities. Vulnerable runtime versions can expose the service to exploits in the
runtime packages it depends on — including TLS negotiation, certificate validation, URL
parsing, and HTTP handling — all of which are core to the webhook delivery pipeline.

**Why this priority**: Known CVEs in the runtime are the lowest-cost class of
vulnerability to remediate (a version bump) and the hardest to justify leaving unpatched.
A compliance scan or deployment review will block promotion to production until they are
resolved.

**Independent Test**: Run the vulnerability scanner against the project dependency graph.
Verify that no vulnerability findings are reported for the runtime packages or any direct
or indirect dependency.

**Acceptance Scenarios**:

1. **Given** the project's dependency declaration, **When** the vulnerability scanner is run
   against the full dependency graph, **Then** zero vulnerability findings are reported for
   the runtime packages and all dependencies.

2. **Given** the project declares a specific runtime version, **When** that version is
   compared against the official security release history for the declared runtime series,
   **Then** the declared version is the latest patch release in that series, or a newer
   version.

---

### User Story 5 - Errors from cryptographic operations are not silently discarded (Priority: P3)

A developer auditing the security properties of the codebase needs confidence that the
service never silently discards an error from a cryptographic or hash operation. Even when
a specific implementation is currently documented as infallible, suppressing its error
violates the service's own error-handling contract, prevents static analysis tools from
confirming correctness, and will silently mask failures if the implementation is ever
replaced by one that can fail.

**Why this priority**: The immediate risk is low since the affected operations currently
cannot fail. However, suppressed errors in security-relevant code paths are consistently
flagged by static analysis tools and establish a pattern that could mask real failures in
the future.

**Independent Test**: Run the automated security scanner against the source. Zero findings
of unhandled error returns from cryptographic operations should be reported.

**Acceptance Scenarios**:

1. **Given** the service source is scanned by the automated security tool, **When** the
   scanner checks for unhandled error returns from I/O and write operations, **Then** zero
   findings of that class are reported for cryptographic code paths.

---

### Edge Cases

- What if a build flag or conditional compilation makes the diagnostic profiling endpoint
  available only in debug builds? The security property must hold unconditionally — any
  configuration that could enable the endpoint in a deployed service is unacceptable.
- What if a future code change reintroduces a weak RNG or unsafe conversion? The build
  pipeline must run the security scanner on every build so that regressions are caught
  before merging (per FR-010).
- What if the runtime upgrade required to clear all CVEs also introduces a breaking change
  in a runtime API? Evaluating API compatibility is a plan.md concern; this spec only
  requires that the declared runtime version carries no known CVEs.
- What if a cryptographic primitive currently documented as infallible begins reporting
  errors in a future version? Because US5 requires all errors to be surfaced, the failure
  will reach the caller rather than being silently discarded.
- What if the overflow guard introduces a performance regression in a high-frequency code
  path? Correctness takes precedence; any performance trade-off is a plan.md concern.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The service MUST use a cryptographically secure source of randomness for
  every random value it generates, including values used for retry jitter; the service
  MUST NOT use any non-cryptographically-secure random number generator in any part of
  the production service.

- **FR-002**: The service MUST NOT register or expose any runtime diagnostic or profiling
  endpoint on any service port. Such an endpoint MUST NOT be accessible to any caller
  through the service's HTTP interface.

- **FR-003**: The service MUST NOT produce a silently truncated or wrapped numeric result
  when an internal computation exceeds the valid range of its target representation. When
  such a condition occurs, the system MUST surface an explicit error to the caller of the
  affected operation rather than silently continuing with an incorrect value.

- **FR-004**: Two operations with different identifiers MUST NOT be treated as duplicates
  of each other; the idempotency guarantee MUST hold for any two distinct operations
  submitted to the system. This guarantee MUST hold under at least 10 000 concurrent
  idempotency checks across distinct operations.

- **FR-005**: The service MUST honour the maximum concurrency limit configured for data
  store access; the effective limit MUST equal the configured value without silent
  alteration.

- **FR-006**: The project's declared runtime version MUST have no known, publicly
  disclosed CVEs. The declared version MUST be sufficient to produce zero findings when
  the vulnerability scanner is run against the project's full dependency graph.

- **FR-007**: The service MUST propagate failures from cryptographic and hash operations
  to their callers. A failure in such an operation MUST NOT be silently suppressed; the
  caller of the affected operation MUST receive an indication of the failure.

- **FR-008**: The automated security scanner MUST report zero findings of the following
  classes against the production source: (a) weak random number generation, (b)
  unauthenticated diagnostic endpoint exposure, (c) unsafe integer type conversion,
  (d) unhandled error returns from write operations.

- **FR-009**: The vulnerability scanner MUST report zero vulnerability findings against
  the project's full dependency graph, including the declared runtime version.

- **FR-010**: The service's build pipeline MUST execute the static security scanner and
  the vulnerability scanner on every build; a build MUST fail if either scanner reports
  any finding.

### Key Entities

No new persistent entities are introduced by this feature. All changes affect the
build-time properties and runtime behaviour of existing service components.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The automated security scanner reports zero findings for all four issue
  classes addressed by this feature — weak random number generation, unauthenticated
  diagnostic endpoint exposure, unsafe numeric type conversions, and unhandled error
  returns from cryptographic operations — against the production source after this feature
  is delivered.

- **SC-002**: The vulnerability scanner reports zero CVE findings against the project's
  full dependency graph, including all runtime packages, after this feature is delivered.

- **SC-003**: The service returns no profiling or diagnostic data to any caller on any
  service port — verifiable by requesting the known profiling paths from the running
  service and confirming no profiling content is returned and no 200 OK response is
  received.

- **SC-004**: The build pipeline executes both the static security scanner and the
  vulnerability scanner on every build, and fails when either reports a finding —
  measurable by observing a build failure when a finding is reintroduced (per FR-010).

- **SC-005**: The idempotency guarantee holds for at least 10 000 concurrent operations
  with distinct identifiers, with zero observed collisions — measurable via a concurrent
  integration test (per FR-004).

## Assumptions

- This feature is a pure hardening pass with no user-visible behaviour changes. No new
  HTTP endpoints, Kafka topics, PostgreSQL schemas, or Redis key spaces are introduced.
- The diagnostic profiling capability removed by US2 is used only for local development.
  If a diagnostic capability is needed in a future operational context, it must be
  introduced as a separately secured feature.
- The runtime upgrade required by US4 is a patch-level upgrade within the same minor
  series (e.g., 1.25.x to the latest 1.25 patch for the Go toolchain currently in use).
  If a minor or major upgrade is necessary to clear all findings, that requires explicit
  approval and is outside the scope of this spec.
- The security scanner findings that form the measurable baseline are exactly those
  described in the task input: 7 code-level issues and 18 CVEs in the standard library.
  Findings introduced by unrelated changes after this spec is written are out of scope.
- Integrating the security scanner and vulnerability scanner into the build pipeline is
  part of the implementation work for this feature, including configuring both scanners
  to fail the build on any finding.
- Authentication and authorisation are out of scope for this feature, consistent with
  features 001–004.
- The retry jitter computation is not used to derive secrets or session tokens; the
  cryptographically secure RNG requirement for US1 follows from the project's policy of
  using a secure source for all randomness, not from a specific cryptographic use of the
  jitter values.
