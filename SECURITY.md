# Security Policy

## Supported versions

Security fixes are applied to the latest release on the default branch. Older tags may not receive patches.

## Reporting a vulnerability

**Please do not open public GitHub issues for security vulnerabilities.**

Send a private report with:

- A description of the issue and affected components (client, server, desktop)
- Steps to reproduce
- Impact assessment (data exposure, remote code execution, etc.)
- Any suggested fix, if you have one

Contact: [support@mtrxai.net](mailto:support@mtrxai.net)

If you are testing against a self-hosted deployment, include your mtrxAI version or git commit SHA.

We aim to acknowledge reports within a few business days and will coordinate disclosure timing with you when possible.

## Scope

In scope:

- The `mtrxAI` application source (client, server, desktop, attestation)
- Authentication, authorization, and cryptography in the lobby and P2P paths
- Build and release pipeline issues that could compromise distributed binaries

Out of scope:

- Third-party services (Ollama, PostgreSQL, cloud provider consoles)
- Deploy configurations in private infrastructure repositories
- Social engineering or physical access attacks

## Safe harbor

Good-faith security research on systems you own or have explicit permission to test is welcome. Do not access other users' data or production systems without authorization.
