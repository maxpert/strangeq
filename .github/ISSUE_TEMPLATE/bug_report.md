---
name: Bug report
about: Report a reproducible problem
title: '[BUG] '
labels: bug
assignees: ''
---

## Problem

Describe what happened and what you expected.

## Reproduction

1. Start the broker with ...
2. Send or consume ...
3. Observe ...

Include a minimal client example or exact commands if possible.

## Environment

- StrangeQ version or commit (`amqp-server --version`):
- OS and architecture (for example, Linux arm64 or macOS arm64):
- Go version, if built from source:
- Client library and version:
- Mode: default standalone, local Raft (`cluster.local`), or multi-node cluster:
- Storage durability profile and filesystem:
- For clusters: node count and relevant queue/shard placement:

## Configuration and logs

Include relevant configuration and logs. Remove passwords, tokens, private keys,
and other sensitive values.

```yaml
# Minimal broker configuration
```

```text
Relevant logs
```

## Additional context

Include workload details, restart/failure steps, or screenshots if relevant.
Report vulnerabilities privately using [SECURITY.md](../../SECURITY.md).
