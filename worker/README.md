# Worker

The `worker` crate handles transaction payloads for a primary.

## Responsibilities

- accept transactions from benchmark clients
- create and persist batches
- disseminate batches to peer workers
- report batch digests to the primary
- serve batches requested during synchronization
- remove committed payloads when notified by the primary

`Worker::spawn` is the crate's public entry point.
