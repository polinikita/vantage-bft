# Primary

The `primary` crate runs consensus and coordinates with local workers.

## Responsibilities

- receive batch digests from workers
- build and exchange protocol data
- order committed output
- request missing dependencies
- notify workers about committed batches

## Protocols

- Autobahn logic is implemented by the primary core and committer modules.
- Vantage is implemented under `src/vantage/`.
- Simple-IT is implemented under `src/simpleit/`.
- `Primary::spawn` selects the configured protocol and starts its tasks.

The public message types and `Primary` entry point are exported from
`src/lib.rs`.
