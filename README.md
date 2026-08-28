# Movies — Divert Demo (Shared Subset + State Isolation)

This demo shows [Okteto Divert](https://www.okteto.com/docs/core/divert/) used in a topology where the shared environment runs only a **subset** of services, and every developer deploys the rest personally — including a stateful service that safely shares infrastructure with everyone else, without a database per developer and without VolumeSnapshot resets.

## What's running where

**Shared namespace** — `okteto.shared.yaml`
- `catalog` (+ MongoDB) — read-only movie reference data, safe to share since nothing ever writes to it
- `Kafka` — the event bus for rentals

**Personal namespace** — `okteto.personal.yaml` (every developer deploys this)
- `frontend`, `api`, `rent-api`, `worker`, and their own local Postgres
- Diverts `catalog` from shared over Divert's HTTP service mirroring (no extra config needed)
- Connects to the shared `Kafka` over an explicit cross-namespace address (Divert's header routing only applies to HTTP, not raw TCP)

## How state isolation works

Every personal `rent-api` publishes rental/return events to the *same* shared Kafka topics, tagging each message with its own namespace. Every personal `worker` has its own consumer group, so it sees the *entire* shared event stream — but only writes messages tagged for its own namespace into its own Postgres, ignoring everyone else's. The event log is shared; each developer's rental state is a private, isolated projection of only their own events. A "Processed by: `<namespace>`" badge on each rental in the UI (and in `GET /api/rent`) shows which worker actually wrote that row.

## Try it

1. **Deploy the shared subset**
   ```bash
   okteto deploy -f okteto.shared.yaml --namespace movies-shared
   ```

2. **Deploy your personal environment**, diverting from shared
   ```bash
   okteto deploy -f okteto.personal.yaml --namespace movies-dev-a -v OKTETO_SHARED_NAMESPACE=movies-shared
   ```

3. **Rent a movie** at `https://movies-movies-dev-a.<domain>` — the rental card shows `Processed by: movies-dev-a`.

4. **Deploy a second personal environment** the same way (`movies-dev-b`), rent a different movie there, and refresh dev-a's tab — its list is unaffected. Both `rent-api`s published to the same shared Kafka topics; each worker only materialized its own.

5. **Confirm it in the logs** — tail both workers while renting in either app:
   ```bash
   kubectl logs -n movies-dev-a deploy/worker -f
   kubectl logs -n movies-dev-b deploy/worker -f
   ```
   Both workers see every event; each only acts on its own (`Successfully created/updated rental`) and explicitly skips the other's (`Not processing message, it belongs to a diverted worker`).

6. **Run the e2e suites**
   ```bash
   okteto test e2e -f okteto.shared.yaml --namespace movies-shared      # catalog-only checks
   okteto test e2e -f okteto.personal.yaml --namespace movies-dev-a     # full app + rent/return round-trip
   ```

## Cleanup
```bash
okteto namespace delete movies-shared movies-dev-a movies-dev-b
```

## License
Apache License 2.0 — see [LICENSE](LICENSE)
