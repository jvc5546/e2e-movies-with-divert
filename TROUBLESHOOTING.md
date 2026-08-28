## Testing Your Divert Setup

### 1. Test Header Propagation Locally

```bash
# Start each service and verify header logging
curl -H "baggage: okteto-divert=test" http://localhost:8080/catalog
```

### 2. Test Divert in Okteto

```bash
# Test with baggage header (routes to your namespace)
curl -H "baggage: okteto-divert=alice-movies" https://movies-movies-staging.okteto.dev

# Test direct access (bypasses Divert)
curl https://movies-alice-movies.okteto.dev
```

### 3. Verify Divert Resources

```bash
# Check Divert custom resources
kubectl get diverts -n alice-movies

# Check HTTPRoutes in shared namespace
kubectl get httproutes -n movies-staging | grep okteto-

# Check service endpoints
kubectl get endpoints -n alice-movies
```

### 4. Browser Testing

1. Open the shared staging URL: `https://movies-movies-staging.okteto.dev`
2. Add query parameter: `?baggage=okteto-divert%3Dalice-movies`
3. Verify your service changes appear
4. Check that other services work normally (shared from staging)


## Troubleshooting

### Issue: Services can't communicate

**Solution**: Verify network policies allow cross-namespace communication
```bash
kubectl get networkpolicies -n alice-movies
kubectl get networkpolicies -n movies-staging
```

### Issue: Baggage header not propagating

**Solution**: Check service logs for header values
```bash
kubectl logs -n alice-movies deployment/frontend -f
```

### Issue: Divert routing not working

**Solution**: Verify Divert custom resource is created
```bash
kubectl describe divert -n alice-movies
kubectl get httproutes -n movies-staging
```

### Issue: Can't connect to shared databases

**Solution**: Verify service discovery and DNS
```bash
kubectl run -it --rm debug --image=nicolaka/netshoot --restart=Never -- nslookup mongodb.movies-staging.svc.cluster.local
```

### Issue: Linkerd not injecting sidecar (Nginx driver only)

**Solution**: Ensure shared namespace has `okteto-shared` label for Linkerd sidecar injection
```bash
kubectl get namespace movies-staging -o yaml | grep okteto-shared
```

**Note**: This issue only applies when using the Nginx driver. Istio driver does not require Linkerd.

## Shared-Subset Kafka Isolation (`okteto.shared.yaml` + `okteto.personal.yaml`)

This topology shares only `catalog` + Kafka; every developer deploys their own `rent-api` + `worker` + local Postgres via `okteto.personal.yaml`, publishing/consuming the *same* shared Kafka topics. Isolation depends on each worker's consumer-group filter, not on separate infrastructure per developer.

### Issue: rentals never show up for a developer

**Likely cause**: the worker's baggage filter (`rentals/worker/pkg/kafka/kafka.go`, `shouldProcessMessage`) is rejecting every message — usually because `OKTETO_DIVERTED_ENVIRONMENT` doesn't match what `rent-api` is tagging messages with.

```bash
# Confirm the worker's own namespace/divert key match
kubectl exec -n <your-namespace> deploy/worker -- env | grep -E 'KUBERNETES_NAMESPACE|OKTETO_DIVERTED_ENVIRONMENT'

# Watch the worker accept/reject messages live
kubectl logs -n <your-namespace> deploy/worker -f
# "Successfully created/updated rental" = accepted
# "Not processing message, it belongs to a diverted worker" = rejected (expected for other devs' events)
```

### Issue: verifying two developers' rentals don't leak into each other

```bash
# Each personal worker gets its own consumer group over the shared topics —
# confirm both groups exist and are independently making progress
kubectl exec -n <shared-namespace> deploy/kafka -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list | grep movies-worker-group

kubectl exec -n <shared-namespace> deploy/kafka -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group movies-worker-group-<namespace>
```
If dev A's Postgres rows ever show a different namespace in the `namespace` column (visible directly in the UI's "Processed by" badge, or via `GET /api/rent`), the filter isn't working — check the log lines above first.
