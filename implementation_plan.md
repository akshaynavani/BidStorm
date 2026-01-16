# BidStorm — Implementation Plan

Real-time bidding (RTB) ad exchange simulator. An exchange receives OpenRTB-style bid
requests over HTTP (FastAPI), enriches them with audience segments from a Redis-backed
DMP, fans them out concurrently over gRPC to a pool of DSP microservices under a hard
latency budget (<100 ms end-to-end, default 80 ms DSP tmax), runs a first- or
second-price auction, and publishes auction / impression / click events to Kafka for an
analytics consumer. DSPs price impressions using a scikit-learn CTR model.

**Tech:** Python 3.11, FastAPI, gRPC (grpc.aio), Redis, Kafka (aiokafka), scikit-learn,
Docker, Kubernetes, Locust, pytest.

## Key design decisions

- **OpenRTB-lite schema.** Pydantic models carry a pragmatic subset of OpenRTB 2.5
  (`id`, `imp[]`, `site`, `device`, `user`, `tmax`, `at`). `at=1` → first price,
  `at=2` → second price, matching the OpenRTB convention.
- **gRPC contract between exchange and DSPs** (`bidstorm/proto/bidding.proto`), stubs
  generated with `grpc_tools.protoc` at build time (Makefile + Dockerfiles).
- **Latency budget.** The exchange computes a per-request deadline =
  `min(request.tmax, EXCHANGE_TMAX_MS) - overhead`; each DSP call uses that as the gRPC
  timeout and `asyncio.wait` drops stragglers. Late/failed DSPs count as no-bid and are
  tracked in metrics. DMP lookups have their own small budget (default 5 ms) and degrade
  to "no segments" on timeout.
- **Auction.** Per-impression, floor-aware. Second price = max(second-highest bid,
  floor) + 0.01 increment, capped at the winner's bid. Ties broken by earliest response,
  then random for fairness.
- **CTR model.** Logistic regression over hashed features (`FeatureHasher`) trained on
  synthetic impression logs with planted segment/placement effects; serialized with
  joblib. DSP bid = pCTR × value-per-click × 1000 (CPM) × pacing multiplier, with a
  per-DSP budget and simple throttle pacing.
- **Analytics.** Exchange emits `auction`, `win`, `impression`, `click` events to Kafka
  topics; a consumer aggregates rolling counters (per DSP spend, win-rate, CTR) into
  Redis hashes exposed via `/stats`. Kafka is optional: a no-op publisher is used if
  `KAFKA_ENABLED=false`.

## Phases

### Phase 1 — Foundations: config, models, proto, auction engine
Deliverables
- Package skeleton, `pyproject.toml`, `Makefile` (proto generation, lint, test), `.env.example`.
- Settings via `pydantic-settings`.
- OpenRTB-lite request/response models.
- gRPC proto for `Bidder.Bid`.
- Auction engine (first/second price, floors, tie-breaking) + unit tests.

Files: `pyproject.toml`, `Makefile`, `.env.example`, `bidstorm/__init__.py`,
`bidstorm/config.py`, `bidstorm/models.py`, `bidstorm/proto/bidding.proto`,
`bidstorm/exchange/auction.py`, `tests/test_auction.py`, `tests/conftest.py`

### Phase 2 — DSP microservice & CTR model
Deliverables
- Synthetic impression-log generator and CTR trainer (scikit-learn logistic regression on
  hashed features, AUC / log-loss report).
- Feature extraction shared between training and serving.
- Bidder strategy: pCTR-based CPM, campaign targeting, budget + pacing.
- Async gRPC DSP server configurable per instance (name, value-per-click, budget,
  artificial latency for realism).
- Unit tests for features and bidder.

Files: `bidstorm/ml/features.py`, `bidstorm/ml/synth.py`, `bidstorm/ml/train_ctr.py`,
`bidstorm/dsp/ctr_model.py`, `bidstorm/dsp/bidder.py`, `bidstorm/dsp/server.py`,
`tests/test_features.py`, `tests/test_bidder.py`

### Phase 3 — Exchange: DMP enrichment, async gRPC fan-out, HTTP API
Deliverables
- Redis DMP client (user → segments, with timeout) + seeding script.
- DSP registry and gRPC fan-out with deadline propagation and straggler dropping.
- FastAPI app: `POST /openrtb2/auction`, `GET /healthz`, `GET /metrics`
  (Prometheus text format, latency histogram, timeouts).
- Tests for fan-out (fake in-process DSPs) and the API (fakeredis + fake fan-out).

Files: `bidstorm/exchange/dmp.py`, `bidstorm/exchange/registry.py`,
`bidstorm/exchange/fanout.py`, `bidstorm/exchange/metrics.py`,
`bidstorm/exchange/service.py`, `bidstorm/exchange/app.py`, `scripts/seed_dmp.py`,
`tests/test_dmp.py`, `tests/test_fanout.py`, `tests/test_api.py`

### Phase 4 — Kafka analytics pipeline & impression tracking
Deliverables
- Event schema + async Kafka publisher (aiokafka) with no-op fallback.
- Win-notice / impression / click tracking endpoints (`/track/imp`, `/track/click`).
- Analytics consumer aggregating events into Redis; `/stats` endpoint.
- Tests for event serialization and aggregation logic.

Files: `bidstorm/events/schema.py`, `bidstorm/events/publisher.py`,
`bidstorm/analytics/aggregator.py`, `bidstorm/analytics/consumer.py`,
`bidstorm/exchange/app.py`, `bidstorm/exchange/service.py`, `tests/test_events.py`,
`tests/test_aggregator.py`

### Phase 5 — Packaging, orchestration & load testing
Deliverables
- Dockerfiles for exchange, DSP, analytics consumer (proto generated in-image).
- `docker-compose.yml` with Redis, Kafka (KRaft), 3 DSPs, exchange, consumer, Locust.
- Kubernetes manifests (namespace, configmap, deployments, services, HPA).
- Locust load test generating realistic bid requests and tracking pixels.

Files: `docker/Dockerfile`, `docker-compose.yml`, `k8s/*.yaml`,
`loadtest/locustfile.py`

After the phases: `.gitignore`, `README.md` (architecture diagram, run & test docs).
