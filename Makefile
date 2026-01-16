PYTHON ?= python
PROTO_SRC := bidstorm/proto/bidding.proto

.PHONY: install proto train seed test lint exchange dsp analytics up down load

install:
	$(PYTHON) -m pip install -e ".[dev,load]"

# Generates bidstorm/proto/bidding_pb2.py and bidding_pb2_grpc.py.
# Running from the repo root with -I . makes the generated grpc module import
# `from bidstorm.proto import bidding_pb2`, so the package works without sys.path hacks.
proto:
	$(PYTHON) -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. --pyi_out=. $(PROTO_SRC)

train:
	$(PYTHON) -m bidstorm.ml.train_ctr --rows 200000 --out models/ctr.joblib

seed:
	$(PYTHON) scripts/seed_dmp.py --users 50000

test: proto
	$(PYTHON) -m pytest -q

lint:
	ruff check bidstorm tests

exchange:
	uvicorn bidstorm.exchange.app:app --host 0.0.0.0 --port 8000 --loop uvloop

dsp:
	$(PYTHON) -m bidstorm.dsp.server

analytics:
	$(PYTHON) -m bidstorm.analytics.consumer

up:
	docker compose up --build -d

down:
	docker compose down -v

load:
	locust -f loadtest/locustfile.py --host http://localhost:8000
