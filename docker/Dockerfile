# One image serves the exchange, DSPs and analytics consumer; the command selects the role.
FROM python:3.11-slim AS build
WORKDIR /app
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY pyproject.toml README.md ./
COPY bidstorm ./bidstorm
COPY scripts ./scripts
RUN pip install --prefix=/install . grpcio-tools \
 && PYTHONPATH=/install/lib/python3.11/site-packages \
    python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. bidstorm/proto/bidding.proto \
 && pip install --prefix=/install --no-deps --force-reinstall . \
 # Bake a trained CTR model into the image so DSPs boot with a real model.
 && PYTHONPATH=/install/lib/python3.11/site-packages \
    python -m bidstorm.ml.train_ctr --rows 150000 --out /app/models/ctr.joblib

FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 DSP_MODEL_PATH=/app/models/ctr.joblib
COPY --from=build /install /usr/local
COPY --from=build /app/models ./models
COPY --from=build /app/scripts ./scripts
RUN useradd --create-home --uid 10001 bidstorm
USER bidstorm
EXPOSE 8000 50051
CMD ["uvicorn", "bidstorm.exchange.app:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--http", "httptools", "--no-access-log"]
