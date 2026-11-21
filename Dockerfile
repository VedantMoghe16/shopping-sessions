FROM debian:bookworm-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/ ./
ARG TARGETARCH
RUN if [ "$TARGETARCH" = amd64 ]; then set -- -march=x86-64-v3; else set --; fi && g++ "$@" -std=c++17 -O3 -DNDEBUG -pthread main.cpp -o /shopping-native

FROM python:3.12-slim-bookworm
WORKDIR /app
COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --only-binary=:all: -r requirements.txt
COPY --from=build /shopping-native ./
COPY pipeline/pipeline.py ./pipeline.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
ENTRYPOINT ["python3", "/app/pipeline.py"]
