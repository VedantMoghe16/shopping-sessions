FROM python:3.12-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /src/requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: -r /src/requirements.txt
WORKDIR /src
COPY src/ ./
ARG TARGETARCH
RUN arrow_dir="$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["platlib"] + "/pyarrow")')" \
    && mkdir -p /app/licenses/pyarrow \
    && if [ "$TARGETARCH" = amd64 ]; then set -- -march=x86-64-v3; else set --; fi \
    && g++ "$@" -std=c++17 -O3 -DNDEBUG -pthread -I"$arrow_dir/include" main.cpp \
        "$arrow_dir/libparquet.so.2000" "$arrow_dir/libarrow.so.2000" -Wl,-rpath,'$ORIGIN' -o /app/shopping-native \
    && cp "$arrow_dir/libparquet.so.2000" "$arrow_dir/libarrow.so.2000" /app/ \
    && cp "$arrow_dir"-*.dist-info/LICENSE.txt "$arrow_dir"-*.dist-info/NOTICE.txt /app/licenses/pyarrow/ \
    && cp /usr/share/doc/nlohmann-json3-dev/copyright /app/licenses/nlohmann-json.txt

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/ ./
USER 1000:1000
ENTRYPOINT ["/app/shopping-native"]
