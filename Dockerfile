FROM python:3.12-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/ ./
COPY tools/train.py ./train.py
RUN set -eu; \
    case "$(g++ -dumpmachine)" in \
        x86_64*) \
            set -- -march=x86-64-v3; \
            printf '%s\n' 'int main() { return !__builtin_cpu_supports("x86-64-v3"); }' \
                | g++ -x c++ - -o /tmp/isa-check;; \
        *) set --;; \
    esac; \
    if [ ! -x /tmp/isa-check ] || /tmp/isa-check; then \
        python3 train.py /tmp/training; \
        g++ "$@" -std=c++17 -O3 -flto=4 -DNDEBUG -pthread \
            -fprofile-generate=/tmp/profile -fprofile-update=atomic -Wl,-u,__gcov_dump \
            main.cpp -o /tmp/shopping-native; \
        NCPU=4 /tmp/shopping-native /tmp/training /tmp/training-output; \
        set -- "$@" -fprofile-use=/tmp/profile -fprofile-correction -fprofile-partial-training -Werror=missing-profile; \
    fi; \
    g++ "$@" -std=c++17 -O3 -flto=4 -DNDEBUG -pthread main.cpp -o /tmp/shopping-native; \
    mkdir -p /app/licenses; \
    mv /tmp/shopping-native /app/; \
    cp /usr/share/doc/nlohmann-json3-dev/copyright /app/licenses/nlohmann-json.txt

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/ ./
USER 1000:1000
ENTRYPOINT ["/app/shopping-native"]
