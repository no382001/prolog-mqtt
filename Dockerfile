FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc cmake make git ca-certificates libc6-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# submodule is already present in the build context
COPY paho.mqtt.c/ paho.mqtt.c/
COPY bridge/ bridge/

# build Paho MQTT C (async, with SSL, no samples)
RUN cmake -S paho.mqtt.c -B paho.mqtt.c/build \
        -DPAHO_BUILD_STATIC=OFF \
        -DPAHO_WITH_SSL=ON \
        -DPAHO_BUILD_SAMPLES=OFF \
        -DPAHO_BUILD_DOCUMENTATION=OFF \
    && cmake --build paho.mqtt.c/build

# build the bridge
RUN make -C bridge

FROM debian:bookworm-slim AS runtime

COPY --from=builder /build/paho.mqtt.c/build/src/libpaho-mqtt3as.so* /usr/local/lib/
COPY --from=builder /build/bridge/mqtt_bridge /usr/local/bin/mqtt_bridge

RUN ldconfig

EXPOSE 7883

ENTRYPOINT ["mqtt_bridge"]
