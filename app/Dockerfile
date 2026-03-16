FROM debian:bookworm-slim AS bridge-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc cmake make git ca-certificates libc6-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY paho.mqtt.c/ paho.mqtt.c/
COPY bridge/      bridge/

RUN cmake -S paho.mqtt.c -B paho.mqtt.c/build \
        -DPAHO_BUILD_STATIC=OFF \
        -DPAHO_WITH_SSL=ON \
        -DPAHO_BUILD_SAMPLES=OFF \
        -DPAHO_BUILD_DOCUMENTATION=OFF \
    && cmake --build paho.mqtt.c/build

RUN make -C bridge

FROM debian:bookworm-slim AS runtime

ENV LANG=C.UTF-8 \
    MQTT_BRIDGE_HOST=localhost

RUN apt-get update && apt-get install -y --no-install-recommends \
    swi-prolog \
    && rm -rf /var/lib/apt/lists/*

COPY --from=bridge-builder /build/paho.mqtt.c/build/src/libpaho-mqtt3as.so* /usr/local/lib/
COPY --from=bridge-builder /build/bridge/mqtt_bridge /usr/local/bin/mqtt_bridge
RUN ldconfig

WORKDIR /app
COPY prolog/ prolog/
COPY app/*.pl .
