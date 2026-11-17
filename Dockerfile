# syntax=docker/dockerfile:1
#
# Image for the OMS API (and the other hgraph-tools commands).
#
# hg_oap is installed from the fork checked out next to this repository, so the build
# needs it as a second build context:
#
#   docker build --build-context hg_oap=../hg_oap -t hgraph-oms .
#
# (docker compose passes it automatically; see compose.yaml.)
#
# hgraph publishes Linux wheels for x86-64 only, so the image is built for linux/amd64
# whatever the build machine is. On Apple Silicon it runs under emulation.

FROM --platform=linux/amd64 python:3.12-slim-bookworm AS build

COPY --from=ghcr.io/astral-sh/uv:0.7.3 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# pyproject.toml refers to hg_oap as ../hg_oap, so keep the same layout here
COPY --from=hg_oap . /src/hg_oap
WORKDIR /src/hgraph_platform_tools
COPY . .

# Exactly the versions in uv.lock; nothing editable, so /src is not needed at run time
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable \
        --extra api --extra ai --extra messaging --extra oap --extra postgres


FROM --platform=linux/amd64 python:3.12-slim-bookworm

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin oms \
    && mkdir /data \
    && chown oms:oms /data

COPY --from=build /opt/venv /opt/venv
COPY scripts /app/scripts

# All state (order log, bookings, static data, entitlements, booked trade files) lives in /data
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    API_DB_DIR=/data \
    ENTITLEMENTS_DB_PATH=/data/hgraph_entitlements.db

USER oms
WORKDIR /data
VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen(f\"http://127.0.0.1:{os.environ['API_PORT']}/api/v1/health\", timeout=3)"]

CMD ["hgraph-tools", "serve"]
