# syntax=docker/dockerfile:1.7
#
# Supersee app image. Pairs with compose.yaml; the `app` service builds
# from this file with context `.`.
#
# In development, compose.yaml bind-mounts ./supersee, ./migrations, and
# ./fixtures over the image's copies, so source edits are picked up live
# (after `docker compose restart app`, or with --reload — see CMD note).
# In production builds the bind mounts are removed and the COPY'd source
# is what actually runs.
#
# PREREQUISITE for a successful build: ./supersee/__init__.py must exist
# in the repo root. That's the next scaffolding step after this file.

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# System deps:
#   - curl: HEALTHCHECK probe
#   - ca-certificates: TLS for outbound calls (Anthropic, OpenSanctions, XRPL WSS)
# psycopg 3 ships binary wheels via the [binary] extra, so libpq-dev and
# build-essential are not needed at the moment.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Project metadata + source. Single COPY/install pair keeps the Dockerfile
# legible at this stage of the project; revisit with a stub-package trick
# if pip-resolve time starts to dominate CI rebuilds.
COPY pyproject.toml README.md ./
COPY supersee ./supersee

RUN pip install -e ".[dev]"

# Non-root runtime user.
RUN useradd --create-home --uid 1000 --shell /bin/bash supersee \
 && chown -R supersee:supersee /app
USER supersee

EXPOSE 8000

# Wait long enough for FastAPI lifespan to: open the Postgres pool, apply
# migrations on first boot, connect to xrplcluster, and start the four
# background tasks. 30s start_period is generous but cheap.
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=4 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

# uvicorn boots the FastAPI app; the app's lifespan hook starts the
# ingestor, scorer, langgraph_app, and scheduler as background tasks
# (see ARCHITECTURE.md). For hot-reload during dev, override in compose
# with: command: ["uvicorn", "supersee.api:app", "--host", "0.0.0.0",
#                 "--port", "8000", "--reload", "--reload-dir", "/app/supersee"]
CMD ["uvicorn", "supersee.api:app", "--host", "0.0.0.0", "--port", "8000"]
