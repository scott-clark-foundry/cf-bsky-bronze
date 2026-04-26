# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Copy the prebuilt uv binary from the official image (faster + more reproducible than curl install)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Non-root user
RUN useradd -u 10001 -m bluesky
WORKDIR /app

# Project files (lockfile + pyproject first for layer-cache friendliness)
COPY --chown=bluesky:bluesky pyproject.toml uv.lock ./
COPY --chown=bluesky:bluesky src ./src

# Install dependencies + the project itself, frozen against uv.lock
RUN uv sync --no-dev --frozen
ENV PATH="/app/.venv/bin:${PATH}"

USER bluesky
ENTRYPOINT ["bluesky-ingest"]
