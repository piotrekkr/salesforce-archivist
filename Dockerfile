# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:74b8fe8ec5931f3930cfb6c87b46aeb1dbd497a609f6abf860fd0f4390f8b040

WORKDIR /archivist

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# fetch container running user from build args
ARG ARCHIVIST_UID=1000
ARG ARCHIVIST_GID=1000

RUN <<EOF
    # add archivist user that will be used to run container by default
    groupadd --gid $ARCHIVIST_GID archivist
    useradd --create-home --gid $ARCHIVIST_GID --uid $ARCHIVIST_UID archivist --no-log-init
    chown -R archivist:archivist /archivist
EOF

USER archivist

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,uid=$ARCHIVIST_UID,gid=$ARCHIVIST_GID,target=/home/archivist/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY --chown=archivist:archivist . .

RUN --mount=type=cache,uid=$ARCHIVIST_UID,gid=$ARCHIVIST_GID,target=/home/archivist/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/archivist/.venv/bin:$PATH"

ENTRYPOINT ["archivist"]

CMD ["--help"]
