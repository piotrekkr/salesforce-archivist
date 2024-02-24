# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm as base

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONFAULTHANDLER=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /archivist

# fetch container running user from build args
ARG ARCHIVIST_UID=1000
ARG ARCHIVIST_GID=1000

RUN <<EOF
    apt-get update
    apt-get install --no-install-recommends --yes curl git
    # add archivist user that will be used to run container by default
    groupadd --gid $ARCHIVIST_GID archivist
    useradd --create-home --gid $ARCHIVIST_GID --uid $ARCHIVIST_UID archivist --no-log-init
    # make archivist and owner of /opt/venv so it can write to it
    mkdir -p /opt/venv
    chown -R archivist:archivist /opt/venv /archivist
EOF

ENV POETRY_VERSION=1.7.1 \
    POETRY_VIRTUAL_ENV=/opt/venv/poetry

RUN --mount=type=cache,target=/root/.cache/pip/ <<EOF
    # create virtual env for poetry
    python -m venv $POETRY_VIRTUAL_ENV
    # install poetry
    $POETRY_VIRTUAL_ENV/bin/pip install poetry~=$POETRY_VERSION
    # make poetry available system wide
    ln -s $POETRY_VIRTUAL_ENV/bin/poetry /usr/local/bin/poetry
EOF

USER archivist

# set archivist virtual env path and update PATH to register virtual env binaries
# this will ensure poetry will usie archivist virtual env when installing packages
ENV VIRTUAL_ENV=/opt/venv/archivist
ENV PATH=$VIRTUAL_ENV/bin:$PATH

# create actual virtual env
RUN python -m venv $VIRTUAL_ENV

COPY --chown=archivist:archivist poetry.lock pyproject.toml ./

# install non dev dependencies
RUN --mount=type=cache,uid=$ARCHIVIST_UID,gid=$ARCHIVIST_GID,target=/home/archivist/.cache/pypoetry/ \
    poetry install --without dev --no-interaction --no-ansi --no-root --sync

COPY --chown=archivist:archivist . .

##############################
# Production
##############################
FROM base as prod

# install app as binary
RUN --mount=type=cache,uid=$ARCHIVIST_UID,gid=$ARCHIVIST_GID,target=/home/archivist/.cache/pypoetry/ \
    poetry install --without dev --no-interaction --no-ansi --sync

ENTRYPOINT ["archivist"]

CMD ["--help"]


##############################
# CI
##############################
FROM base as ci

# install additional dev dependencies
RUN --mount=type=cache,uid=$ARCHIVIST_UID,gid=$ARCHIVIST_GID,target=/home/archivist/.cache/pypoetry/ \
    poetry install --no-interaction --no-ansi --sync



##############################
# Development
##############################
FROM ci as dev

USER root

# Install development tools
RUN <<EOF
    apt-get update
    apt-get install --no-install-recommends --yes git curl gnupg ssh sudo vim
    usermod --shell /usr/bin/bash archivist
    echo 'archivist ALL=(root) NOPASSWD:ALL' > /etc/sudoers.d/archivist
    chmod 0440 /etc/sudoers.d/archivist
EOF

USER archivist
