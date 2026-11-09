FROM golang:1.26-bookworm
ARG USER_UID=1000
ARG USER_GID=1000
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git make python3 sudo \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "$USER_GID" developer \
    && useradd --uid "$USER_UID" --gid "$USER_GID" --create-home --shell /bin/bash developer \
    && printf 'developer ALL=(ALL) NOPASSWD:ALL\n' > /etc/sudoers.d/developer \
    && chmod 0440 /etc/sudoers.d/developer
ENV HOME=/home/developer
ENV GOPATH=/home/developer/go
ENV PATH=/home/developer/.local/bin:/home/developer/go/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
USER developer
WORKDIR /workspace
CMD ["sleep", "infinity"]
