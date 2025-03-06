# Pull in base image
FROM docker.io/library/python:3.12-slim

# Image information
LABEL org.opencontainers.image.source=https://github.com/AV-EFI/efi-agent
LABEL org.opencontainers.image.description="Register PIDs according to the AVefi schema"
LABEL org.opencontainers.image.licenses=MIT

# Add git and ncurses-term for better usability in terminal
RUN apt-get update && apt-get install -y --no-install-recommends \
    ncurses-term git \
    && rm -rf /var/lib/apt/lists/*

# Clone repository into app directory
RUN git clone https://github.com/AV-EFI/efi-agent.git app

# Set current working directory
WORKDIR /app

# Install app in developer mode
RUN pip install --no-cache-dir -e .

# Set default entry point
ENTRYPOINT [ "efi-agent" ]
