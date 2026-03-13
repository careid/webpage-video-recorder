# Webpage Video Recorder - Docker Image
# Contains all dependencies for recording webpages with audio

FROM node:20-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99 \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # X Virtual Framebuffer
    xvfb \
    x11-xkb-utils \
    xfonts-base \
    xfonts-100dpi \
    xfonts-75dpi \
    xfonts-scalable \
    # Video recording
    ffmpeg \
    # Audio system
    pulseaudio \
    pulseaudio-utils \
    # Chromium browser
    chromium \
    chromium-sandbox \
    # Chromium dependencies
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    # Utilities
    ca-certificates \
    wget \
    curl \
    # Cleanup
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create app directory
WORKDIR /app

# Create recordings directory
RUN mkdir -p /app/recordings

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm install --production

# Copy application files
COPY lib/ ./lib/
COPY record.js ./record.js

# Make record.js executable
RUN chmod +x ./record.js

# Create PulseAudio configuration
RUN mkdir -p /root/.config/pulse && \
    echo "default-sample-format = s16le" > /root/.config/pulse/daemon.conf && \
    echo "default-sample-rate = 44100" >> /root/.config/pulse/daemon.conf && \
    echo "exit-idle-time = -1" >> /root/.config/pulse/daemon.conf

# Set up entrypoint
ENTRYPOINT ["node", "record.js"]

# Default command (shows help)
CMD ["--help"]

# Example usage:
# docker run --rm -v $(pwd)/recordings:/app/recordings webpage-recorder \
#   --url "https://example.com/video" \
#   --duration 30 \
#   --output /app/recordings/recording.mp4
