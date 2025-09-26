FROM python:3.12.8-slim-bookworm

# Install dependencies
# This first line is a hack until the python docker maintainers fix a bug introduced on 12/6/2024
RUN echo "deb http://deb.debian.org/debian/ bookworm-proposed-updates main" >> /etc/apt/sources.list

# Normal command to install all the components we need
RUN apt-get update && apt-get install -y \
    xvfb \
    chromium \
    chromium-driver \
    unzip \
    && rm -rf /var/lib/apt/lists/* 

# Set environment variables
# ENV DISPLAY=:99

# Copy your project code
COPY *.py /app/
COPY requirements.txt /app/
COPY config_debug.json /app/

# Install Python dependencies
WORKDIR /app
RUN pip install selenium pyvirtualdisplay 
RUN pip install -r requirements.txt

# Start Xvfb and Chrome
# CMD ["/bin/bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & google-chrome --headless --disable-gpu --remote-debugging-port=9222 & python your_script.py"]
CMD ["/bin/bash", "-c", "python3 scraper.py"]
