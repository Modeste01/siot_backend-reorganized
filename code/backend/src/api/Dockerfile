# FROM python:3.12-bookworm
# WORKDIR /api
# COPY requirements.txt .
# RUN pip install --upgrade pip
# RUN pip install -r requirements.txt
# COPY *.py .
# EXPOSE 8000
# CMD ["python", "main.py"]

FROM python:3.12.8-slim-bookworm

# Install dependencies
# This first line is a hack until the python docker maintainers fix a bug introduced on 12/6/2024
RUN echo "deb http://deb.debian.org/debian/ bookworm-proposed-updates main" >> /etc/apt/sources.list

# Normal command to install all the components we need
# RUN apt-get update && apt-get install -y \
#     xvfb \
#     chromium \
#     chromium-driver \
#     unzip \
#     && rm -rf /var/lib/apt/lists/* 


# Copy your project code
COPY *.py /app/
COPY requirements.txt /app/
COPY index.html /app/
COPY script.js /app/

# Install Python dependencies
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Start the API with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
