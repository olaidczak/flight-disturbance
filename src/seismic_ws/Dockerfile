# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Build from the repository root:
# docker build -f src/seismic_ws/Dockerfile .
COPY src/seismic_ws/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common ./common
COPY src/seismic_ws/main.py ./main.py

# Run Python application
CMD ["python", "main.py"]
