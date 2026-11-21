# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Build context = src/ (main.py imports common/):
#   docker build -f src/seismic_ws/Dockerfile src
COPY seismic_ws/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY common/ common/
COPY seismic_ws/main.py .

# Run Python application
CMD ["python", "main.py"]
