# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY main.py .

# Run Python application
CMD ["python", "main.py"]