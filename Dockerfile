FROM python:3.11-slim

WORKDIR /workspace

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your production pipeline code into the container
COPY pipeline.py .

# Explicit execution instruction
CMD ["python", "pipeline.py"]