FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your scripts (ensure functions.py is in the same directory)
COPY customer_tag_log.py .
COPY functions.py .

# Command to run the script
CMD ["python", "customer_tag_log.py"]