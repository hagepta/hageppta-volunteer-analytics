# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY volunteer-analytics-cloud.py .

EXPOSE 8080

# Define the command to start the function
CMD ["functions-framework", "--target", "generate_and_save_plots", "--port", "8080"]