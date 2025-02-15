# Use the official Python image from Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application code and requirements to the container
COPY app.py /app/
COPY requirements.txt /app/

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Optional: Add wait-for-it.sh to ensure that MySQL is ready
# (download this script and place it in the same directory as Dockerfile)
COPY wait-for-it.sh /wait-for-it.sh
#RUN chmod +x /wait-for-it.sh

# Expose the port the Dash app will run on
EXPOSE 8050

# Run the Dash application, but wait for MySQL to be ready first
#CMD ["./wait-for-it.sh", "mysql:3306", "--", "python", "app.py"]
CMD ["python", "app.py"]

