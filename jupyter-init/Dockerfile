# Stage 1: Build the environment using Miniconda
FROM continuumio/miniconda3:4.10.3 AS build

FROM jupyter/pyspark-notebook:latest

# Copy the requirements.txt into the Docker image
COPY requirements.txt /tmp/
# Install required Python packages with pip (that are not compatible in conda)
COPY requirements_pip.txt /tmp/

# Create a new conda environment using the requirements.txt
RUN conda create --name crypto_env --file /tmp/requirements.txt -y

# Activate the environment and install the pip libraries (ccxt)
RUN /bin/bash -c "source activate crypto_env && \
    python -m ipykernel install --user --name=crypto_env --display-name 'Python (crypto_env)' && \
    pip install --upgrade pip && \
    pip install -r /tmp/requirements_pip.txt"

# Activate the environment by default when entering the container
RUN echo "conda activate crypto_env" >> ~/.bashrc

# Clean up conda cache to reduce image size
RUN conda clean --all -f -y

# Optional: Set the working directory (if needed)
WORKDIR /home/jovyan/work/crypto-bot

