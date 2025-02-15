#!/bin/bash

set -e  # Exit immediately on error

# Load environment variables from .env
if [ -f "/app/.env" ]; then
  set -o allexport
  source /app/.env
  set +o allexport
else
  echo "Error: .env file not found!"
  exit 1
fi

# Authenticate with Bitwarden
if ! bw unlock --check; then
  echo "Unlocking Bitwarden vault..."
  export BW_SESSION=$(bw login --apikey --raw)
fi

# Retrieve secrets from Bitwarden
MYSQL_USER=$(bw secret get mysql_user 2>/dev/null) || { echo "Failed to get MYSQL_USER"; exit 1; }
MYSQL_ROOT_PASSWORD=$(bw secret get mysql_root_password 2>/dev/null) || { echo "Failed to get MYSQL_ROOT_PASSWORD"; exit 1; }
MYSQL_PASSWORD=$(bw secret get mysql_password 2>/dev/null) || { echo "Failed to get MYSQL_PASSWORD"; exit 1; }
COIN_API_KEY=$(bw secret get coin_api_key 2>/dev/null) || { echo "Failed to get COIN_API_KEY"; exit 1; }

# Export secrets to a shared environment file
cat <<EOF > /run/secrets/env_secrets.env
MYSQL_USER=$MYSQL_USER
MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD
MYSQL_PASSWORD=$MYSQL_PASSWORD
COIN_API_KEY=$COIN_API_KEY
EOF

echo "Secrets exported to /run/secrets/env_secrets.env"

# Keep container running to allow others to access the file
exec tail -f /dev/null


