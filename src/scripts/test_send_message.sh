#!/bin/bash

USERNAME="username"
PASSWORD="password"

# Set certificate path and download URL
CERT_PATH="/data/CA.pem"
CERT_DIR="/data"
CERT_URL="https://storage.yandexcloud.net/cloud-certs/CA.pem"

BROKER="rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
TOPIC="student.topic.cohort39.KvaytkovskyAleksey_in"

# Download cert if it's not exists
if [ ! -f "$CERT_PATH" ]; then
  echo "Certificate not found at $CERT_PATH. Downloading..."
  # Download the certificate
  wget "$CERT_URL" -P "$CERT_DIR"
  echo "Certificate downloaded."
else
  echo "Certificate already exists at $CERT_PATH."
fi

# Define the message key and value
MESSAGE='key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}'

# Send the message to Kafka
# The 'echo' command pipes the message to kafkacat, which sends it and then exits.
echo "Sending message to topic: $TOPIC..."
echo "$MESSAGE" | kafkacat -b "$BROKER" \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username="$USERNAME" \
    -X sasl.password="$PASSWORD" \
    -X ssl.ca.location="$CERT_PATH" \
    -t "$TOPIC" \
    -K: \
    -P

echo "Message sent successfully."
