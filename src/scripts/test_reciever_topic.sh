#!/bin/bash

USERNAME="username"
PASSWORD="password"

# Путь к сертификату
CERT_PATH="/data/CA.pem"
CERT_DIR="/data"
CERT_URL="https://storage.yandexcloud.net/cloud-certs/CA.pem"

BROKER="rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
# --- ЭТО ЕДИНСТВЕННОЕ ИЗМЕНЕНИЕ ---
# Устанавливаем ваш ВЫХОДНОЙ топик
TOPIC="student.topic.cohort39.KvaytkovskyAleksey_out"

# Скачиваем сертификат, если его нет
if [ ! -f "$CERT_PATH" ]; then
  echo "Certificate not found at $CERT_PATH. Downloading..."
  wget "$CERT_URL" -P "$CERT_DIR"
  echo "Certificate downloaded."
else
  echo "Certificate already exists at $CERT_PATH."
fi


echo "Starting Kafka consumer for topic: $TOPIC"
echo "Press [Ctrl+C] to stop."

# Флаг -C переключает kafkacat в режим потребителя (consumer)
kafkacat -b "$BROKER" \
    -C \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username="$USERNAME" \
    -X sasl.password="$PASSWORD" \
    -X ssl.ca.location="$CERT_PATH" \
    -t "$TOPIC" \
    -f '\nKey: %k\nValue: %s\n'