#!/bin/bash

# Ожидание запуска MinIO
until curl -s http://minio:9000/minio/health/live; do
  echo "Waiting for MinIO..."
  sleep 5
done

# Настройка mc клиента
mc alias set myminio http://minio:9000 minioadmin minioadmin

# Создание bucket'ов
mc mb myminio/warehouse
mc mb myminio/checkpoints
mc mb myminio/temp

# Настройка политик доступа
mc anonymous set download myminio/warehouse

echo "MinIO setup completed!"