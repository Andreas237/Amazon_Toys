#!/bin/bash

export S3_BUCKET_NAME="doghouse-df071646-aba6-4927-9be5-9a3e1545d6a8"

pushd /root/ubiquiti_video_backup
source .venv/bin/activate 2>&1 1>/dev/null
python3 main.py
deactivate
popd
