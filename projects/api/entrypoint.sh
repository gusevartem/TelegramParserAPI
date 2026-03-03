#!/bin/sh
export WORKER_ID=API_$(cat /etc/hostname)
exec "$@"