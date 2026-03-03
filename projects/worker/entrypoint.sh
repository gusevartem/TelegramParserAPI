#!/bin/sh
export WORKER_ID=WORKER_$(cat /etc/hostname)
exec "$@"