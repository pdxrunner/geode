#!/bin/sh
set -e

service docker start

exec "$@"
