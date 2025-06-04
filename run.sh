#!/bin/bash

IMAGE_NAME=zanon-eval

docker run --rm \
  -v "$(pwd)/src:/app/src" \
  -v "$(pwd)/res:/app/res" \
  -v "$(pwd)/tests:/app/tests" \
  -v "$(pwd)/data_xes:/app/data_xes" \
  -v "$(pwd)/main.py:/app/main.py"\
  -e PYTHONPATH=/app/src \
  $IMAGE_NAME
