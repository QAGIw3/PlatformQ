#!/bin/bash
set -e

# This script generates the gRPC Python stubs from the .proto files.
# It should be run from the root of the project.

PROTO_DIR="libs/shared/protos"
GENERATED_DIR="services/notification-service/app/grpc_generated"

# Ensure the output directory exists
mkdir -p "${GENERATED_DIR}"

# Generate the gRPC code
python3 -m grpc_tools.protoc \
    -I"${PROTO_DIR}" \
    --python_out="${GENERATED_DIR}" \
    --grpc_python_out="${GENERATED_DIR}" \
    "${PROTO_DIR}"/graph_intelligence.proto

touch "${GENERATED_DIR}/__init__.py" 