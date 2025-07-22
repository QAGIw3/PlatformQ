#!/bin/bash
set -e

# This script generates the gRPC Python stubs from the .proto files.
# It should be run from the root of the project.

# Define paths
PROTO_DIR="libs/shared/protos"
GENERATED_DIR="services/functions-service/app/grpc_generated"
PROTO_FILE="${PROTO_DIR}/connector.proto"

# Ensure the output directory exists
mkdir -p "${GENERATED_DIR}"

# Generate the gRPC code
echo "Generating gRPC code from ${PROTO_FILE}..."
python3 -m grpc_tools.protoc \
    -I"${PROTO_DIR}" \
    --python_out="${GENERATED_DIR}" \
    --grpc_python_out="${GENERATED_DIR}" \
    "${PROTO_FILE}"

# Create an __init__.py file in the generated directory to make it a package
touch "${GENERATED_DIR}/__init__.py"

echo "Done." 