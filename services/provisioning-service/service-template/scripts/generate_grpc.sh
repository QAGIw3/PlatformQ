#!/bin/bash
set -e

# This script generates the gRPC Python stubs from the .proto files.
# It should be run from the root of the project.
#
# USAGE:
# 1. Place your .proto files in the `libs/shared/protos` directory.
# 2. Uncomment the gRPC dependencies in `requirements.txt`.
# 3. Run this script from the project root:
#    bash services/__SERVICE_NAME__/scripts/generate_grpc.sh

# --- CONFIGURATION ---
# The directory where your .proto files are located.
PROTO_DIR="libs/shared/protos"

# The directory where the generated Python files will be placed.
GENERATED_DIR="services/__SERVICE_NAME__/app/grpc_generated"

# --- SCRIPT ---
if [ ! -d "$PROTO_DIR" ] || [ -z "$(ls -A $PROTO_DIR/*.proto 2>/dev/null)" ]; then
    echo "No .proto files found in ${PROTO_DIR}. Skipping gRPC generation."
    exit 0
fi

echo "Found .proto files in ${PROTO_DIR}. Generating gRPC code..."

# Ensure the output directory exists
mkdir -p "${GENERATED_DIR}"

# Generate the gRPC code for all .proto files found
python3 -m grpc_tools.protoc \
    -I"${PROTO_DIR}" \
    --python_out="${GENERATED_DIR}" \
    --grpc_python_out="${GENERATED_DIR}" \
    "${PROTO_DIR}"/*.proto

# Create an __init__.py file in the generated directory to make it a package
touch "${GENERATED_DIR}/__init__.py"

echo "gRPC code generated successfully in ${GENERATED_DIR}." 