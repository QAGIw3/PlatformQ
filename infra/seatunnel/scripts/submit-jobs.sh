#!/bin/bash

# SeaTunnel Job Submission Script
# Submits all configured SeaTunnel jobs to the cluster

set -e

SEATUNNEL_HOME="/opt/seatunnel"
CONFIG_DIR="${SEATUNNEL_HOME}/config"
SEATUNNEL_BIN="${SEATUNNEL_HOME}/bin/seatunnel.sh"

echo "Starting SeaTunnel job submission..."
echo "SeaTunnel Home: ${SEATUNNEL_HOME}"
echo "Config Directory: ${CONFIG_DIR}"

# Wait for SeaTunnel master to be ready
echo "Waiting for SeaTunnel master to be ready..."
for i in {1..30}; do
    if curl -f http://seatunnel-master:5070/health > /dev/null 2>&1; then
        echo "SeaTunnel master is ready!"
        break
    fi
    echo "Waiting... (${i}/30)"
    sleep 10
done

# Function to submit a job
submit_job() {
    local config_file=$1
    local job_name=$(basename ${config_file} .conf)
    
    echo "----------------------------------------"
    echo "Submitting job: ${job_name}"
    echo "Config file: ${config_file}"
    
    # Check if job is already running
    if curl -s http://seatunnel-master:5070/api/v1/jobs | grep -q "\"name\":\"${job_name}\""; then
        echo "Job ${job_name} is already running, skipping..."
        return 0
    fi
    
    # Submit the job
    ${SEATUNNEL_BIN} --config ${config_file} \
        --master seatunnel-master:5070 \
        --deploy-mode cluster \
        --name ${job_name}
    
    if [ $? -eq 0 ]; then
        echo "Job ${job_name} submitted successfully!"
    else
        echo "Failed to submit job ${job_name}"
        return 1
    fi
}

# Submit all jobs
echo "Submitting SeaTunnel jobs..."

# Compute Futures data sync job
if [ -f "${CONFIG_DIR}/compute-futures-sync.conf" ]; then
    submit_job "${CONFIG_DIR}/compute-futures-sync.conf"
fi

# Platform CDC sync job
if [ -f "${CONFIG_DIR}/platform-cdc-sync.conf" ]; then
    submit_job "${CONFIG_DIR}/platform-cdc-sync.conf"
fi

# Add more jobs as needed
for config_file in ${CONFIG_DIR}/*.conf; do
    if [ -f "${config_file}" ]; then
        # Skip already processed configs
        if [[ "${config_file}" != *"compute-futures-sync.conf" ]] && \
           [[ "${config_file}" != *"platform-cdc-sync.conf" ]]; then
            submit_job "${config_file}"
        fi
    fi
done

echo "----------------------------------------"
echo "Job submission complete!"

# Monitor job status
echo "Monitoring job status..."
while true; do
    echo "----------------------------------------"
    echo "Current job status:"
    curl -s http://seatunnel-master:5070/api/v1/jobs | jq '.jobs[] | {name: .name, status: .status, startTime: .startTime}'
    
    # Check if all jobs are running
    failed_jobs=$(curl -s http://seatunnel-master:5070/api/v1/jobs | jq -r '.jobs[] | select(.status == "FAILED") | .name')
    
    if [ ! -z "${failed_jobs}" ]; then
        echo "WARNING: The following jobs have failed:"
        echo "${failed_jobs}"
    fi
    
    # Sleep for monitoring interval
    sleep 60
done 