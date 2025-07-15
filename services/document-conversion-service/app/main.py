import pulsar
import avro.schema
import avro.io
import io
import logging
import requests
import os
import sys
import subprocess
import tempfile

if "/app" not in sys.path:
    sys.path.append("/app")
    
from shared_lib.config import ConfigLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
PULSAR_URL = settings.get("PULSAR_URL")
SUBSCRIPTION_NAME = "conversion-service-sub"
TOPIC_NAME = "persistent://public/default/document-conversion-requests"
SCHEMA_PATH = "schemas/conversion_requested.avsc"

def convert_document(source_url: str, output_format: str) -> str:
    """Downloads a document, converts it using LibreOffice, and returns the path to the converted file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download the source file
        source_filename = os.path.basename(source_url)
        local_path = os.path.join(tmpdir, source_filename)
        logger.info(f"Downloading {source_url} to {local_path}")
        with requests.get(source_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        
        # Run LibreOffice conversion
        logger.info(f"Converting {local_path} to {output_format}")
        subprocess.run(
            ["soffice", "--headless", "--convert-to", output_format, "--outdir", tmpdir, local_path],
            check=True
        )
        
        # Determine the output path
        base_name = os.path.splitext(source_filename)[0]
        output_path = os.path.join(tmpdir, f"{base_name}.{output_format}")
        
        if not os.path.exists(output_path):
            raise RuntimeError("Conversion failed: Output file not found.")
            
        logger.info(f"Conversion successful. Output at {output_path}")
        # In a real app, we would read this file and return its content
        # or upload it to a destination like Nextcloud/OpenKM.
        return output_path

def main():
    logger.info("Starting Document Conversion Service...")
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME)
    
    try:
        schema = avro.schema.parse(open(SCHEMA_PATH).read())
        while True:
            msg = consumer.receive()
            try:
                # ... (deserialization logic) ...
                data = ... 
                logger.info(f"Received conversion request: {data}")
                
                converted_file_path = convert_document(
                    source_url=data["source_document_url"],
                    output_format=data["output_format"]
                )
                
                # Here, you would upload the converted file and/or
                # publish a 'conversion_completed' event.
                
                consumer.acknowledge(msg)
            except Exception as e:
                consumer.negative_acknowledge(msg)
                logger.error(f"Failed to process message: {e}")
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    main() 