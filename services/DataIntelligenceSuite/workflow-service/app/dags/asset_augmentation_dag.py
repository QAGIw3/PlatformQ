from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import httpx
from platformq_shared.storage import StorageProxy
import tempfile
import os
import subprocess

logger = logging.getLogger(__name__)

def download_asset(asset_uri, temp_dir):
    proxy = StorageProxy()
    local_path = proxy.download(asset_uri, temp_dir)
    return local_path

def process_with_blender(local_path, output_dir):
    cmd = f'python /path/to/blender_processor.py --input {local_path} --output {output_dir} --mode render'
    return cmd

def register_derived_asset(original_asset_id, derived_path, asset_type='render'):
    api_url = 'http://digital-asset-service:8000/api/v1/digital-assets'
    payload = {
        'original_id': original_asset_id,
        'derived_path': derived_path,
        'asset_type': asset_type,
        'relationship': 'DERIVED_FROM'
    }
    response = httpx.post(api_url, json=payload)
    response.raise_for_status()
    return response.json()

with DAG(
    dag_id='digital_asset_processing',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    def process_asset(**kwargs):
        conf = kwargs['dag_run'].conf
        asset_uri = conf.get('asset_uri')
        asset_type = conf.get('asset_type')
        asset_id = conf.get('asset_id')

        if asset_type != 'cad-file':
            logger.info(f'Skipping augmentation for asset type {asset_type}')
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = download_asset(asset_uri, temp_dir)
            output_dir = temp_dir + '/output'
            os.mkdir(output_dir)

            # Run Blender processing
            subprocess.run(process_with_blender(local_path, output_dir), shell=True, check=True)

            # Upload derived asset
            proxy = StorageProxy()
            derived_uri = proxy.upload(output_dir + '/render.png', 'derived_assets')

            # Register derived asset
            register_derived_asset(asset_id, derived_uri)

    augmentation_task = PythonOperator(
        task_id='augment_asset',
        python_callable=process_asset,
        provide_context=True
    ) 