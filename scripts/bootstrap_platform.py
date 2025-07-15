import requests
import logging
import hvac
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KONG_ADMIN_URL = "http://localhost:8001"
MINIO_URL = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

def get_jwt_secret_from_vault():
    """Fetches the JWT secret from Vault."""
    client = hvac.Client(url="http://localhost:8200", token="root")
    assert client.is_authenticated()
    response = client.secrets.kv.v2.read_secret_version(path="platformq/jwt")
    return response['data']['data']['secret_key']

def bootstrap_minio():
    """Creates the necessary buckets in Minio."""
    logger.info("Connecting to Minio...")
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    buckets = ["mlflow", "platformq-datalake"]
    for bucket in buckets:
        try:
            found = client.bucket_exists(bucket)
            if not found:
                client.make_bucket(bucket)
                logger.info(f"  - Created Minio bucket: {bucket}")
            else:
                logger.info(f"  - Minio bucket '{bucket}' already exists.")
        except S3Error as e:
            logger.error(f"Failed to create or check bucket {bucket}: {e}")
            raise

def bootstrap_kong():
    """
    Configures the Kong API Gateway with initial services, routes, and JWT authentication.
    """
    try:
        jwt_secret = get_jwt_secret_from_vault()
        
        # Step 1: Create a Service in Kong
        logger.info("Creating/Updating Kong Service for auth-service...")
        service_payload = {
            "name": "auth-service",
            "host": "auth-service.service.consul",
            "port": 80
        }
        requests.put(f"{KONG_ADMIN_URL}/services/auth-service", json=service_payload).raise_for_status()
        logger.info("  - Service 'auth-service' configured.")

        # Step 2: Create a Route
        logger.info("Creating/Updating Kong Route for auth-service...")
        route_payload = {
            "paths": ["/auth"],
            "strip_path": True,
        }
        requests.put(f"{KONG_ADMIN_URL}/services/auth-service/routes/auth-route", json=route_payload).raise_for_status()
        logger.info("  - Route for '/auth' configured.")

        # Step 3: Enable and Configure Plugins
        logger.info("Enabling plugins on auth-service...")
        
        # JWT Plugin
        jwt_plugin_payload = {
            "name": "jwt",
            "config": {
                "claims_to_verify": ["exp"],
                "key_claim_name": "iss",
                "header_names": ["Authorization"],
                "claims_to_headers": ["tid", "roles"],
                "header_prefix": "X-",
            },
        }
        requests.post(f"{KONG_ADMIN_URL}/services/auth-service/plugins", json=jwt_plugin_payload).raise_for_status()

        # CORS Plugin to allow browser access
        cors_plugin_payload = {
            "name": "cors",
            "config": {
                "origins": ["*"], # For development; restrict in production
                "methods": ["GET", "POST", "PUT", "PATCH", "DELETE"],
                "headers": ["Authorization", "Content-Type"],
                "exposed_headers": ["Content-Length"],
                "credentials": True,
            },
        }
        requests.post(f"{KONG_ADMIN_URL}/services/auth-service/plugins", json=cors_plugin_payload).raise_for_status()
        
        logger.info("  - JWT and CORS plugins enabled.")

        # Step 4: Create a Consumer to associate the JWT with
        logger.info("Creating a generic consumer for JWTs...")
        consumer_payload = {"username": "generic_jwt_consumer"}
        requests.put(f"{KONG_ADMIN_URL}/consumers/generic_jwt_consumer", json=consumer_payload).raise_for_status()
        logger.info("  - Consumer 'generic_jwt_consumer' created.")

        # Step 5: Create JWT credentials for the consumer
        logger.info("Creating JWT credentials...")
        jwt_payload = {
            "key": "auth-service-issuer", # A unique identifier for the issuer
            "secret": jwt_secret
        }
        requests.post(f"{KONG_ADMIN_URL}/consumers/generic_jwt_consumer/jwt", json=jwt_payload).raise_for_status()
        logger.info("  - JWT credentials created.")

        logger.info("Kong gateway bootstrap complete.")

    except Exception as e:
        logger.error(f"Failed to bootstrap Kong gateway: {e}")
        if isinstance(e, requests.HTTPError):
            logger.error(f"Response body: {e.response.text}")

if __name__ == "__main__":
    bootstrap_minio()
    bootstrap_kong() 