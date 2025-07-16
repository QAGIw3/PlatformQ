"""
PlatformQ Service Operator for Apache Airflow

This operator calls PlatformQ microservices with proper authentication
and service discovery through Consul.
"""

import os
import json
from typing import Dict, Any, Optional, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import consul


class PlatformQServiceOperator(BaseOperator):
    """
    Calls PlatformQ services with proper authentication and retries
    
    :param service_name: Name of the service to call (e.g., 'digital-asset-service')
    :param endpoint: API endpoint path (e.g., '/api/v1/assets')
    :param method: HTTP method (GET, POST, PUT, PATCH, DELETE)
    :param payload: Request payload (for POST/PUT/PATCH)
    :param headers: Additional headers to include
    :param auth_token: Authentication token (defaults to service token from Vault)
    :param consul_url: Consul URL for service discovery
    :param timeout: Request timeout in seconds
    :param retries: Number of retries for failed requests
    """
    
    template_fields = ['endpoint', 'payload', 'headers']
    ui_color = '#95E1D3'
    
    @apply_defaults
    def __init__(
        self,
        service_name: str,
        endpoint: str,
        method: str = 'GET',
        payload: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, str]] = None,
        auth_token: Optional[str] = None,
        consul_url: Optional[str] = None,
        timeout: int = 30,
        retries: int = 3,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.service_name = service_name
        self.endpoint = endpoint
        self.method = method.upper()
        self.payload = payload
        self.headers = headers or {}
        self.auth_token = auth_token
        self.consul_url = consul_url or os.getenv('PLATFORMQ_CONSUL_URL', 'http://consul:8500')
        self.timeout = timeout
        self.retries = retries
    
    def _get_service_url(self) -> str:
        """Get service URL from Consul service discovery"""
        try:
            # Try Consul first
            c = consul.Consul(host=self.consul_url.replace('http://', '').replace(':8500', ''))
            _, services = c.health.service(self.service_name, passing=True)
            
            if services:
                service = services[0]
                host = service['Service']['Address'] or service['Node']['Address']
                port = service['Service']['Port']
                return f"http://{host}:{port}"
        except Exception as e:
            self.log.warning(f"Consul lookup failed: {e}, falling back to direct service name")
        
        # Fallback to direct service name (works in Docker Compose/K8s)
        return f"http://{self.service_name}:8000"
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        headers = {}
        
        if self.auth_token:
            headers['Authorization'] = f"Bearer {self.auth_token}"
        else:
            # In production, get service token from Vault
            # For now, use a placeholder
            headers['X-Service-Auth'] = 'airflow-service'
        
        return headers
    
    def execute(self, context) -> Dict[str, Any]:
        """Execute the service call"""
        service_url = self._get_service_url()
        url = f"{service_url}{self.endpoint}"
        
        self.log.info(f"Calling {self.method} {url}")
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=self.retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST", "PATCH"],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Prepare headers
        headers = {
            'Content-Type': 'application/json',
            **self._get_auth_headers(),
            **self.headers
        }
        
        # Prepare request arguments
        request_args = {
            'method': self.method,
            'url': url,
            'headers': headers,
            'timeout': self.timeout
        }
        
        # Add payload if provided
        if self.payload:
            if isinstance(self.payload, dict):
                request_args['json'] = self.payload
            else:
                request_args['data'] = self.payload
                headers['Content-Type'] = 'text/plain'
        
        try:
            # Make the request
            response = session.request(**request_args)
            
            # Check for errors
            response.raise_for_status()
            
            # Parse response
            if response.headers.get('Content-Type', '').startswith('application/json'):
                result = response.json()
            else:
                result = {'text': response.text, 'status_code': response.status_code}
            
            self.log.info(f"Request successful: {response.status_code}")
            
            # Store response in XCom
            context['ti'].xcom_push(key='response', value=result)
            context['ti'].xcom_push(key='status_code', value=response.status_code)
            
            return result
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"Request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                self.log.error(f"Response: {e.response.text}")
            raise
        finally:
            session.close()
    
    def on_kill(self):
        """Cleanup when task is killed"""
        self.log.info("Task killed") 