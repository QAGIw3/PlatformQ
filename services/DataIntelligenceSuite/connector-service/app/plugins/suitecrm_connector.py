from .base import BaseConnector
from typing import Optional, Dict, Any
import httpx
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class SuiteCRMConnector(BaseConnector):
    """
    Connector for SuiteCRM to sync contacts, accounts, opportunities, and other CRM data
    """
    
    @property
    def connector_type(self) -> str:
        return "suitecrm"
    
    @property
    def schedule(self) -> Optional[str]:
        # Run every hour by default
        return "0 * * * *"
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "").rstrip("/")
        self.username = config.get("username")
        self.password = config.get("password")
        self.access_token = None
        self.tenant_id = config.get("tenant_id")
        self.last_sync_time = config.get("last_sync_time")
        
    async def _authenticate(self):
        """Authenticate with SuiteCRM OAuth2"""
        async with httpx.AsyncClient() as client:
            auth_url = f"{self.base_url}/Api/access_token"
            
            data = {
                "grant_type": "password",
                "client_id": self.config.get("client_id", "sugar"),
                "client_secret": self.config.get("client_secret", ""),
                "username": self.username,
                "password": self.password,
                "platform": "platformq"
            }
            
            response = await client.post(auth_url, data=data)
            response.raise_for_status()
            
            auth_data = response.json()
            self.access_token = auth_data["access_token"]
            return self.access_token
            
    async def _make_api_request(self, endpoint: str, method: str = "GET", 
                              params: Optional[Dict] = None, 
                              data: Optional[Dict] = None):
        """Make authenticated API request to SuiteCRM"""
        if not self.access_token:
            await self._authenticate()
            
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/Api/V8/{endpoint}"
        
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            )
            
            if response.status_code == 401:
                # Token expired, re-authenticate
                await self._authenticate()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data
                )
                
            response.raise_for_status()
            return response.json()
            
    async def _sync_module(self, module_name: str, fields: list, 
                          asset_type: str, transform_func=None):
        """Generic sync function for SuiteCRM modules"""
        logger.info(f"Syncing {module_name} from SuiteCRM")
        
        # Build filter for modified records
        filters = {}
        if self.last_sync_time:
            filters["date_modified"] = {
                "$gte": self.last_sync_time
            }
            
        page = 1
        page_size = 100
        total_synced = 0
        
        while True:
            # Fetch records
            params = {
                "fields[module]": ",".join(fields),
                "page[size]": page_size,
                "page[number]": page,
                "sort": "-date_modified"
            }
            
            if filters:
                params["filter"] = json.dumps(filters)
                
            try:
                response = await self._make_api_request(
                    f"module/{module_name}",
                    params=params
                )
                
                records = response.get("data", [])
                if not records:
                    break
                    
                # Process each record
                for record in records:
                    try:
                        # Transform record to asset format
                        if transform_func:
                            asset_data = transform_func(record)
                        else:
                            asset_data = self._default_transform(
                                record, module_name, asset_type
                            )
                            
                        # Create digital asset
                        await self._create_digital_asset(asset_data)
                        total_synced += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing {module_name} record {record.get('id')}: {e}")
                        
                # Check if more pages exist
                meta = response.get("meta", {})
                if page >= meta.get("total-pages", 1):
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching {module_name} page {page}: {e}")
                break
                
        logger.info(f"Synced {total_synced} {module_name} records")
        
    def _default_transform(self, record: Dict, module_name: str, 
                          asset_type: str) -> Dict[str, Any]:
        """Default transformation for SuiteCRM records"""
        attributes = record.get("attributes", {})
        
        # Build asset name
        name_parts = []
        if attributes.get("first_name"):
            name_parts.append(attributes["first_name"])
        if attributes.get("last_name"):
            name_parts.append(attributes["last_name"])
        if attributes.get("name"):
            name_parts.append(attributes["name"])
            
        asset_name = " ".join(name_parts) or f"{module_name} {record['id']}"
        
        return {
            "asset_name": asset_name,
            "asset_type": asset_type,
            "source_tool": "SuiteCRM",
            "external_id": record["id"],
            "tenant_id": self.tenant_id,
            "metadata": {
                "module": module_name,
                "record_type": record.get("type", module_name),
                "created_date": attributes.get("date_entered"),
                "modified_date": attributes.get("date_modified"),
                "assigned_user": attributes.get("assigned_user_id"),
                "attributes": attributes
            },
            "tags": self._extract_tags(attributes, module_name)
        }
        
    def _extract_tags(self, attributes: Dict, module_name: str) -> list:
        """Extract relevant tags from record attributes"""
        tags = [f"crm:{module_name.lower()}"]
        
        # Add status tags
        if attributes.get("status"):
            tags.append(f"status:{attributes['status'].lower()}")
            
        # Add type tags
        if attributes.get("account_type"):
            tags.append(f"type:{attributes['account_type'].lower()}")
        elif attributes.get("opportunity_type"):
            tags.append(f"type:{attributes['opportunity_type'].lower()}")
            
        # Add priority/importance tags
        if attributes.get("priority"):
            tags.append(f"priority:{attributes['priority'].lower()}")
            
        return tags
        
    def _transform_contact(self, record: Dict) -> Dict[str, Any]:
        """Transform SuiteCRM contact to asset format"""
        attributes = record.get("attributes", {})
        
        # Build full name
        name_parts = []
        if attributes.get("salutation"):
            name_parts.append(attributes["salutation"])
        if attributes.get("first_name"):
            name_parts.append(attributes["first_name"])
        if attributes.get("last_name"):
            name_parts.append(attributes["last_name"])
            
        full_name = " ".join(name_parts)
        
        return {
            "asset_name": full_name,
            "asset_type": "CRM_CONTACT",
            "source_tool": "SuiteCRM",
            "external_id": record["id"],
            "tenant_id": self.tenant_id,
            "metadata": {
                "module": "Contacts",
                "email": attributes.get("email1"),
                "phone": attributes.get("phone_work") or attributes.get("phone_mobile"),
                "title": attributes.get("title"),
                "department": attributes.get("department"),
                "account_id": attributes.get("account_id"),
                "account_name": attributes.get("account_name"),
                "address": {
                    "street": attributes.get("primary_address_street"),
                    "city": attributes.get("primary_address_city"),
                    "state": attributes.get("primary_address_state"),
                    "postal_code": attributes.get("primary_address_postalcode"),
                    "country": attributes.get("primary_address_country")
                },
                "description": attributes.get("description"),
                "created_date": attributes.get("date_entered"),
                "modified_date": attributes.get("date_modified"),
                "assigned_user": attributes.get("assigned_user_id"),
                "do_not_call": attributes.get("do_not_call", False),
                "attributes": attributes
            },
            "tags": self._extract_contact_tags(attributes)
        }
        
    def _extract_contact_tags(self, attributes: Dict) -> list:
        """Extract tags specific to contacts"""
        tags = ["crm:contact"]
        
        # Lead source
        if attributes.get("lead_source"):
            tags.append(f"source:{attributes['lead_source'].lower()}")
            
        # Status
        if attributes.get("do_not_call"):
            tags.append("do-not-call")
            
        # Department
        if attributes.get("department"):
            tags.append(f"dept:{attributes['department'].lower().replace(' ', '-')}")
            
        return tags
        
    def _transform_account(self, record: Dict) -> Dict[str, Any]:
        """Transform SuiteCRM account to asset format"""
        attributes = record.get("attributes", {})
        
        return {
            "asset_name": attributes.get("name", f"Account {record['id']}"),
            "asset_type": "CRM_ACCOUNT",
            "source_tool": "SuiteCRM",
            "external_id": record["id"],
            "tenant_id": self.tenant_id,
            "metadata": {
                "module": "Accounts",
                "account_type": attributes.get("account_type"),
                "industry": attributes.get("industry"),
                "annual_revenue": attributes.get("annual_revenue"),
                "employees": attributes.get("employees"),
                "website": attributes.get("website"),
                "phone": attributes.get("phone_office"),
                "billing_address": {
                    "street": attributes.get("billing_address_street"),
                    "city": attributes.get("billing_address_city"),
                    "state": attributes.get("billing_address_state"),
                    "postal_code": attributes.get("billing_address_postalcode"),
                    "country": attributes.get("billing_address_country")
                },
                "description": attributes.get("description"),
                "created_date": attributes.get("date_entered"),
                "modified_date": attributes.get("date_modified"),
                "assigned_user": attributes.get("assigned_user_id"),
                "parent_id": attributes.get("parent_id"),
                "attributes": attributes
            },
            "tags": self._extract_account_tags(attributes)
        }
        
    def _extract_account_tags(self, attributes: Dict) -> list:
        """Extract tags specific to accounts"""
        tags = ["crm:account"]
        
        # Account type
        if attributes.get("account_type"):
            tags.append(f"type:{attributes['account_type'].lower()}")
            
        # Industry
        if attributes.get("industry"):
            tags.append(f"industry:{attributes['industry'].lower().replace(' ', '-')}")
            
        # Size based on employees
        employees = attributes.get("employees")
        if employees:
            try:
                emp_count = int(employees)
                if emp_count < 50:
                    tags.append("size:small")
                elif emp_count < 500:
                    tags.append("size:medium")
                else:
                    tags.append("size:large")
            except:
                pass
                
        return tags
        
    def _transform_opportunity(self, record: Dict) -> Dict[str, Any]:
        """Transform SuiteCRM opportunity to asset format"""
        attributes = record.get("attributes", {})
        
        return {
            "asset_name": attributes.get("name", f"Opportunity {record['id']}"),
            "asset_type": "CRM_OPPORTUNITY",
            "source_tool": "SuiteCRM",
            "external_id": record["id"],
            "tenant_id": self.tenant_id,
            "metadata": {
                "module": "Opportunities",
                "account_id": attributes.get("account_id"),
                "account_name": attributes.get("account_name"),
                "amount": attributes.get("amount"),
                "currency": attributes.get("currency_id"),
                "sales_stage": attributes.get("sales_stage"),
                "probability": attributes.get("probability"),
                "date_closed": attributes.get("date_closed"),
                "opportunity_type": attributes.get("opportunity_type"),
                "lead_source": attributes.get("lead_source"),
                "campaign_id": attributes.get("campaign_id"),
                "next_step": attributes.get("next_step"),
                "description": attributes.get("description"),
                "created_date": attributes.get("date_entered"),
                "modified_date": attributes.get("date_modified"),
                "assigned_user": attributes.get("assigned_user_id"),
                "attributes": attributes
            },
            "tags": self._extract_opportunity_tags(attributes)
        }
        
    def _extract_opportunity_tags(self, attributes: Dict) -> list:
        """Extract tags specific to opportunities"""
        tags = ["crm:opportunity"]
        
        # Sales stage
        if attributes.get("sales_stage"):
            tags.append(f"stage:{attributes['sales_stage'].lower().replace(' ', '-')}")
            
        # Opportunity type
        if attributes.get("opportunity_type"):
            tags.append(f"type:{attributes['opportunity_type'].lower()}")
            
        # Lead source
        if attributes.get("lead_source"):
            tags.append(f"source:{attributes['lead_source'].lower()}")
            
        # Value-based tags
        amount = attributes.get("amount")
        if amount:
            try:
                value = float(amount)
                if value >= 1000000:
                    tags.append("value:high")
                elif value >= 100000:
                    tags.append("value:medium")
                else:
                    tags.append("value:low")
            except:
                pass
                
        return tags
        
    async def run(self, context: Optional[Dict[str, Any]] = None):
        """Main execution method to sync all CRM data"""
        try:
            logger.info(f"Starting SuiteCRM sync for tenant {self.tenant_id}")
            
            # Sync contacts
            await self._sync_module(
                "Contacts",
                ["id", "first_name", "last_name", "salutation", "title", 
                 "department", "email1", "phone_work", "phone_mobile",
                 "account_id", "account_name", "primary_address_street",
                 "primary_address_city", "primary_address_state",
                 "primary_address_postalcode", "primary_address_country",
                 "lead_source", "description", "do_not_call",
                 "date_entered", "date_modified", "assigned_user_id"],
                "CRM_CONTACT",
                self._transform_contact
            )
            
            # Sync accounts
            await self._sync_module(
                "Accounts",
                ["id", "name", "account_type", "industry", "annual_revenue",
                 "employees", "website", "phone_office", "billing_address_street",
                 "billing_address_city", "billing_address_state",
                 "billing_address_postalcode", "billing_address_country",
                 "description", "parent_id", "date_entered", "date_modified",
                 "assigned_user_id"],
                "CRM_ACCOUNT",
                self._transform_account
            )
            
            # Sync opportunities
            await self._sync_module(
                "Opportunities",
                ["id", "name", "account_id", "account_name", "amount",
                 "currency_id", "sales_stage", "probability", "date_closed",
                 "opportunity_type", "lead_source", "campaign_id", "next_step",
                 "description", "date_entered", "date_modified", "assigned_user_id"],
                "CRM_OPPORTUNITY",
                self._transform_opportunity
            )
            
            # Update last sync time
            self.config["last_sync_time"] = datetime.utcnow().isoformat()
            
            logger.info("SuiteCRM sync completed successfully")
            
        except Exception as e:
            logger.error(f"SuiteCRM sync failed: {e}")
            raise 