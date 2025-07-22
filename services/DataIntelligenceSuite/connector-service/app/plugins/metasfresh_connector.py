from .base import BaseConnector
from typing import Optional, Dict, Any
import httpx
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class MetasfreshConnector(BaseConnector):
    """
    Connector for Metasfresh ERP to sync products, orders, invoices, and business partners
    """
    
    @property
    def connector_type(self) -> str:
        return "metasfresh"
    
    @property
    def schedule(self) -> Optional[str]:
        # Run every 30 minutes
        return "*/30 * * * *"
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "").rstrip("/")
        self.api_key = config.get("api_key")
        self.tenant_id = config.get("tenant_id")
        self.last_sync_time = config.get("last_sync_time")
        
    async def _make_api_request(self, endpoint: str, method: str = "GET",
                              params: Optional[Dict] = None,
                              data: Optional[Dict] = None):
        """Make authenticated API request to Metasfresh"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        url = f"{self.base_url}/api/v2/{endpoint}"
        
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=30.0
            )
            
            response.raise_for_status()
            return response.json()
            
    async def _sync_products(self):
        """Sync products/materials from Metasfresh"""
        logger.info("Syncing products from Metasfresh")
        
        page = 0
        page_size = 100
        total_synced = 0
        
        while True:
            params = {
                "pageSize": page_size,
                "page": page
            }
            
            if self.last_sync_time:
                params["updatedAfter"] = self.last_sync_time
                
            try:
                response = await self._make_api_request(
                    "products",
                    params=params
                )
                
                products = response.get("products", [])
                if not products:
                    break
                    
                for product in products:
                    asset_data = self._transform_product(product)
                    await self._create_digital_asset(asset_data)
                    total_synced += 1
                    
                # Check if more pages exist
                if len(products) < page_size:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error syncing products page {page}: {e}")
                break
                
        logger.info(f"Synced {total_synced} products")
        
    def _transform_product(self, product: Dict) -> Dict[str, Any]:
        """Transform Metasfresh product to asset format"""
        return {
            "asset_name": product.get("name", f"Product {product['id']}"),
            "asset_type": "ERP_PRODUCT",
            "source_tool": "Metasfresh",
            "external_id": str(product["id"]),
            "tenant_id": self.tenant_id,
            "metadata": {
                "product_id": product["id"],
                "sku": product.get("value"),
                "name": product.get("name"),
                "description": product.get("description"),
                "product_category": product.get("productCategory", {}).get("name"),
                "product_category_id": product.get("productCategoryId"),
                "uom": product.get("uom", {}).get("symbol"),
                "uom_id": product.get("uomId"),
                "is_stocked": product.get("isStocked", False),
                "is_purchased": product.get("isPurchased", False),
                "is_sold": product.get("isSold", False),
                "discontinued": product.get("discontinued", False),
                "manufacturer": product.get("manufacturer"),
                "manufacturer_part_number": product.get("manufacturerPartNumber"),
                "attributes": product.get("attributes", []),
                "created_date": product.get("created"),
                "updated_date": product.get("updated")
            },
            "tags": self._extract_product_tags(product)
        }
        
    def _extract_product_tags(self, product: Dict) -> list:
        """Extract tags from product data"""
        tags = ["erp:product"]
        
        # Product type tags
        if product.get("isStocked"):
            tags.append("type:stocked")
        if product.get("isPurchased"):
            tags.append("type:purchased")
        if product.get("isSold"):
            tags.append("type:sold")
            
        # Category tag
        if product.get("productCategory", {}).get("name"):
            category = product["productCategory"]["name"].lower().replace(" ", "-")
            tags.append(f"category:{category}")
            
        # Status tag
        if product.get("discontinued"):
            tags.append("status:discontinued")
        else:
            tags.append("status:active")
            
        return tags
        
    async def _sync_business_partners(self):
        """Sync business partners (customers/vendors) from Metasfresh"""
        logger.info("Syncing business partners from Metasfresh")
        
        page = 0
        page_size = 100
        total_synced = 0
        
        while True:
            params = {
                "pageSize": page_size,
                "page": page
            }
            
            if self.last_sync_time:
                params["updatedAfter"] = self.last_sync_time
                
            try:
                response = await self._make_api_request(
                    "businessPartners",
                    params=params
                )
                
                partners = response.get("businessPartners", [])
                if not partners:
                    break
                    
                for partner in partners:
                    asset_data = self._transform_business_partner(partner)
                    await self._create_digital_asset(asset_data)
                    total_synced += 1
                    
                    # Also sync contacts for this partner
                    if partner.get("contacts"):
                        for contact in partner["contacts"]:
                            contact_asset = self._transform_contact(contact, partner)
                            await self._create_digital_asset(contact_asset)
                            total_synced += 1
                    
                if len(partners) < page_size:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error syncing business partners page {page}: {e}")
                break
                
        logger.info(f"Synced {total_synced} business partners and contacts")
        
    def _transform_business_partner(self, partner: Dict) -> Dict[str, Any]:
        """Transform Metasfresh business partner to asset format"""
        return {
            "asset_name": partner.get("name", f"Partner {partner['id']}"),
            "asset_type": "ERP_BUSINESS_PARTNER",
            "source_tool": "Metasfresh",
            "external_id": str(partner["id"]),
            "tenant_id": self.tenant_id,
            "metadata": {
                "partner_id": partner["id"],
                "value": partner.get("value"),
                "name": partner.get("name"),
                "name2": partner.get("name2"),
                "is_customer": partner.get("isCustomer", False),
                "is_vendor": partner.get("isVendor", False),
                "is_employee": partner.get("isEmployee", False),
                "customer_group": partner.get("customerGroup", {}).get("name"),
                "vendor_group": partner.get("vendorGroup", {}).get("name"),
                "payment_term": partner.get("paymentTerm", {}).get("name"),
                "price_list": partner.get("priceList", {}).get("name"),
                "credit_limit": partner.get("creditLimit"),
                "tax_id": partner.get("taxId"),
                "locations": partner.get("locations", []),
                "bank_accounts": partner.get("bankAccounts", []),
                "created_date": partner.get("created"),
                "updated_date": partner.get("updated")
            },
            "tags": self._extract_partner_tags(partner)
        }
        
    def _extract_partner_tags(self, partner: Dict) -> list:
        """Extract tags from business partner data"""
        tags = ["erp:business-partner"]
        
        # Partner type tags
        if partner.get("isCustomer"):
            tags.append("type:customer")
        if partner.get("isVendor"):
            tags.append("type:vendor")
        if partner.get("isEmployee"):
            tags.append("type:employee")
            
        # Group tags
        if partner.get("customerGroup", {}).get("name"):
            group = partner["customerGroup"]["name"].lower().replace(" ", "-")
            tags.append(f"customer-group:{group}")
            
        if partner.get("vendorGroup", {}).get("name"):
            group = partner["vendorGroup"]["name"].lower().replace(" ", "-")
            tags.append(f"vendor-group:{group}")
            
        return tags
        
    def _transform_contact(self, contact: Dict, partner: Dict) -> Dict[str, Any]:
        """Transform business partner contact to asset format"""
        return {
            "asset_name": f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or f"Contact {contact['id']}",
            "asset_type": "ERP_CONTACT",
            "source_tool": "Metasfresh",
            "external_id": str(contact["id"]),
            "tenant_id": self.tenant_id,
            "metadata": {
                "contact_id": contact["id"],
                "partner_id": partner["id"],
                "partner_name": partner.get("name"),
                "first_name": contact.get("firstName"),
                "last_name": contact.get("lastName"),
                "email": contact.get("email"),
                "phone": contact.get("phone"),
                "mobile": contact.get("mobile"),
                "fax": contact.get("fax"),
                "title": contact.get("title"),
                "greeting": contact.get("greeting"),
                "is_default_contact": contact.get("isDefaultContact", False),
                "description": contact.get("description")
            },
            "tags": ["erp:contact", f"partner:{partner['id']}"]
        }
        
    async def _sync_orders(self):
        """Sync sales/purchase orders from Metasfresh"""
        logger.info("Syncing orders from Metasfresh")
        
        # Sync sales orders
        await self._sync_order_type("salesOrders", "SALES_ORDER")
        
        # Sync purchase orders
        await self._sync_order_type("purchaseOrders", "PURCHASE_ORDER")
        
    async def _sync_order_type(self, endpoint: str, asset_type: str):
        """Generic order sync for sales/purchase orders"""
        page = 0
        page_size = 50
        total_synced = 0
        
        while True:
            params = {
                "pageSize": page_size,
                "page": page
            }
            
            if self.last_sync_time:
                params["updatedAfter"] = self.last_sync_time
                
            try:
                response = await self._make_api_request(
                    endpoint,
                    params=params
                )
                
                orders = response.get("orders", [])
                if not orders:
                    break
                    
                for order in orders:
                    asset_data = self._transform_order(order, asset_type)
                    await self._create_digital_asset(asset_data)
                    total_synced += 1
                    
                if len(orders) < page_size:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error syncing {endpoint} page {page}: {e}")
                break
                
        logger.info(f"Synced {total_synced} {endpoint}")
        
    def _transform_order(self, order: Dict, order_type: str) -> Dict[str, Any]:
        """Transform order to asset format"""
        asset_name = f"{order.get('documentNo', 'Order')} - {order.get('businessPartner', {}).get('name', '')}"
        
        return {
            "asset_name": asset_name.strip(),
            "asset_type": f"ERP_{order_type}",
            "source_tool": "Metasfresh",
            "external_id": str(order["id"]),
            "tenant_id": self.tenant_id,
            "metadata": {
                "order_id": order["id"],
                "document_no": order.get("documentNo"),
                "document_type": order.get("documentType", {}).get("name"),
                "document_status": order.get("documentStatus"),
                "partner_id": order.get("businessPartnerId"),
                "partner_name": order.get("businessPartner", {}).get("name"),
                "date_ordered": order.get("dateOrdered"),
                "date_promised": order.get("datePromised"),
                "currency": order.get("currency", {}).get("isoCode"),
                "grand_total": float(order.get("grandTotal", 0)),
                "total_lines": float(order.get("totalLines", 0)),
                "description": order.get("description"),
                "order_lines": self._extract_order_lines(order.get("orderLines", [])),
                "created_date": order.get("created"),
                "updated_date": order.get("updated")
            },
            "tags": self._extract_order_tags(order, order_type)
        }
        
    def _extract_order_lines(self, order_lines: list) -> list:
        """Extract simplified order line data"""
        lines = []
        for line in order_lines:
            lines.append({
                "line_no": line.get("line"),
                "product_id": line.get("productId"),
                "product_name": line.get("product", {}).get("name"),
                "quantity": float(line.get("qtyOrdered", 0)),
                "uom": line.get("uom", {}).get("symbol"),
                "price": float(line.get("priceActual", 0)),
                "line_total": float(line.get("lineNetAmt", 0))
            })
        return lines
        
    def _extract_order_tags(self, order: Dict, order_type: str) -> list:
        """Extract tags from order data"""
        tags = [f"erp:{order_type.lower().replace('_', '-')}"]
        
        # Document status
        status = order.get("documentStatus", "").lower()
        if status:
            tags.append(f"status:{status}")
            
        # Currency
        currency = order.get("currency", {}).get("isoCode")
        if currency:
            tags.append(f"currency:{currency.lower()}")
            
        # Value range
        total = float(order.get("grandTotal", 0))
        if total >= 100000:
            tags.append("value:high")
        elif total >= 10000:
            tags.append("value:medium")
        else:
            tags.append("value:low")
            
        return tags
        
    async def _sync_invoices(self):
        """Sync invoices from Metasfresh"""
        logger.info("Syncing invoices from Metasfresh")
        
        page = 0
        page_size = 50
        total_synced = 0
        
        while True:
            params = {
                "pageSize": page_size,
                "page": page
            }
            
            if self.last_sync_time:
                params["updatedAfter"] = self.last_sync_time
                
            try:
                response = await self._make_api_request(
                    "invoices",
                    params=params
                )
                
                invoices = response.get("invoices", [])
                if not invoices:
                    break
                    
                for invoice in invoices:
                    asset_data = self._transform_invoice(invoice)
                    await self._create_digital_asset(asset_data)
                    total_synced += 1
                    
                if len(invoices) < page_size:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error syncing invoices page {page}: {e}")
                break
                
        logger.info(f"Synced {total_synced} invoices")
        
    def _transform_invoice(self, invoice: Dict) -> Dict[str, Any]:
        """Transform invoice to asset format"""
        asset_name = f"Invoice {invoice.get('documentNo', invoice['id'])} - {invoice.get('businessPartner', {}).get('name', '')}"
        
        return {
            "asset_name": asset_name.strip(),
            "asset_type": "ERP_INVOICE",
            "source_tool": "Metasfresh",
            "external_id": str(invoice["id"]),
            "tenant_id": self.tenant_id,
            "metadata": {
                "invoice_id": invoice["id"],
                "document_no": invoice.get("documentNo"),
                "document_type": invoice.get("documentType", {}).get("name"),
                "document_status": invoice.get("documentStatus"),
                "is_sales_transaction": invoice.get("isSOTrx", True),
                "partner_id": invoice.get("businessPartnerId"),
                "partner_name": invoice.get("businessPartner", {}).get("name"),
                "date_invoiced": invoice.get("dateInvoiced"),
                "date_due": invoice.get("dateDue"),
                "currency": invoice.get("currency", {}).get("isoCode"),
                "grand_total": float(invoice.get("grandTotal", 0)),
                "total_lines": float(invoice.get("totalLines", 0)),
                "is_paid": invoice.get("isPaid", False),
                "payment_term": invoice.get("paymentTerm", {}).get("name"),
                "description": invoice.get("description"),
                "created_date": invoice.get("created"),
                "updated_date": invoice.get("updated")
            },
            "tags": self._extract_invoice_tags(invoice)
        }
        
    def _extract_invoice_tags(self, invoice: Dict) -> list:
        """Extract tags from invoice data"""
        tags = ["erp:invoice"]
        
        # Transaction type
        if invoice.get("isSOTrx", True):
            tags.append("type:sales-invoice")
        else:
            tags.append("type:purchase-invoice")
            
        # Payment status
        if invoice.get("isPaid"):
            tags.append("status:paid")
        else:
            tags.append("status:unpaid")
            
        # Document status
        doc_status = invoice.get("documentStatus", "").lower()
        if doc_status:
            tags.append(f"doc-status:{doc_status}")
            
        return tags
        
    async def run(self, context: Optional[Dict[str, Any]] = None):
        """Main execution method to sync all ERP data"""
        try:
            logger.info(f"Starting Metasfresh sync for tenant {self.tenant_id}")
            
            # Sync in logical order
            await self._sync_products()
            await self._sync_business_partners()
            await self._sync_orders()
            await self._sync_invoices()
            
            # Update last sync time
            self.config["last_sync_time"] = datetime.utcnow().isoformat()
            
            logger.info("Metasfresh sync completed successfully")
            
        except Exception as e:
            logger.error(f"Metasfresh sync failed: {e}")
            raise 