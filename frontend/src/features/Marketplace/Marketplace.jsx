import React, { useState, useEffect } from 'react';
import { Card, Row, Col, Tag, Button, Select, InputNumber, Slider, Empty, Spin, Modal, Form, Input, message, Statistic, Space, Tooltip, Badge } from 'antd';
import { ShoppingCartOutlined, DollarOutlined, FileProtectOutlined, ClockCircleOutlined, SafetyOutlined, EyeOutlined, DownloadOutlined, LockOutlined } from '@ant-design/icons';
import api from '../../api/api';
import './Marketplace.css';

const { Option } = Select;
const { Meta } = Card;

/**
 * Marketplace Component
 * 
 * The main marketplace interface for PlatformQ's decentralized asset trading.
 * This component provides a comprehensive UI for browsing, filtering, purchasing,
 * and licensing digital assets with blockchain-based ownership and royalties.
 * 
 * Key Features:
 * - Asset browsing with real-time filtering
 * - Purchase and license workflows
 * - Asset listing with customizable royalty settings
 * - Price filtering and sorting
 * - Responsive grid layout
 * 
 * State Management:
 * - assets: Array of marketplace assets fetched from backend
 * - filters: Object containing all filter criteria
 * - selectedAsset: Currently selected asset for purchase/license
 * - Modal visibility states for different workflows
 * 
 * @component
 */
const Marketplace = () => {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    forSale: true,
    licensable: null,
    assetType: null,
    minPrice: null,
    maxPrice: null,
    sortBy: 'created_at',
    sortOrder: 'desc'
  });
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [purchaseModalVisible, setPurchaseModalVisible] = useState(false);
  const [licenseModalVisible, setLicenseModalVisible] = useState(false);
  const [listingModalVisible, setListingModalVisible] = useState(false);
  const [purchaseForm] = Form.useForm();
  const [licenseForm] = Form.useForm();
  const [listingForm] = Form.useForm();

  useEffect(() => {
    fetchMarketplaceAssets();
  }, [filters]);

  /**
   * Fetches marketplace assets from the backend with applied filters
   * 
   * This function is the core data fetching mechanism for the marketplace.
   * It constructs a query string from the current filter state and retrieves
   * matching assets from the digital-asset-service.
   * 
   * Filter Parameters:
   * - forSale: Boolean to show only assets for sale
   * - licensable: Boolean to show only licensable assets
   * - assetType: Specific asset type filter (3D_MODEL, SIMULATION, etc.)
   * - minPrice/maxPrice: Price range filters
   * - sortBy: Field to sort by (created_at, sale_price, asset_name)
   * - sortOrder: Sort direction (asc/desc)
   * 
   * Error Handling:
   * - Network errors display user-friendly message
   * - Console logging for debugging
   * - Loading state management for UI feedback
   * 
   * @async
   * @returns {Promise<void>} Updates component state with fetched assets
   */
  const fetchMarketplaceAssets = async () => {
    try {
      setLoading(true);
      
      // Build query parameters from filter state
      const params = new URLSearchParams();
      Object.entries(filters).forEach(([key, value]) => {
        if (value !== null) {
          params.append(key, value);
        }
      });
      
      // Fetch assets from backend
      const response = await api.get(`/digital-asset-service/api/v1/marketplace/assets?${params}`);
      
      // Update state with fetched assets
      setAssets(response.data.assets || []);
    } catch (error) {
      console.error('Failed to fetch marketplace assets:', error);
      message.error('Failed to load marketplace assets');
    } finally {
      setLoading(false);
    }
  };

  const handlePurchaseAsset = async (values) => {
    try {
      // In a real implementation, this would initiate blockchain transaction
      message.info('Purchase functionality coming soon!');
      setPurchaseModalVisible(false);
      purchaseForm.resetFields();
    } catch (error) {
      console.error('Purchase failed:', error);
      message.error('Failed to complete purchase');
    }
  };

  const handlePurchaseLicense = async (values) => {
    try {
      // In a real implementation, this would interact with the UsageLicense smart contract
      message.info('License purchase functionality coming soon!');
      setLicenseModalVisible(false);
      licenseForm.resetFields();
    } catch (error) {
      console.error('License purchase failed:', error);
      message.error('Failed to purchase license');
    }
  };

  const handleListAsset = async (values) => {
    try {
      const response = await api.post(`/digital-asset-service/api/v1/digital-assets/${values.cid}/list-for-sale`, null, {
        params: {
          price: values.price,
          currency: values.currency || 'ETH',
          royalty_percentage: values.royaltyPercentage
        }
      });
      
      message.success('Asset listed successfully!');
      setListingModalVisible(false);
      listingForm.resetFields();
      fetchMarketplaceAssets();
    } catch (error) {
      console.error('Listing failed:', error);
      message.error('Failed to list asset');
    }
  };

  const formatPrice = (price, currency = 'ETH') => {
    if (!price) return 'Free';
    return `${price} ${currency}`;
  };

  const getAssetTypeColor = (type) => {
    const colors = {
      '3D_MODEL': 'blue',
      'SIMULATION': 'green',
      'DATASET': 'purple',
      'DOCUMENT': 'orange',
      'CODE': 'cyan',
      'IMAGE': 'magenta'
    };
    return colors[type] || 'default';
  };

  const renderAssetCard = (asset) => {
    const isLicensable = asset.is_licensable && asset.license_terms;
    const licensePrice = isLicensable ? asset.license_terms.price : null;
    
    return (
      <Card
        key={asset.asset_id}
        hoverable
        className="asset-card"
        cover={
          <div className="asset-cover">
            <FileProtectOutlined style={{ fontSize: 48, color: '#1890ff' }} />
            <Tag color={getAssetTypeColor(asset.asset_type)} className="asset-type-tag">
              {asset.asset_type}
            </Tag>
          </div>
        }
        actions={[
          asset.is_for_sale && (
            <Tooltip title="Purchase Asset">
              <Button 
                type="link" 
                icon={<ShoppingCartOutlined />}
                onClick={() => {
                  setSelectedAsset(asset);
                  setPurchaseModalVisible(true);
                }}
              >
                Buy
              </Button>
            </Tooltip>
          ),
          isLicensable && (
            <Tooltip title="Purchase License">
              <Button 
                type="link" 
                icon={<LockOutlined />}
                onClick={() => {
                  setSelectedAsset(asset);
                  setLicenseModalVisible(true);
                }}
              >
                License
              </Button>
            </Tooltip>
          ),
          <Tooltip title="View Details">
            <Button type="link" icon={<EyeOutlined />}>
              View
            </Button>
          </Tooltip>
        ].filter(Boolean)}
      >
        <Meta
          title={asset.asset_name}
          description={
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <div>
                {asset.is_for_sale && (
                  <Tag color="green" icon={<DollarOutlined />}>
                    For Sale: {formatPrice(asset.sale_price)}
                  </Tag>
                )}
                {isLicensable && (
                  <Tag color="blue" icon={<ClockCircleOutlined />}>
                    License: {formatPrice(licensePrice)}/{asset.license_terms.duration}s
                  </Tag>
                )}
              </div>
              {asset.royalty_percentage > 0 && (
                <Tag color="purple" icon={<SafetyOutlined />}>
                  {(asset.royalty_percentage / 100).toFixed(1)}% Royalty
                </Tag>
              )}
              <div className="asset-meta">
                Created: {new Date(asset.created_at).toLocaleDateString()}
              </div>
            </Space>
          }
        />
      </Card>
    );
  };

  return (
    <div className="marketplace">
      <Card className="marketplace-header">
        <h1>Digital Asset Marketplace</h1>
        <p>Buy, sell, and license digital assets with automatic royalty distribution</p>
        
        <Row gutter={[16, 16]} className="marketplace-filters">
          <Col xs={24} sm={12} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Asset Type"
              allowClear
              onChange={(value) => setFilters({ ...filters, assetType: value })}
            >
              <Option value="3D_MODEL">3D Model</Option>
              <Option value="SIMULATION">Simulation</Option>
              <Option value="DATASET">Dataset</Option>
              <Option value="DOCUMENT">Document</Option>
              <Option value="CODE">Code</Option>
              <Option value="IMAGE">Image</Option>
            </Select>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Listing Type"
              value={filters.forSale}
              onChange={(value) => setFilters({ ...filters, forSale: value, licensable: !value })}
            >
              <Option value={true}>For Sale</Option>
              <Option value={false}>For License</Option>
              <Option value={null}>All</Option>
            </Select>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <InputNumber
              style={{ width: '100%' }}
              placeholder="Max Price (ETH)"
              min={0}
              step={0.01}
              onChange={(value) => setFilters({ ...filters, maxPrice: value })}
            />
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Sort By"
              value={filters.sortBy}
              onChange={(value) => setFilters({ ...filters, sortBy: value })}
            >
              <Option value="created_at">Date Listed</Option>
              <Option value="sale_price">Price</Option>
              <Option value="asset_name">Name</Option>
            </Select>
          </Col>
        </Row>
        
        <div style={{ marginTop: 16 }}>
          <Button 
            type="primary" 
            icon={<DollarOutlined />}
            onClick={() => setListingModalVisible(true)}
          >
            List Your Asset
          </Button>
        </div>
      </Card>

      {loading ? (
        <div className="marketplace-loading">
          <Spin size="large" />
        </div>
      ) : assets.length > 0 ? (
        <Row gutter={[16, 16]} className="marketplace-grid">
          {assets.map(renderAssetCard)}
        </Row>
      ) : (
        <Empty
          description="No assets found"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          style={{ marginTop: 48 }}
        >
          <Button type="primary" onClick={() => setListingModalVisible(true)}>
            Be the first to list!
          </Button>
        </Empty>
      )}

      {/* Purchase Modal */}
      <Modal
        title="Purchase Asset"
        visible={purchaseModalVisible}
        onCancel={() => {
          setPurchaseModalVisible(false);
          purchaseForm.resetFields();
        }}
        footer={null}
      >
        {selectedAsset && (
          <Form
            form={purchaseForm}
            layout="vertical"
            onFinish={handlePurchaseAsset}
            initialValues={{ assetId: selectedAsset.asset_id }}
          >
            <Card className="asset-preview">
              <Statistic
                title={selectedAsset.asset_name}
                value={selectedAsset.sale_price}
                suffix="ETH"
                prefix={<DollarOutlined />}
              />
              <p>Type: {selectedAsset.asset_type}</p>
              {selectedAsset.royalty_percentage > 0 && (
                <p>Royalty: {(selectedAsset.royalty_percentage / 100).toFixed(1)}%</p>
              )}
            </Card>
            
            <Form.Item
              name="walletAddress"
              label="Your Wallet Address"
              rules={[
                { required: true, message: 'Please enter your wallet address' },
                { pattern: /^0x[a-fA-F0-9]{40}$/, message: 'Invalid Ethereum address' }
              ]}
            >
              <Input placeholder="0x..." />
            </Form.Item>
            
            <Form.Item>
              <Button type="primary" htmlType="submit" block>
                Complete Purchase
              </Button>
            </Form.Item>
          </Form>
        )}
      </Modal>

      {/* License Modal */}
      <Modal
        title="Purchase License"
        visible={licenseModalVisible}
        onCancel={() => {
          setLicenseModalVisible(false);
          licenseForm.resetFields();
        }}
        footer={null}
      >
        {selectedAsset && selectedAsset.license_terms && (
          <Form
            form={licenseForm}
            layout="vertical"
            onFinish={handlePurchaseLicense}
          >
            <Card className="license-preview">
              <h3>{selectedAsset.asset_name}</h3>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Statistic
                  title="License Price"
                  value={selectedAsset.license_terms.price}
                  suffix="ETH"
                  prefix={<DollarOutlined />}
                />
                <div>
                  <strong>Duration:</strong> {selectedAsset.license_terms.duration} seconds
                </div>
                <div>
                  <strong>Type:</strong> {selectedAsset.license_terms.type}
                </div>
                {selectedAsset.license_terms.max_usage > 0 && (
                  <div>
                    <strong>Max Usage:</strong> {selectedAsset.license_terms.max_usage} times
                  </div>
                )}
              </Space>
            </Card>
            
            <Form.Item>
              <Button type="primary" htmlType="submit" block>
                Purchase License
              </Button>
            </Form.Item>
          </Form>
        )}
      </Modal>

      {/* Listing Modal */}
      <Modal
        title="List Asset for Sale"
        visible={listingModalVisible}
        onCancel={() => {
          setListingModalVisible(false);
          listingForm.resetFields();
        }}
        footer={null}
      >
        <Form
          form={listingForm}
          layout="vertical"
          onFinish={handleListAsset}
        >
          <Form.Item
            name="cid"
            label="Asset CID"
            rules={[{ required: true, message: 'Please enter asset CID' }]}
          >
            <Input placeholder="Enter your asset CID" />
          </Form.Item>
          
          <Form.Item
            name="price"
            label="Sale Price"
            rules={[{ required: true, message: 'Please enter price' }]}
          >
            <InputNumber
              style={{ width: '100%' }}
              min={0}
              step={0.001}
              placeholder="0.00"
              suffix="ETH"
            />
          </Form.Item>
          
          <Form.Item
            name="royaltyPercentage"
            label="Royalty Percentage"
            tooltip="Percentage of future sales you'll receive"
          >
            <Slider
              marks={{
                0: '0%',
                250: '2.5%',
                500: '5%',
                1000: '10%',
                2500: '25%',
                5000: '50%'
              }}
              max={5000}
              defaultValue={250}
            />
          </Form.Item>
          
          <Form.Item>
            <Button type="primary" htmlType="submit" block>
              List Asset
            </Button>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default Marketplace; 