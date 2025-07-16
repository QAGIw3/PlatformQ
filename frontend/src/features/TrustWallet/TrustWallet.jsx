import React, { useState, useEffect } from 'react';
import { Card, Tabs, Badge, Button, Modal, Form, Select, Alert, Tooltip, Progress, Statistic, List, Tag, Space, Divider, message, Upload, Typography, Input, InputNumber } from 'antd';
import { WalletOutlined, ExportOutlined, ImportOutlined, SafetyOutlined, GlobalOutlined, QrcodeOutlined, FileProtectOutlined, CheckCircleOutlined, ClockCircleOutlined, TrophyOutlined, TeamOutlined, CodeOutlined, BulbOutlined, LineChartOutlined } from '@ant-design/icons';
import api from '../../api/api';
import './TrustWallet.css';

const { TabPane } = Tabs;
const { Option } = Select;
const { Title, Text, Paragraph } = Typography;

/**
 * TrustWallet Component
 * 
 * A comprehensive trust management interface for PlatformQ users.
 * This component serves as the central hub for managing verifiable credentials,
 * reputation scores, and cross-chain credential exports.
 * 
 * Main Features:
 * 1. Credential Management
 *    - View all earned verifiable credentials
 *    - Check credential validity and expiration
 *    - Export credentials to blockchain as SoulBound Tokens
 *    - Create verifiable presentations for external verification
 * 
 * 2. Reputation Dashboard
 *    - Multi-dimensional reputation visualization
 *    - Real-time score updates across 5 dimensions
 *    - Progress tracking for each reputation aspect
 *    - Overall trust score calculation
 * 
 * 3. Cross-Chain Integration
 *    - Support for multiple blockchain networks
 *    - Real-time network status monitoring
 *    - Gas fee estimation
 *    - SBT deployment status
 * 
 * State Management:
 * - credentials: Array of user's verifiable credentials
 * - reputationScores: Object with multi-dimensional scores
 * - supportedChains: Array of blockchain network configurations
 * - presentationTemplates: Pre-defined presentation templates
 * 
 * API Integration:
 * - Fetches credentials from verifiable-credential-service
 * - Retrieves reputation scores from graph-intelligence-service
 * - Manages cross-chain exports via blockchain bridge
 * 
 * @component
 */
const TrustWallet = () => {
  const [credentials, setCredentials] = useState([]);
  const [loading, setLoading] = useState(true);
  const [exportModalVisible, setExportModalVisible] = useState(false);
  const [presentationModalVisible, setPresentationModalVisible] = useState(false);
  const [selectedCredential, setSelectedCredential] = useState(null);
  const [supportedChains, setSupportedChains] = useState([]);
  const [reputationScores, setReputationScores] = useState(null);
  const [presentationTemplates, setPresentationTemplates] = useState([]);
  const [exportForm] = Form.useForm();
  const [presentationForm] = Form.useForm();

  useEffect(() => {
    fetchCredentials();
    fetchSupportedChains();
    fetchReputationScores();
    fetchPresentationTemplates();
  }, []);

  const fetchCredentials = async () => {
    try {
      setLoading(true);
      // Get user DID first
      const userDid = localStorage.getItem('userDid') || 'did:platformq:default:user123';
      const response = await api.get(`/verifiable-credential-service/api/v1/dids/${userDid}/credentials`);
      setCredentials(response.data);
    } catch (error) {
      console.error('Failed to fetch credentials:', error);
      message.error('Failed to load credentials');
    } finally {
      setLoading(false);
    }
  };

  const fetchSupportedChains = async () => {
    try {
      const response = await api.get('/verifiable-credential-service/api/v1/cross-chain/chains');
      setSupportedChains(response.data);
    } catch (error) {
      console.error('Failed to fetch supported chains:', error);
    }
  };

  const fetchReputationScores = async () => {
    try {
      const response = await api.get('/graph-intelligence-service/api/v1/reputation/multi-dimensional');
      setReputationScores(response.data);
    } catch (error) {
      console.error('Failed to fetch reputation scores:', error);
    }
  };

  const fetchPresentationTemplates = async () => {
    try {
      const response = await api.get('/verifiable-credential-service/api/v1/presentations/templates');
      setPresentationTemplates(response.data);
    } catch (error) {
      console.error('Failed to fetch presentation templates:', error);
    }
  };

  const handleExportCredential = async (values) => {
    try {
      const response = await api.post('/verifiable-credential-service/api/v1/cross-chain/export', {
        credential_id: selectedCredential.id,
        target_chain: values.targetChain,
        recipient_address: values.recipientAddress
      });
      
      message.success('Credential exported successfully!');
      setExportModalVisible(false);
      exportForm.resetFields();
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Failed to export credential');
    }
  };

  const handleCreatePresentation = async (values) => {
    try {
      const response = await api.post('/verifiable-credential-service/api/v1/presentations', {
        credential_ids: values.credentialIds,
        purpose: values.purpose,
        challenge: values.challenge,
        domain: values.domain,
        expires_in: values.expiresIn || 3600
      });
      
      // Download presentation as JSON
      const blob = new Blob([JSON.stringify(response.data, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'verifiable-presentation.json';
      a.click();
      
      message.success('Presentation created successfully!');
      setPresentationModalVisible(false);
      presentationForm.resetFields();
    } catch (error) {
      console.error('Presentation creation failed:', error);
      message.error('Failed to create presentation');
    }
  };

  const getCredentialIcon = (type) => {
    if (type.includes('AssetCreation')) return <FileProtectOutlined />;
    if (type.includes('TrustScore')) return <SafetyOutlined />;
    if (type.includes('Researcher')) return <TeamOutlined />;
    if (type.includes('Processing')) return <CodeOutlined />;
    return <FileProtectOutlined />;
  };

  const getReputationIcon = (dimension) => {
    switch (dimension) {
      case 'technical': return <CodeOutlined />;
      case 'collaboration': return <TeamOutlined />;
      case 'governance': return <SafetyOutlined />;
      case 'creativity': return <BulbOutlined />;
      case 'reliability': return <LineChartOutlined />;
      default: return <TrophyOutlined />;
    }
  };

  const renderCredentialCard = (credential) => {
    const isExpired = credential.expirationDate && new Date(credential.expirationDate) < new Date();
    const credentialType = Array.isArray(credential.type) ? credential.type[1] || credential.type[0] : credential.type;
    
    return (
      <Card
        key={credential.id}
        className="credential-card"
        hoverable
        actions={[
          <Tooltip title="Export to Blockchain">
            <Button 
              type="text" 
              icon={<ExportOutlined />}
              onClick={() => {
                setSelectedCredential(credential);
                setExportModalVisible(true);
              }}
            >
              Export
            </Button>
          </Tooltip>,
          <Tooltip title="Create Presentation">
            <Button 
              type="text" 
              icon={<QrcodeOutlined />}
              onClick={() => {
                setSelectedCredential(credential);
                presentationForm.setFieldsValue({ credentialIds: [credential.id] });
                setPresentationModalVisible(true);
              }}
            >
              Present
            </Button>
          </Tooltip>
        ]}
      >
        <Card.Meta
          avatar={getCredentialIcon(credentialType)}
          title={
            <Space>
              {credentialType}
              {isExpired ? (
                <Tag color="red" icon={<ClockCircleOutlined />}>Expired</Tag>
              ) : (
                <Tag color="green" icon={<CheckCircleOutlined />}>Valid</Tag>
              )}
            </Space>
          }
          description={
            <>
              <Text type="secondary">Issued: {new Date(credential.issuanceDate).toLocaleDateString()}</Text>
              <br />
              <Text type="secondary">Issuer: {credential.issuer?.id || credential.issuer}</Text>
              {credential.credentialSubject && (
                <div className="credential-claims">
                  <Divider style={{ margin: '8px 0' }} />
                  <Space direction="vertical" size="small" style={{ width: '100%' }}>
                    {Object.entries(credential.credentialSubject).map(([key, value]) => (
                      key !== 'id' && (
                        <div key={key}>
                          <Text strong>{key}: </Text>
                          <Text>{typeof value === 'object' ? JSON.stringify(value) : value}</Text>
                        </div>
                      )
                    ))}
                  </Space>
                </div>
              )}
            </>
          }
        />
      </Card>
    );
  };

  const renderReputationDashboard = () => {
    if (!reputationScores) {
      return <Card loading={true} />;
    }

    const dimensions = [
      { key: 'technicalProwess', label: 'Technical', color: '#1890ff' },
      { key: 'collaborationRating', label: 'Collaboration', color: '#52c41a' },
      { key: 'governanceInfluence', label: 'Governance', color: '#722ed1' },
      { key: 'creativityIndex', label: 'Creativity', color: '#fa8c16' },
      { key: 'reliabilityScore', label: 'Reliability', color: '#13c2c2' }
    ];

    return (
      <Card title="Multi-Dimensional Reputation" className="reputation-dashboard">
        <div className="reputation-grid">
          {dimensions.map(dim => (
            <Card key={dim.key} size="small" className="dimension-card">
              <Statistic
                title={
                  <Space>
                    {getReputationIcon(dim.key.toLowerCase())}
                    {dim.label}
                  </Space>
                }
                value={reputationScores[dim.key] || 0}
                suffix="/ 100"
                valueStyle={{ color: dim.color }}
              />
              <Progress 
                percent={reputationScores[dim.key] || 0} 
                strokeColor={dim.color}
                showInfo={false}
                size="small"
              />
            </Card>
          ))}
        </div>
        <Divider />
        <div className="total-score">
          <Statistic
            title="Overall Trust Score"
            value={reputationScores.totalScore || 0}
            suffix="/ 100"
            prefix={<TrophyOutlined />}
            valueStyle={{ color: '#1890ff', fontSize: '32px' }}
          />
        </div>
      </Card>
    );
  };

  return (
    <div className="trust-wallet">
      <Card className="wallet-header">
        <Title level={2}>
          <WalletOutlined /> Trust Wallet
        </Title>
        <Paragraph>
          Manage your verifiable credentials, export them to public blockchains, 
          and create presentations for external verification.
        </Paragraph>
      </Card>

      <Tabs defaultActiveKey="credentials" size="large">
        <TabPane 
          tab={
            <span>
              <FileProtectOutlined />
              My Credentials
              <Badge count={credentials.length} style={{ marginLeft: 8 }} />
            </span>
          } 
          key="credentials"
        >
          <div className="credentials-grid">
            {loading ? (
              <Card loading={true} />
            ) : credentials.length > 0 ? (
              credentials.map(renderCredentialCard)
            ) : (
              <Alert
                message="No Credentials Yet"
                description="You haven't earned any verifiable credentials yet. Complete tasks and contribute to the platform to earn credentials."
                type="info"
                showIcon
              />
            )}
          </div>
        </TabPane>

        <TabPane 
          tab={
            <span>
              <TrophyOutlined />
              Reputation
            </span>
          } 
          key="reputation"
        >
          {renderReputationDashboard()}
        </TabPane>

        <TabPane 
          tab={
            <span>
              <GlobalOutlined />
              Cross-Chain
            </span>
          } 
          key="crosschain"
        >
          <Card title="Supported Blockchain Networks">
            <List
              dataSource={supportedChains}
              renderItem={chain => (
                <List.Item>
                  <List.Item.Meta
                    title={chain.name}
                    description={
                      <Space>
                        <Text>Chain ID: {chain.chain_id}</Text>
                        {chain.connected ? (
                          <Tag color="green">Connected</Tag>
                        ) : (
                          <Tag color="red">Disconnected</Tag>
                        )}
                        {chain.sbt_deployed && <Tag color="blue">SBT Ready</Tag>}
                      </Space>
                    }
                  />
                  {chain.gas_price && (
                    <Text type="secondary">
                      Gas: {(chain.gas_price / 1e9).toFixed(2)} Gwei
                    </Text>
                  )}
                </List.Item>
              )}
            />
          </Card>
        </TabPane>
      </Tabs>

      {/* Export Modal */}
      <Modal
        title="Export Credential to Blockchain"
        visible={exportModalVisible}
        onCancel={() => {
          setExportModalVisible(false);
          exportForm.resetFields();
        }}
        footer={null}
      >
        <Form
          form={exportForm}
          layout="vertical"
          onFinish={handleExportCredential}
        >
          <Alert
            message="Export as SoulBound Token"
            description="Your credential will be permanently recorded on the selected blockchain as a non-transferable token."
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />
          
          <Form.Item
            name="targetChain"
            label="Target Blockchain"
            rules={[{ required: true, message: 'Please select a blockchain' }]}
          >
            <Select placeholder="Select blockchain network">
              {supportedChains
                .filter(chain => chain.connected && chain.sbt_deployed)
                .map(chain => (
                  <Option key={chain.network} value={chain.network}>
                    {chain.name}
                  </Option>
                ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="recipientAddress"
            label="Recipient Address"
            rules={[
              { required: true, message: 'Please enter recipient address' },
              { pattern: /^0x[a-fA-F0-9]{40}$/, message: 'Invalid Ethereum address' }
            ]}
          >
            <Input placeholder="0x..." />
          </Form.Item>

          <Form.Item>
            <Button type="primary" htmlType="submit" block>
              Export Credential
            </Button>
          </Form.Item>
        </Form>
      </Modal>

      {/* Presentation Modal */}
      <Modal
        title="Create Verifiable Presentation"
        visible={presentationModalVisible}
        onCancel={() => {
          setPresentationModalVisible(false);
          presentationForm.resetFields();
        }}
        footer={null}
        width={600}
      >
        <Form
          form={presentationForm}
          layout="vertical"
          onFinish={handleCreatePresentation}
        >
          <Form.Item
            name="credentialIds"
            label="Select Credentials"
            rules={[{ required: true, message: 'Please select at least one credential' }]}
          >
            <Select mode="multiple" placeholder="Select credentials to include">
              {credentials.map(cred => (
                <Option key={cred.id} value={cred.id}>
                  {Array.isArray(cred.type) ? cred.type[1] || cred.type[0] : cred.type}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="purpose"
            label="Purpose"
            initialValue="authentication"
          >
            <Select>
              <Option value="authentication">Authentication</Option>
              <Option value="verification">Verification</Option>
              <Option value="assertion">Assertion</Option>
              <Option value="capabilityInvocation">Capability Invocation</Option>
            </Select>
          </Form.Item>

          <Form.Item
            name="template"
            label="Use Template (Optional)"
          >
            <Select placeholder="Select a template" allowClear>
              {presentationTemplates.map(template => (
                <Option key={template.name} value={template.name}>
                  {template.name} - {template.description}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="challenge"
            label="Challenge (Optional)"
          >
            <Input placeholder="Challenge string from verifier" />
          </Form.Item>

          <Form.Item
            name="domain"
            label="Domain (Optional)"
          >
            <Input placeholder="Domain of the verifier" />
          </Form.Item>

          <Form.Item
            name="expiresIn"
            label="Expires In (seconds)"
            initialValue={3600}
          >
            <InputNumber min={60} max={86400} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item>
            <Button type="primary" htmlType="submit" block>
              Create Presentation
            </Button>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default TrustWallet; 