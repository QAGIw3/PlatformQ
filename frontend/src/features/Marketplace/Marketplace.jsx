import React, { useState, useEffect } from 'react';
import { useWeb3 } from '../../hooks/useWeb3';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/tabs';
import { Badge } from '../../components/ui/badge';
import { Input } from '../../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Alert, AlertDescription } from '../../components/ui/alert';
import { Loader2, ShoppingCart, Package, Gavel, Users, Coins, BarChart3, Vote } from 'lucide-react';
import axios from 'axios';
import './Marketplace.css';

const BLOCKCHAIN_SERVICE_URL = process.env.REACT_APP_BLOCKCHAIN_SERVICE_URL || 'http://localhost:8001';
const MLOPS_SERVICE_URL = process.env.REACT_APP_MLOPS_SERVICE_URL || 'http://localhost:8002';
const DATASET_SERVICE_URL = process.env.REACT_APP_DATASET_SERVICE_URL || 'http://localhost:8003';
const COMPUTE_SERVICE_URL = process.env.REACT_APP_COMPUTE_SERVICE_URL || 'http://localhost:8004';

const Marketplace = () => {
  const { account, chainId, connectWallet, switchChain } = useWeb3();
  const [activeTab, setActiveTab] = useState('models');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  
  // Marketplace state
  const [models, setModels] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [computeOffers, setComputeOffers] = useState([]);
  const [auctions, setAuctions] = useState([]);
  const [bundles, setBundles] = useState([]);
  const [fractionalAssets, setFractionalAssets] = useState([]);
  const [loanOffers, setLoanOffers] = useState([]);
  const [yieldPools, setYieldPools] = useState([]);
  const [daoProposals, setDaoProposals] = useState([]);
  
  // Form state
  const [auctionForm, setAuctionForm] = useState({
    tokenId: '',
    auctionType: 'english',
    startPrice: '',
    endPrice: '',
    duration: '86400', // 1 day in seconds
    minBidIncrement: '0.01',
    priceDecrement: '0.001'
  });
  
  const [bundleForm, setBundleForm] = useState({
    name: '',
    description: '',
    assetIds: [],
    price: '',
    maxSupply: '0'
  });
  
  const [fractionalizeForm, setFractionalizeForm] = useState({
    tokenId: '',
    totalShares: '10000',
    sharePrice: '0.001',
    reservePrice: '1'
  });

  // Dataset search and filter state
  const [datasetFilters, setDatasetFilters] = useState({
    searchQuery: '',
    category: 'all',
    minSize: '',
    maxSize: '',
    format: 'all',
    priceRange: 'all',
    qualityScore: 0,
    sortBy: 'relevance'
  });
  
  // Compute resource state
  const [computeFilters, setComputeFilters] = useState({
    resourceType: 'all',
    gpuType: 'all',
    minMemory: '',
    maxPrice: '',
    region: 'all',
    availability: 'all'
  });
  
  // Revenue sharing state
  const [revenueShares, setRevenueShares] = useState([]);
  const [selectedAssetForRevenue, setSelectedAssetForRevenue] = useState(null);

  useEffect(() => {
    if (account) {
      loadMarketplaceData();
    }
  }, [account, activeTab]);

  const loadMarketplaceData = async () => {
    setLoading(true);
    setError('');
    
    try {
      switch (activeTab) {
        case 'models':
          await loadModels();
          break;
        case 'datasets':
          await loadDatasets();
          break;
        case 'compute':
          await loadComputeOffers();
          break;
        case 'auctions':
          await loadAuctions();
          break;
        case 'bundles':
          await loadBundles();
          break;
        case 'fractional':
          await loadFractionalAssets();
          break;
        case 'defi':
          await loadDeFiData();
          break;
        case 'dao':
          await loadDAOData();
          break;
      }
    } catch (err) {
      setError(err.message || 'Failed to load marketplace data');
    } finally {
      setLoading(false);
    }
  };

  const loadModels = async () => {
    const response = await axios.get(`${MLOPS_SERVICE_URL}/api/v1/marketplace/models`);
    setModels(response.data.models || []);
  };

  const loadDatasets = async () => {
    const params = new URLSearchParams();
    if (datasetFilters.searchQuery) params.append('q', datasetFilters.searchQuery);
    if (datasetFilters.category !== 'all') params.append('category', datasetFilters.category);
    if (datasetFilters.format !== 'all') params.append('format', datasetFilters.format);
    if (datasetFilters.minSize) params.append('min_size', datasetFilters.minSize);
    if (datasetFilters.maxSize) params.append('max_size', datasetFilters.maxSize);
    if (datasetFilters.qualityScore > 0) params.append('min_quality', datasetFilters.qualityScore);
    
    const response = await axios.get(`${DATASET_SERVICE_URL}/api/v1/datasets/search?${params}`);
    setDatasets(response.data || []);
  };

  const loadComputeOffers = async () => {
    const response = await axios.get(`${COMPUTE_SERVICE_URL}/api/v1/offerings/search`);
    setComputeOffers(response.data || []);
  };

  const loadAuctions = async () => {
    const response = await axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/auctions/active`);
    setAuctions(response.data.auctions || []);
  };

  const loadBundles = async () => {
    const response = await axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/bundles`);
    setBundles(response.data.bundles || []);
  };

  const loadFractionalAssets = async () => {
    const response = await axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/fractional`);
    setFractionalAssets(response.data.assets || []);
  };

  const loadDeFiData = async () => {
    const [loansRes, yieldsRes] = await Promise.all([
      axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/defi/lending/offers`),
      axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/defi/yield-farming/pools`)
    ]);
    setLoanOffers(loansRes.data.offers || []);
    setYieldPools(yieldsRes.data.pools || []);
  };

  const loadDAOData = async () => {
    const response = await axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/dao/proposals/active`);
    setDaoProposals(response.data.proposals || []);
  };

  const createAuction = async () => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/defi/auctions/create`, {
        chain: getChainName(chainId),
        ...auctionForm
      });
      
      setSuccess('Auction created successfully!');
      await loadAuctions();
      setAuctionForm({
        tokenId: '',
        auctionType: 'english',
        startPrice: '',
        endPrice: '',
        duration: '86400',
        minBidIncrement: '0.01',
        priceDecrement: '0.001'
      });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to create auction');
    } finally {
      setLoading(false);
        }
  };

  const bidOnAuction = async (auctionId, bidAmount) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/defi/auctions/${auctionId}/bid`, {
        chain: getChainName(chainId),
        bid_amount: bidAmount
      });
      
      setSuccess('Bid placed successfully!');
      await loadAuctions();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to place bid');
    } finally {
      setLoading(false);
    }
  };

  const createBundle = async () => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/bundles/create`, {
        chain: getChainName(chainId),
        ...bundleForm
      });
      
      setSuccess('Bundle created successfully!');
      await loadBundles();
      setBundleForm({
        name: '',
        description: '',
        assetIds: [],
        price: '',
        maxSupply: '0'
      });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to create bundle');
    } finally {
      setLoading(false);
    }
  };

  const fractionalizeAsset = async () => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/fractional/create`, {
        chain: getChainName(chainId),
        ...fractionalizeForm
      });
      
      setSuccess('Asset fractionalized successfully!');
      await loadFractionalAssets();
      setFractionalizeForm({
        tokenId: '',
        totalShares: '10000',
        sharePrice: '0.001',
        reservePrice: '1'
      });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to fractionalize asset');
    } finally {
      setLoading(false);
    }
  };

  const stakeInPool = async (poolId, amount) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/defi/yield-farming/stake`, {
        chain: getChainName(chainId),
        pool_id: poolId,
        amount: amount
      });
      
      setSuccess('Tokens staked successfully!');
      await loadDeFiData();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to stake tokens');
    } finally {
      setLoading(false);
    }
  };

  const voteOnProposal = async (proposalId, voteType) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/dao/proposals/${proposalId}/vote`, {
        chain: getChainName(chainId),
        vote_type: voteType
      });
      
      setSuccess('Vote cast successfully!');
      await loadDAOData();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to cast vote');
    } finally {
      setLoading(false);
    }
  };

  const purchaseDataset = async (datasetId, price) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${DATASET_SERVICE_URL}/api/v1/datasets/${datasetId}/purchase`, {
        chain: getChainName(chainId),
        payment_amount: price
      });
      
      setSuccess('Dataset purchased successfully! Check your downloads.');
      await loadDatasets();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to purchase dataset');
    } finally {
      setLoading(false);
    }
  };

  const provisionComputeResource = async (resourceId, duration, price) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${COMPUTE_SERVICE_URL}/api/v1/compute/${resourceId}/provision`, {
        chain: getChainName(chainId),
        duration_hours: duration,
        payment_amount: price
      });
      
      setSuccess('Compute resource provisioned! Access details sent to your email.');
      await loadComputeOffers();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to provision compute resource');
    } finally {
      setLoading(false);
    }
  };

  const purchaseBundle = async (bundleId, price) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/bundles/${bundleId}/purchase`, {
        chain: getChainName(chainId),
        payment_amount: price
      });
      
      setSuccess('Bundle purchased successfully!');
      await loadBundles();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to purchase bundle');
    } finally {
      setLoading(false);
    }
  };

  const buyFractionalShares = async (assetId, shareAmount, totalPrice) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/fractional/${assetId}/buy`, {
        chain: getChainName(chainId),
        share_amount: shareAmount,
        payment_amount: totalPrice
      });
      
      setSuccess('Fractional shares purchased successfully!');
      await loadFractionalAssets();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to purchase shares');
    } finally {
      setLoading(false);
    }
  };

  const configureRevenueSharing = async (assetId, recipients) => {
    setLoading(true);
    setError('');
    
    try {
      await axios.post(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/revenue-sharing/configure`, {
        chain: getChainName(chainId),
        asset_id: assetId,
        recipients: recipients // Array of {address, percentage}
      });
      
      setSuccess('Revenue sharing configured successfully!');
      setSelectedAssetForRevenue(null);
      setRevenueShares([]);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to configure revenue sharing');
    } finally {
      setLoading(false);
    }
  };

  const loadRevenueData = async () => {
    const response = await axios.get(`${BLOCKCHAIN_SERVICE_URL}/api/v1/marketplace/revenue-sharing/assets`);
    setRevenueShares(response.data.assets || []);
  };

  const getChainName = (chainId) => {
    const chainMap = {
      1: 'ethereum',
      137: 'polygon',
      43114: 'avalanche',
      56: 'bsc',
      250: 'fantom'
    };
    return chainMap[chainId] || 'ethereum';
  };

  const renderModelsTab = () => (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {models.map((model) => (
        <Card key={model.id} className="hover:shadow-lg transition-shadow">
          <CardHeader>
            <CardTitle className="flex justify-between items-center">
              <span>{model.name}</span>
              <Badge>{model.category}</Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-gray-600 mb-4">{model.description}</p>
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-sm">Accuracy:</span>
                <span className="font-semibold">{(model.metrics?.accuracy * 100).toFixed(2)}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm">Version:</span>
                <span className="font-semibold">{model.version}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm">Price:</span>
                <span className="font-semibold">{model.price} ETH</span>
              </div>
          </div>
            <div className="mt-4 space-y-2">
              <Button className="w-full" size="sm">
                Purchase License
              </Button>
              <Button className="w-full" variant="outline" size="sm">
                View Performance
              </Button>
              </div>
          </CardContent>
        </Card>
      ))}
              </div>
  );

  const renderAuctionsTab = () => (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Create Auction</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Input
              placeholder="Token ID"
              value={auctionForm.tokenId}
              onChange={(e) => setAuctionForm({...auctionForm, tokenId: e.target.value})}
            />
            <Select
              value={auctionForm.auctionType}
              onValueChange={(value) => setAuctionForm({...auctionForm, auctionType: value})}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="english">English Auction</SelectItem>
                <SelectItem value="dutch">Dutch Auction</SelectItem>
              </SelectContent>
            </Select>
            <Input
              placeholder="Start Price (ETH)"
              value={auctionForm.startPrice}
              onChange={(e) => setAuctionForm({...auctionForm, startPrice: e.target.value})}
            />
            {auctionForm.auctionType === 'dutch' && (
              <Input
                placeholder="End Price (ETH)"
                value={auctionForm.endPrice}
                onChange={(e) => setAuctionForm({...auctionForm, endPrice: e.target.value})}
              />
            )}
          </div>
          <Button 
            className="mt-4" 
            onClick={createAuction}
            disabled={loading || !auctionForm.tokenId || !auctionForm.startPrice}
          >
            {loading ? <Loader2 className="animate-spin" /> : 'Create Auction'}
          </Button>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {auctions.map((auction) => (
          <Card key={auction.id} className="hover:shadow-lg transition-shadow">
            <CardHeader>
              <CardTitle className="flex justify-between items-center">
                <span>Token #{auction.tokenId}</span>
                <Badge variant={auction.type === 'english' ? 'default' : 'secondary'}>
                  {auction.type}
                </Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-sm">Current Price:</span>
                  <span className="font-semibold">{auction.currentPrice} ETH</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm">Ends In:</span>
                  <span className="font-semibold">{auction.timeRemaining}</span>
                </div>
                {auction.type === 'english' && (
                  <div className="flex justify-between">
                    <span className="text-sm">Highest Bidder:</span>
                    <span className="font-semibold text-xs">
                      {auction.highestBidder?.slice(0, 6)}...{auction.highestBidder?.slice(-4)}
                    </span>
        </div>
                )}
              </div>
              <Button 
                className="w-full mt-4" 
                size="sm"
                onClick={() => {
                  if (auction.type === 'english') {
                    const bidAmount = prompt('Enter bid amount (ETH):');
                    if (bidAmount) bidOnAuction(auction.id, bidAmount);
                  } else {
                    bidOnAuction(auction.id, auction.currentPrice);
                  }
                }}
              >
                {auction.type === 'english' ? 'Place Bid' : 'Buy Now'}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );

  const renderDeFiTab = () => (
    <div className="space-y-6">
                <div>
        <h3 className="text-lg font-semibold mb-4">Yield Farming Pools</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {yieldPools.map((pool) => (
            <Card key={pool.id}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Coins className="h-5 w-5" />
                  {pool.stakingToken} Pool
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-sm">APY:</span>
                    <span className="font-semibold text-green-600">{pool.apy}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Total Staked:</span>
                    <span className="font-semibold">${pool.totalStaked.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Lock Period:</span>
                    <span className="font-semibold">{pool.lockPeriod} days</span>
                  </div>
                </div>
                <Button 
                  className="w-full mt-4" 
                  size="sm"
                  onClick={() => {
                    const amount = prompt('Enter stake amount:');
                    if (amount) stakeInPool(pool.id, amount);
                  }}
                >
                  Stake Tokens
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>

                <div>
        <h3 className="text-lg font-semibold mb-4">NFT Lending</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {loanOffers.map((offer) => (
            <Card key={offer.id}>
              <CardHeader>
                <CardTitle>Loan Offer #{offer.id}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-sm">Max Loan:</span>
                    <span className="font-semibold">{offer.maxLoanAmount} USDC</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Interest Rate:</span>
                    <span className="font-semibold">{offer.interestRate}% APR</span>
                </div>
                  <div className="flex justify-between">
                    <span className="text-sm">Duration:</span>
                    <span className="font-semibold">{offer.minDuration}-{offer.maxDuration} days</span>
                  </div>
                </div>
                <Button className="w-full mt-4" size="sm">
                  Borrow Against NFT
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );

  const renderDAOTab = () => (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Vote className="h-5 w-5" />
            Active Proposals
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {daoProposals.map((proposal) => (
              <div key={proposal.id} className="border rounded-lg p-4">
                <div className="flex justify-between items-start mb-2">
                  <h4 className="font-semibold">{proposal.title}</h4>
                  <Badge>{proposal.status}</Badge>
                </div>
                <p className="text-sm text-gray-600 mb-4">{proposal.description}</p>
                <div className="grid grid-cols-3 gap-2 mb-4">
                  <div className="text-center">
                    <p className="text-sm text-gray-500">For</p>
                    <p className="font-semibold text-green-600">{proposal.forVotes}</p>
                  </div>
                  <div className="text-center">
                    <p className="text-sm text-gray-500">Against</p>
                    <p className="font-semibold text-red-600">{proposal.againstVotes}</p>
                  </div>
                  <div className="text-center">
                    <p className="text-sm text-gray-500">Abstain</p>
                    <p className="font-semibold text-gray-600">{proposal.abstainVotes}</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <Button 
                    size="sm" 
                    variant="outline"
                    onClick={() => voteOnProposal(proposal.id, 'for')}
                  >
                    Vote For
              </Button>
                  <Button 
                    size="sm" 
                    variant="outline"
                    onClick={() => voteOnProposal(proposal.id, 'against')}
                  >
                    Vote Against
                  </Button>
                  <Button 
                    size="sm" 
                    variant="outline"
                    onClick={() => voteOnProposal(proposal.id, 'abstain')}
                  >
                    Abstain
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );

  return (
    <div className="container mx-auto p-6">
      <div className="mb-6">
        <h1 className="text-3xl font-bold mb-2">Decentralized Marketplace</h1>
        <p className="text-gray-600">Trade AI models, datasets, and compute resources</p>
      </div>

      {!account ? (
        <Card>
          <CardContent className="text-center py-8">
            <p className="mb-4">Connect your wallet to access the marketplace</p>
            <Button onClick={connectWallet}>Connect Wallet</Button>
          </CardContent>
        </Card>
      ) : (
        <>
          {error && (
            <Alert variant="destructive" className="mb-4">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          
          {success && (
            <Alert className="mb-4">
              <AlertDescription>{success}</AlertDescription>
            </Alert>
          )}

          <Tabs value={activeTab} onValueChange={setActiveTab}>
            <TabsList className="grid grid-cols-8 w-full">
              <TabsTrigger value="models">Models</TabsTrigger>
              <TabsTrigger value="datasets">Datasets</TabsTrigger>
              <TabsTrigger value="compute">Compute</TabsTrigger>
              <TabsTrigger value="auctions">Auctions</TabsTrigger>
              <TabsTrigger value="bundles">Bundles</TabsTrigger>
              <TabsTrigger value="fractional">Fractional</TabsTrigger>
              <TabsTrigger value="defi">DeFi</TabsTrigger>
              <TabsTrigger value="dao">DAO</TabsTrigger>
            </TabsList>
          
            <TabsContent value="models">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                renderModelsTab()
              )}
            </TabsContent>

            <TabsContent value="datasets">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                <div className="space-y-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Search Datasets</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <Input
                          placeholder="Search datasets..."
                          value={datasetFilters.searchQuery}
                          onChange={(e) => setDatasetFilters({...datasetFilters, searchQuery: e.target.value})}
                        />
                        <Select
                          value={datasetFilters.category}
                          onValueChange={(value) => setDatasetFilters({...datasetFilters, category: value})}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="All Categories" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Categories</SelectItem>
                            <SelectItem value="image">Image</SelectItem>
                            <SelectItem value="text">Text</SelectItem>
                            <SelectItem value="audio">Audio</SelectItem>
                            <SelectItem value="video">Video</SelectItem>
                            <SelectItem value="tabular">Tabular</SelectItem>
                          </SelectContent>
                        </Select>
                        <Select
                          value={datasetFilters.format}
                          onValueChange={(value) => setDatasetFilters({...datasetFilters, format: value})}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="All Formats" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Formats</SelectItem>
                            <SelectItem value="csv">CSV</SelectItem>
                            <SelectItem value="json">JSON</SelectItem>
                            <SelectItem value="parquet">Parquet</SelectItem>
                            <SelectItem value="hdf5">HDF5</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <Button 
                        className="mt-4" 
                        onClick={loadDatasets}
                        disabled={loading}
                      >
                        {loading ? <Loader2 className="animate-spin" /> : 'Search Datasets'}
                      </Button>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Dataset Results</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {datasets.map((dataset) => (
                          <Card key={dataset.id} className="hover:shadow-lg transition-shadow">
                            <CardHeader>
                              <CardTitle className="flex justify-between items-center">
                                <span>{dataset.name}</span>
                                <Badge>{dataset.category}</Badge>
                              </CardTitle>
                            </CardHeader>
                            <CardContent>
                              <p className="text-sm text-gray-600 mb-4">{dataset.description}</p>
                              <div className="space-y-2">
                                <div className="flex justify-between">
                                  <span className="text-sm">Size:</span>
                                  <span className="font-semibold">{dataset.size} MB</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Format:</span>
                                  <span className="font-semibold">{dataset.format}</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Price:</span>
                                  <span className="font-semibold">{dataset.price} ETH</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Quality Score:</span>
                                  <span className="font-semibold">{dataset.qualityScore}</span>
                                </div>
                              </div>
                              <Button 
                                className="w-full mt-4" 
                                size="sm"
                                onClick={() => {
                                  // Placeholder for dataset purchase logic
                                  alert(`Purchase dataset: ${dataset.name}`);
                                  purchaseDataset(dataset.id, dataset.price);
                                }}
                              >
                                Purchase Dataset
                              </Button>
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            <TabsContent value="compute">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                <div className="space-y-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Search Compute Resources</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <Select
                          value={computeFilters.resourceType}
                          onValueChange={(value) => setComputeFilters({...computeFilters, resourceType: value})}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="All Resource Types" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Resource Types</SelectItem>
                            <SelectItem value="gpu">GPU</SelectItem>
                            <SelectItem value="cpu">CPU</SelectItem>
                            <SelectItem value="storage">Storage</SelectItem>
                          </SelectContent>
                        </Select>
                        <Select
                          value={computeFilters.gpuType}
                          onValueChange={(value) => setComputeFilters({...computeFilters, gpuType: value})}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="All GPU Types" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All GPU Types</SelectItem>
                            <SelectItem value="nvidia-rtx-3090">NVIDIA RTX 3090</SelectItem>
                            <SelectItem value="nvidia-a100">NVIDIA A100</SelectItem>
                            <SelectItem value="amd-radeon-rx-6800">AMD Radeon RX 6800</SelectItem>
                          </SelectContent>
                        </Select>
                        <Select
                          value={computeFilters.region}
                          onValueChange={(value) => setComputeFilters({...computeFilters, region: value})}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="All Regions" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Regions</SelectItem>
                            <SelectItem value="us-east">US East</SelectItem>
                            <SelectItem value="us-west">US West</SelectItem>
                            <SelectItem value="eu-central">EU Central</SelectItem>
                            <SelectItem value="ap-southeast">AP Southeast</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <Button 
                        className="mt-4" 
                        onClick={loadComputeOffers}
                        disabled={loading}
                      >
                        {loading ? <Loader2 className="animate-spin" /> : 'Search Compute Resources'}
                      </Button>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Compute Resource Results</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {computeOffers.map((offer) => (
                          <Card key={offer.id} className="hover:shadow-lg transition-shadow">
                            <CardHeader>
                              <CardTitle className="flex justify-between items-center">
                                <span>{offer.resourceType} - {offer.gpuType}</span>
                                <Badge>{offer.region}</Badge>
                              </CardTitle>
                            </CardHeader>
                            <CardContent>
                              <div className="space-y-2">
                                <div className="flex justify-between">
                                  <span className="text-sm">Price:</span>
                                  <span className="font-semibold">{offer.price} ETH</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Memory:</span>
                                  <span className="font-semibold">{offer.memory} GB</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Availability:</span>
                                  <span className="font-semibold">{offer.availability}</span>
                                </div>
                              </div>
                              <Button 
                                className="w-full mt-4" 
                                size="sm"
                                onClick={() => {
                                  // Placeholder for compute resource purchase logic
                                  alert(`Purchase compute resource: ${offer.resourceType} - ${offer.gpuType}`);
                                  const duration = prompt('Enter duration in hours:');
                                  if (duration) provisionComputeResource(offer.id, duration, offer.price);
                                }}
                              >
                                Purchase Resource
                              </Button>
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            <TabsContent value="auctions">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                renderAuctionsTab()
              )}
            </TabsContent>

            <TabsContent value="bundles">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                <div className="space-y-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Create Bundle</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <Input
                          placeholder="Bundle Name"
                          value={bundleForm.name}
                          onChange={(e) => setBundleForm({...bundleForm, name: e.target.value})}
                        />
                        <Input
                          placeholder="Bundle Description"
                          value={bundleForm.description}
                          onChange={(e) => setBundleForm({...bundleForm, description: e.target.value})}
                        />
                        <Input
                          placeholder="Bundle Price (ETH)"
                          value={bundleForm.price}
                          onChange={(e) => setBundleForm({...bundleForm, price: e.target.value})}
                        />
                        <Input
                          placeholder="Max Supply"
                          value={bundleForm.maxSupply}
                          onChange={(e) => setBundleForm({...bundleForm, maxSupply: e.target.value})}
                        />
                      </div>
                      <Button 
                        className="mt-4" 
                        onClick={createBundle}
                        disabled={loading || !bundleForm.name || !bundleForm.price}
                      >
                        {loading ? <Loader2 className="animate-spin" /> : 'Create Bundle'}
                      </Button>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Your Bundles</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {bundles.map((bundle) => (
                          <Card key={bundle.id} className="hover:shadow-lg transition-shadow">
                            <CardHeader>
                              <CardTitle className="flex justify-between items-center">
                                <span>{bundle.name}</span>
                                <Badge>{bundle.totalSupply}/{bundle.maxSupply}</Badge>
                              </CardTitle>
                            </CardHeader>
                            <CardContent>
                              <p className="text-sm text-gray-600 mb-4">{bundle.description}</p>
                              <div className="space-y-2">
                                <div className="flex justify-between">
                                  <span className="text-sm">Price:</span>
                                  <span className="font-semibold">{bundle.price} ETH</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Supply:</span>
                                  <span className="font-semibold">{bundle.totalSupply}/{bundle.maxSupply}</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Revenue Share:</span>
                                  <span className="font-semibold">{bundle.revenueShare}%</span>
                                </div>
                              </div>
                              <Button 
                                className="w-full mt-4" 
                                size="sm"
                                onClick={() => {
                                  // Placeholder for bundle purchase logic
                                  alert(`Purchase bundle: ${bundle.name}`);
                                  purchaseBundle(bundle.id, bundle.price);
                                }}
                              >
                                Purchase Bundle
                              </Button>
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            <TabsContent value="fractional">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                <div className="space-y-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Fractionalize Asset</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <Input
                          placeholder="Token ID"
                          value={fractionalizeForm.tokenId}
                          onChange={(e) => setFractionalizeForm({...fractionalizeForm, tokenId: e.target.value})}
                        />
                        <Input
                          placeholder="Total Shares"
                          value={fractionalizeForm.totalShares}
                          onChange={(e) => setFractionalizeForm({...fractionalizeForm, totalShares: e.target.value})}
                        />
                        <Input
                          placeholder="Share Price (ETH)"
                          value={fractionalizeForm.sharePrice}
                          onChange={(e) => setFractionalizeForm({...fractionalizeForm, sharePrice: e.target.value})}
                        />
                        <Input
                          placeholder="Reserve Price (ETH)"
                          value={fractionalizeForm.reservePrice}
                          onChange={(e) => setFractionalizeForm({...fractionalizeForm, reservePrice: e.target.value})}
                        />
                      </div>
                      <Button 
                        className="mt-4" 
                        onClick={fractionalizeAsset}
                        disabled={loading || !fractionalizeForm.tokenId || !fractionalizeForm.totalShares}
                      >
                        {loading ? <Loader2 className="animate-spin" /> : 'Fractionalize Asset'}
                      </Button>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Your Fractional Assets</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {fractionalAssets.map((asset) => (
                          <Card key={asset.id} className="hover:shadow-lg transition-shadow">
                            <CardHeader>
                              <CardTitle className="flex justify-between items-center">
                                <span>Fractional Asset #{asset.id}</span>
                                <Badge>{asset.totalShares} Shares</Badge>
                              </CardTitle>
                            </CardHeader>
                            <CardContent>
                              <div className="space-y-2">
                                <div className="flex justify-between">
                                  <span className="text-sm">Total Shares:</span>
                                  <span className="font-semibold">{asset.totalShares}</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Share Price:</span>
                                  <span className="font-semibold">{asset.sharePrice} ETH</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm">Reserve Price:</span>
                                  <span className="font-semibold">{asset.reservePrice} ETH</span>
                                </div>
                              </div>
                              <Button 
                                className="w-full mt-4" 
                                size="sm"
                                onClick={() => {
                                  // Placeholder for fractional asset purchase logic
                                  alert(`Purchase fractional asset: ${asset.id}`);
                                  const shares = prompt('Enter number of shares to purchase:');
                                  if (shares) {
                                    const totalPrice = parseFloat(shares) * parseFloat(asset.sharePrice);
                                    buyFractionalShares(asset.id, shares, totalPrice.toString());
                                  }
                                }}
                              >
                                Purchase Fractional Asset
                              </Button>
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            <TabsContent value="defi">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                renderDeFiTab()
              )}
            </TabsContent>

            <TabsContent value="dao">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                renderDAOTab()
              )}
            </TabsContent>

            {/* Add other tab contents as needed */}
          </Tabs>

          {/* Revenue Sharing Modal */}
          {selectedAssetForRevenue && (
            <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4">
              <Card className="max-w-lg w-full">
                <CardHeader>
                  <CardTitle>Configure Revenue Sharing</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <p className="text-sm text-gray-600">
                      Configure how revenue from this asset will be distributed.
                    </p>
                    <div className="space-y-2">
                      {revenueShares.map((share, index) => (
                        <div key={index} className="flex gap-2">
                          <Input
                            placeholder="Wallet address"
                            value={share.address}
                            onChange={(e) => {
                              const newShares = [...revenueShares];
                              newShares[index].address = e.target.value;
                              setRevenueShares(newShares);
                            }}
                          />
                          <Input
                            type="number"
                            placeholder="Percentage"
                            value={share.percentage}
                            onChange={(e) => {
                              const newShares = [...revenueShares];
                              newShares[index].percentage = e.target.value;
                              setRevenueShares(newShares);
                            }}
                          />
                        </div>
                      ))}
                    </div>
                    <Button
                      variant="outline"
                      onClick={() => {
                        setRevenueShares([...revenueShares, { address: '', percentage: '' }]);
                      }}
                    >
                      Add Recipient
                    </Button>
                    <div className="flex gap-2">
                      <Button
                        onClick={() => {
                          configureRevenueSharing(selectedAssetForRevenue.id, revenueShares);
                        }}
                      >
                        Configure
                      </Button>
                      <Button
                        variant="outline"
                        onClick={() => {
                          setSelectedAssetForRevenue(null);
                          setRevenueShares([]);
                        }}
                      >
                        Cancel
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default Marketplace; 