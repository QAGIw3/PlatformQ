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
    const response = await axios.get(`${DATASET_SERVICE_URL}/api/v1/datasets/search`);
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

            <TabsContent value="auctions">
              {loading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="animate-spin h-8 w-8" />
                </div>
              ) : (
                renderAuctionsTab()
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
        </>
      )}
    </div>
  );
};

export default Marketplace; 