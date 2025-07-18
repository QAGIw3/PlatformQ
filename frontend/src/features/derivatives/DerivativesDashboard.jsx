import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useWeb3React } from '@web3-react/core';
import { 
  Box, Grid, Paper, Typography, Button, Tab, Tabs, 
  Card, CardContent, Chip, IconButton, Tooltip,
  Table, TableBody, TableCell, TableHead, TableRow,
  Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, Select, MenuItem, FormControl, InputLabel,
  Slider, Switch, FormControlLabel, Alert, Skeleton,
  LinearProgress, Badge, Divider, CircularProgress
} from '@mui/material';
import {
  TrendingUp, TrendingDown, ShowChart, AccountBalance,
  Warning, InfoOutlined, Settings, History, Assessment,
  Speed, MonetizationOn, Lock, Timer, NotificationsActive
} from '@mui/icons-material';
import { useSnackbar } from 'notistack';
import { ethers } from 'ethers';

// Custom components
import TradingChart from './components/TradingChart';
import OrderBook from './components/OrderBook';
import PositionManager from './components/PositionManager';
import MarketSelector from './components/MarketSelector';
import CollateralManager from './components/CollateralManager';
import RiskMetrics from './components/RiskMetrics';
import FundingRate from './components/FundingRate';
import LeaderBoard from './components/LeaderBoard';

// Hooks
import { useDerivativesContract } from '../../hooks/useDerivativesContract';
import { useWebSocket } from '../../hooks/useWebSocket';
import { usePriceFeeds } from '../../hooks/usePriceFeeds';
import { useMarketData } from '../../hooks/useMarketData';

// Utils
import { formatNumber, formatUSD, calculatePnL, calculateHealthFactor } from '../../utils/formatters';
import { MARKET_CONFIGS, COLLATERAL_TOKENS } from '../../constants/derivatives';

const DerivativesDashboard = () => {
  const { account, library } = useWeb3React();
  const { enqueueSnackbar } = useSnackbar();
  const derivativesContract = useDerivativesContract();

  // State management
  const [selectedMarket, setSelectedMarket] = useState('BTC-USD');
  const [activeTab, setActiveTab] = useState(0);
  const [orderType, setOrderType] = useState('market');
  const [side, setSide] = useState('long');
  const [size, setSize] = useState('');
  const [leverage, setLeverage] = useState(10);
  const [price, setPrice] = useState('');
  const [reduceOnly, setReduceOnly] = useState(false);
  const [postOnly, setPostOnly] = useState(false);
  const [positions, setPositions] = useState([]);
  const [orders, setOrders] = useState([]);
  const [collateralBalances, setCollateralBalances] = useState({});
  const [marketStats, setMarketStats] = useState({});
  const [isLoading, setIsLoading] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedPosition, setSelectedPosition] = useState(null);

  // WebSocket connections for real-time data
  const { 
    marketData, 
    orderBookData, 
    fundingRate,
    liquidations,
    isConnected 
  } = useWebSocket(selectedMarket);

  // Price feeds
  const { prices, volatility } = usePriceFeeds([selectedMarket]);

  // Market data
  const { 
    markets, 
    marketMetrics, 
    openInterest,
    volume24h 
  } = useMarketData();

  // Load user data on mount
  useEffect(() => {
    if (account && derivativesContract) {
      loadUserData();
      const interval = setInterval(loadUserData, 5000); // Refresh every 5s
      return () => clearInterval(interval);
    }
  }, [account, derivativesContract]);

  const loadUserData = async () => {
    try {
      // Load positions
      const userPositions = await derivativesContract.getUserPositions(account);
      setPositions(userPositions.map(formatPosition));

      // Load orders
      const userOrders = await derivativesContract.getUserOrders(account);
      setOrders(userOrders.map(formatOrder));

      // Load collateral balances
      const balances = {};
      for (const token of COLLATERAL_TOKENS) {
        const balance = await derivativesContract.collateralBalances(account, token.address);
        balances[token.symbol] = ethers.utils.formatUnits(balance, token.decimals);
      }
      setCollateralBalances(balances);

      // Load market statistics
      const stats = await derivativesContract.getMarketStats(selectedMarket);
      setMarketStats(formatMarketStats(stats));
    } catch (error) {
      console.error('Error loading user data:', error);
    }
  };

  // Calculate account metrics
  const accountMetrics = useMemo(() => {
    const totalCollateral = Object.entries(collateralBalances).reduce((sum, [symbol, balance]) => {
      const token = COLLATERAL_TOKENS.find(t => t.symbol === symbol);
      const price = prices[symbol] || 0;
      return sum + (parseFloat(balance) * price * (token?.weight || 1));
    }, 0);

    const usedCollateral = positions.reduce((sum, pos) => sum + pos.collateral, 0);
    const freeCollateral = totalCollateral - usedCollateral;

    const unrealizedPnL = positions.reduce((sum, pos) => {
      const currentPrice = prices[pos.market] || pos.entryPrice;
      return sum + calculatePnL(pos, currentPrice);
    }, 0);

    const totalEquity = totalCollateral + unrealizedPnL;
    const marginRatio = totalCollateral > 0 ? (usedCollateral / totalCollateral) * 100 : 0;

    return {
      totalCollateral,
      usedCollateral,
      freeCollateral,
      unrealizedPnL,
      totalEquity,
      marginRatio
    };
  }, [collateralBalances, positions, prices]);

  // Handle order submission
  const handleSubmitOrder = async () => {
    if (!account || !derivativesContract) {
      enqueueSnackbar('Please connect wallet', { variant: 'warning' });
      return;
    }

    if (!size || parseFloat(size) <= 0) {
      enqueueSnackbar('Please enter valid size', { variant: 'error' });
      return;
    }

    setIsLoading(true);
    try {
      const marketConfig = MARKET_CONFIGS[selectedMarket];
      const sizeWei = ethers.utils.parseUnits(size, 18);
      
      let tx;
      if (orderType === 'market') {
        tx = await derivativesContract.openPosition(
          marketConfig.id,
          side === 'long',
          sizeWei,
          leverage,
          300 // 3% max slippage
        );
      } else {
        const priceWei = ethers.utils.parseUnits(price, 8);
        tx = await derivativesContract.placeOrder({
          marketId: marketConfig.id,
          isLong: side === 'long',
          isMarket: false,
          size: sizeWei,
          price: priceWei,
          leverage,
          reduceOnly,
          expiry: Math.floor(Date.now() / 1000) + 86400, // 24h expiry
          nonce: Date.now()
        });
      }

      enqueueSnackbar('Order submitted', { variant: 'info' });
      const receipt = await tx.wait();
      
      if (receipt.status === 1) {
        enqueueSnackbar('Order executed successfully', { variant: 'success' });
        loadUserData();
        setSize('');
        setPrice('');
      }
    } catch (error) {
      console.error('Order submission error:', error);
      enqueueSnackbar(error.reason || 'Order failed', { variant: 'error' });
    } finally {
      setIsLoading(false);
    }
  };

  // Handle position close
  const handleClosePosition = async (positionId) => {
    setIsLoading(true);
    try {
      const tx = await derivativesContract.closePosition(positionId);
      enqueueSnackbar('Closing position...', { variant: 'info' });
      
      const receipt = await tx.wait();
      if (receipt.status === 1) {
        enqueueSnackbar('Position closed successfully', { variant: 'success' });
        loadUserData();
      }
    } catch (error) {
      console.error('Position close error:', error);
      enqueueSnackbar(error.reason || 'Failed to close position', { variant: 'error' });
    } finally {
      setIsLoading(false);
    }
  };

  // Risk indicators
  const getRiskIndicator = (healthFactor) => {
    if (healthFactor > 2) return { color: 'success', text: 'Healthy' };
    if (healthFactor > 1.5) return { color: 'warning', text: 'Monitor' };
    if (healthFactor > 1.2) return { color: 'error', text: 'At Risk' };
    return { color: 'error', text: 'Critical' };
  };

  return (
    <Box sx={{ flexGrow: 1, p: 3 }}>
      {/* Header */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12}>
          <Paper sx={{ p: 2, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box display="flex" alignItems="center" gap={2}>
              <Typography variant="h4">Derivatives Trading</Typography>
              {isConnected ? (
                <Chip label="Connected" color="success" size="small" />
              ) : (
                <Chip label="Connecting..." color="warning" size="small" />
              )}
            </Box>
            <Box display="flex" gap={2}>
              <MarketSelector 
                selected={selectedMarket}
                onChange={setSelectedMarket}
                markets={markets}
              />
              <IconButton onClick={() => setDialogOpen(true)}>
                <Settings />
              </IconButton>
            </Box>
          </Paper>
        </Grid>
      </Grid>

      {/* Account Overview */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total Equity
              </Typography>
              <Typography variant="h5">
                {formatUSD(accountMetrics.totalEquity)}
              </Typography>
              <Box display="flex" alignItems="center" mt={1}>
                {accountMetrics.unrealizedPnL >= 0 ? (
                  <TrendingUp color="success" />
                ) : (
                  <TrendingDown color="error" />
                )}
                <Typography 
                  variant="body2" 
                  color={accountMetrics.unrealizedPnL >= 0 ? 'success.main' : 'error.main'}
                  ml={1}
                >
                  {formatUSD(Math.abs(accountMetrics.unrealizedPnL))}
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Free Collateral
              </Typography>
              <Typography variant="h5">
                {formatUSD(accountMetrics.freeCollateral)}
              </Typography>
              <LinearProgress 
                variant="determinate" 
                value={accountMetrics.marginRatio} 
                sx={{ mt: 2 }}
                color={accountMetrics.marginRatio > 80 ? 'error' : 'primary'}
              />
              <Typography variant="body2" color="textSecondary" mt={1}>
                {accountMetrics.marginRatio.toFixed(1)}% Margin Used
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                24h Volume
              </Typography>
              <Typography variant="h5">
                {formatUSD(volume24h[selectedMarket] || 0)}
              </Typography>
              <Typography variant="body2" color="textSecondary" mt={1}>
                Open Interest: {formatUSD(openInterest[selectedMarket] || 0)}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography color="textSecondary" gutterBottom>
                  Funding Rate
                </Typography>
                <Timer fontSize="small" color="action" />
              </Box>
              <Typography variant="h5">
                {fundingRate ? `${(fundingRate * 100).toFixed(4)}%` : '0.00%'}
              </Typography>
              <Typography variant="body2" color="textSecondary" mt={1}>
                Next in {marketData?.nextFundingTime || '00:00'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Main Trading Interface */}
      <Grid container spacing={2}>
        {/* Chart and Order Book */}
        <Grid item xs={12} lg={8}>
          <Paper sx={{ height: 600, position: 'relative' }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
              <Tabs value={activeTab} onChange={(e, v) => setActiveTab(v)}>
                <Tab label="Chart" />
                <Tab label="Depth" />
                <Tab label="Trades" />
                <Tab label="Liquidations" />
              </Tabs>
            </Box>
            
            {activeTab === 0 && (
              <TradingChart 
                market={selectedMarket}
                data={marketData}
                positions={positions}
                orders={orders}
              />
            )}
            
            {activeTab === 1 && (
              <OrderBook 
                data={orderBookData}
                currentPrice={prices[selectedMarket]}
                onPriceClick={setPrice}
              />
            )}
            
            {activeTab === 2 && (
              <RecentTrades market={selectedMarket} />
            )}
            
            {activeTab === 3 && (
              <LiquidationFeed 
                liquidations={liquidations}
                currentPrice={prices[selectedMarket]}
              />
            )}
          </Paper>
        </Grid>

        {/* Trading Panel */}
        <Grid item xs={12} lg={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Place Order
            </Typography>
            
            {/* Order Type Selector */}
            <Box sx={{ mb: 2 }}>
              <Button
                variant={orderType === 'market' ? 'contained' : 'outlined'}
                onClick={() => setOrderType('market')}
                sx={{ mr: 1 }}
              >
                Market
              </Button>
              <Button
                variant={orderType === 'limit' ? 'contained' : 'outlined'}
                onClick={() => setOrderType('limit')}
              >
                Limit
              </Button>
            </Box>

            {/* Side Selector */}
            <Box sx={{ mb: 2, display: 'flex', gap: 1 }}>
              <Button
                fullWidth
                variant={side === 'long' ? 'contained' : 'outlined'}
                color="success"
                onClick={() => setSide('long')}
                startIcon={<TrendingUp />}
              >
                Long
              </Button>
              <Button
                fullWidth
                variant={side === 'short' ? 'contained' : 'outlined'}
                color="error"
                onClick={() => setSide('short')}
                startIcon={<TrendingDown />}
              >
                Short
              </Button>
            </Box>

            {/* Price Input (for limit orders) */}
            {orderType === 'limit' && (
              <TextField
                fullWidth
                label="Price"
                value={price}
                onChange={(e) => setPrice(e.target.value)}
                type="number"
                sx={{ mb: 2 }}
                InputProps={{
                  endAdornment: <Typography variant="body2">USD</Typography>
                }}
              />
            )}

            {/* Size Input */}
            <TextField
              fullWidth
              label="Size"
              value={size}
              onChange={(e) => setSize(e.target.value)}
              type="number"
              sx={{ mb: 2 }}
              helperText={`≈ ${formatUSD(parseFloat(size || 0) * (prices[selectedMarket] || 0))}`}
            />

            {/* Leverage Slider */}
            <Box sx={{ mb: 2 }}>
              <Typography gutterBottom>
                Leverage: {leverage}x
              </Typography>
              <Slider
                value={leverage}
                onChange={(e, v) => setLeverage(v)}
                min={1}
                max={MARKET_CONFIGS[selectedMarket]?.maxLeverage || 100}
                marks={[
                  { value: 1, label: '1x' },
                  { value: 10, label: '10x' },
                  { value: 25, label: '25x' },
                  { value: 50, label: '50x' },
                  { value: 100, label: '100x' }
                ]}
              />
            </Box>

            {/* Order Options */}
            <Box sx={{ mb: 2 }}>
              <FormControlLabel
                control={
                  <Switch
                    checked={reduceOnly}
                    onChange={(e) => setReduceOnly(e.target.checked)}
                  />
                }
                label="Reduce Only"
              />
              {orderType === 'limit' && (
                <FormControlLabel
                  control={
                    <Switch
                      checked={postOnly}
                      onChange={(e) => setPostOnly(e.target.checked)}
                    />
                  }
                  label="Post Only"
                />
              )}
            </Box>

            {/* Order Summary */}
            <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
              <Typography variant="body2" color="textSecondary">
                Required Margin
              </Typography>
              <Typography variant="h6">
                {formatUSD((parseFloat(size || 0) * (prices[selectedMarket] || 0)) / leverage)}
              </Typography>
              
              <Divider sx={{ my: 1 }} />
              
              <Box display="flex" justifyContent="space-between">
                <Typography variant="body2" color="textSecondary">
                  Fee ({side === 'long' ? 'Taker' : 'Maker'})
                </Typography>
                <Typography variant="body2">
                  {formatUSD(
                    parseFloat(size || 0) * 
                    (prices[selectedMarket] || 0) * 
                    (side === 'long' ? 0.0005 : -0.0002)
                  )}
                </Typography>
              </Box>
              
              <Box display="flex" justifyContent="space-between" mt={1}>
                <Typography variant="body2" color="textSecondary">
                  Liquidation Price
                </Typography>
                <Typography variant="body2" color="error">
                  {calculateLiquidationPrice(
                    prices[selectedMarket] || 0,
                    side === 'long',
                    leverage
                  ).toFixed(2)}
                </Typography>
              </Box>
            </Paper>

            {/* Submit Button */}
            <Button
              fullWidth
              variant="contained"
              color={side === 'long' ? 'success' : 'error'}
              onClick={handleSubmitOrder}
              disabled={isLoading || !account}
              size="large"
            >
              {isLoading ? (
                <CircularProgress size={24} />
              ) : (
                `${side === 'long' ? 'Long' : 'Short'} ${selectedMarket}`
              )}
            </Button>

            {!account && (
              <Alert severity="warning" sx={{ mt: 2 }}>
                Please connect your wallet to trade
              </Alert>
            )}
          </Paper>

          {/* Collateral Management */}
          <Paper sx={{ p: 2, mt: 2 }}>
            <CollateralManager 
              balances={collateralBalances}
              onDeposit={loadUserData}
              onWithdraw={loadUserData}
            />
          </Paper>
        </Grid>
      </Grid>

      {/* Positions and Orders */}
      <Grid container spacing={2} sx={{ mt: 2 }}>
        <Grid item xs={12}>
          <PositionManager
            positions={positions}
            orders={orders}
            prices={prices}
            onClosePosition={handleClosePosition}
            onCancelOrder={(orderId) => console.log('Cancel order:', orderId)}
            onEditPosition={(position) => {
              setSelectedPosition(position);
              setDialogOpen(true);
            }}
          />
        </Grid>
      </Grid>

      {/* Risk Metrics */}
      <Grid container spacing={2} sx={{ mt: 2 }}>
        <Grid item xs={12} md={6}>
          <RiskMetrics 
            positions={positions}
            prices={prices}
            volatility={volatility}
          />
        </Grid>
        <Grid item xs={12} md={6}>
          <LeaderBoard market={selectedMarket} />
        </Grid>
      </Grid>

      {/* Settings Dialog */}
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Trading Settings</DialogTitle>
        <DialogContent>
          {/* Add settings content here */}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

// Helper Components
const RecentTrades = ({ market }) => {
  // Implementation for recent trades display
  return <Box p={2}>Recent trades for {market}</Box>;
};

const LiquidationFeed = ({ liquidations, currentPrice }) => {
  // Implementation for liquidation feed
  return (
    <Box p={2}>
      {liquidations.map((liq, i) => (
        <Box key={i} mb={1}>
          <Typography variant="body2">
            Liquidation: {liq.size} @ {liq.price}
          </Typography>
        </Box>
      ))}
    </Box>
  );
};

// Utility functions
const formatPosition = (position) => {
  // Format position data from contract
  return {
    id: position.id,
    market: position.market,
    side: position.isLong ? 'long' : 'short',
    size: parseFloat(ethers.utils.formatUnits(position.size, 18)),
    entryPrice: parseFloat(ethers.utils.formatUnits(position.entryPrice, 8)),
    collateral: parseFloat(ethers.utils.formatUnits(position.collateral, 18)),
    leverage: position.leverage.toNumber(),
    unrealizedPnl: 0,
    healthFactor: 1.5
  };
};

const formatOrder = (order) => {
  // Format order data from contract
  return {
    id: order.id,
    market: order.market,
    side: order.isLong ? 'long' : 'short',
    type: order.isMarket ? 'market' : 'limit',
    size: parseFloat(ethers.utils.formatUnits(order.size, 18)),
    price: parseFloat(ethers.utils.formatUnits(order.price, 8)),
    filled: 0,
    status: 'open'
  };
};

const formatMarketStats = (stats) => {
  return {
    openInterestLong: parseFloat(ethers.utils.formatUnits(stats.openInterestLong, 18)),
    openInterestShort: parseFloat(ethers.utils.formatUnits(stats.openInterestShort, 18)),
    volume24h: parseFloat(ethers.utils.formatUnits(stats.volume24h, 18)),
    fundingRate: parseFloat(ethers.utils.formatUnits(stats.fundingRate, 18))
  };
};

const calculateLiquidationPrice = (entryPrice, isLong, leverage) => {
  const maintenanceMargin = 0.005; // 0.5%
  if (isLong) {
    return entryPrice * (1 - (1 / leverage) + maintenanceMargin);
  } else {
    return entryPrice * (1 + (1 / leverage) - maintenanceMargin);
  }
};

export default DerivativesDashboard; 