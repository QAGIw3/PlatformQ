"""
Cross-Market Correlation Analysis

Analyzes correlations and dependencies across multiple markets to detect
systemic risks, contagion patterns, and arbitrage opportunities.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import networkx as nx
from scipy.stats import pearsonr, spearmanr
from scipy.spatial.distance import correlation as correlation_distance
from sklearn.covariance import LedoitWolf, GraphicalLassoCV
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
import asyncio
from pyignite import Client as IgniteClient
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


@dataclass
class CorrelationResult:
    """Result of correlation analysis between markets."""
    market_pair: Tuple[str, str]
    pearson_correlation: float
    spearman_correlation: float
    rolling_correlation: List[float]
    correlation_stability: float
    is_significant: bool
    p_value: float
    
    
@dataclass
class ContagionRisk:
    """Contagion risk assessment."""
    source_market: str
    affected_markets: List[str]
    contagion_probability: float
    expected_impact: Dict[str, float]
    propagation_path: List[str]
    time_to_impact: Dict[str, float]  # hours
    

@dataclass
class SystemicRiskMetrics:
    """System-wide risk metrics."""
    systemic_risk_score: float
    market_concentration: float
    correlation_clustering: float
    eigenvector_centrality: Dict[str, float]
    vulnerable_markets: List[str]
    risk_factors: Dict[str, float]
    

class CrossMarketCorrelationAnalyzer:
    """
    Analyzes correlations across markets to detect systemic risks.
    
    Features:
    - Dynamic correlation tracking
    - Contagion risk modeling
    - Network-based systemic risk analysis
    - Regime change detection
    - Cross-market arbitrage detection
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite = ignite_client
        
        # Configuration
        self.min_correlation_threshold = 0.3
        self.contagion_threshold = 0.7
        self.lookback_windows = [24, 168, 720]  # 1d, 1w, 1m in hours
        
        # Caches
        self.correlation_cache = "cross_market_correlations"
        self.market_data_cache = "market_time_series"
        self.risk_network_cache = "market_risk_network"
        
        # Analysis components
        self.correlation_matrix = None
        self.risk_network = None
        self.pca_model = None
        
        # Thread pool for parallel computation
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    async def analyze_correlations(self, 
                                 markets: List[str],
                                 time_window: int = 168) -> Dict[str, CorrelationResult]:
        """
        Analyze correlations between all market pairs.
        
        Args:
            markets: List of market IDs
            time_window: Hours of historical data to analyze
            
        Returns:
            Dictionary of correlation results
        """
        # Fetch market data
        market_data = await self._fetch_market_data(markets, time_window)
        
        if len(market_data) < 2:
            logger.warning("Insufficient markets for correlation analysis")
            return {}
            
        # Prepare return series
        returns_data = self._calculate_returns(market_data)
        
        # Analyze all pairs
        correlation_results = {}
        
        for i in range(len(markets)):
            for j in range(i + 1, len(markets)):
                market1, market2 = markets[i], markets[j]
                
                result = await self._analyze_pair_correlation(
                    market1, market2, returns_data
                )
                
                correlation_results[f"{market1}_{market2}"] = result
                
        # Update correlation matrix
        self._update_correlation_matrix(markets, correlation_results)
        
        # Store results
        await self._store_correlations(correlation_results)
        
        return correlation_results
        
    async def _analyze_pair_correlation(self,
                                      market1: str,
                                      market2: str,
                                      returns_data: pd.DataFrame) -> CorrelationResult:
        """Analyze correlation between two markets."""
        returns1 = returns_data[market1].dropna()
        returns2 = returns_data[market2].dropna()
        
        # Align series
        aligned = pd.DataFrame({
            market1: returns1,
            market2: returns2
        }).dropna()
        
        if len(aligned) < 30:  # Minimum samples
            return CorrelationResult(
                market_pair=(market1, market2),
                pearson_correlation=0,
                spearman_correlation=0,
                rolling_correlation=[],
                correlation_stability=0,
                is_significant=False,
                p_value=1.0
            )
            
        # Calculate correlations
        pearson_corr, pearson_p = pearsonr(aligned[market1], aligned[market2])
        spearman_corr, spearman_p = spearmanr(aligned[market1], aligned[market2])
        
        # Rolling correlation
        window = min(24, len(aligned) // 4)
        rolling_corr = aligned[market1].rolling(window).corr(aligned[market2]).dropna()
        
        # Correlation stability (low std = stable)
        correlation_stability = 1 - rolling_corr.std() if len(rolling_corr) > 0 else 0
        
        # Significance test
        is_significant = pearson_p < 0.05 and abs(pearson_corr) > self.min_correlation_threshold
        
        return CorrelationResult(
            market_pair=(market1, market2),
            pearson_correlation=float(pearson_corr),
            spearman_correlation=float(spearman_corr),
            rolling_correlation=rolling_corr.tolist(),
            correlation_stability=float(correlation_stability),
            is_significant=is_significant,
            p_value=float(pearson_p)
        )
        
    async def detect_contagion_risk(self,
                                  shock_market: str,
                                  shock_magnitude: float) -> List[ContagionRisk]:
        """
        Detect contagion risk from a shock in one market.
        
        Args:
            shock_market: Market experiencing shock
            shock_magnitude: Size of shock (% change)
            
        Returns:
            List of contagion risks
        """
        if self.correlation_matrix is None:
            logger.warning("Correlation matrix not initialized")
            return []
            
        contagion_risks = []
        
        # Get correlations with shocked market
        if shock_market not in self.correlation_matrix.index:
            return []
            
        correlations = self.correlation_matrix.loc[shock_market]
        
        # Build contagion network
        contagion_graph = nx.DiGraph()
        
        for market, correlation in correlations.items():
            if market == shock_market:
                continue
                
            # Contagion probability based on correlation
            contagion_prob = abs(correlation) * (1 - np.exp(-abs(shock_magnitude)))
            
            if contagion_prob > self.contagion_threshold:
                contagion_graph.add_edge(
                    shock_market, market,
                    weight=contagion_prob,
                    impact=shock_magnitude * correlation
                )
                
        # Analyze propagation paths
        for market in contagion_graph.nodes():
            if market == shock_market:
                continue
                
            # Find all paths from shock source
            paths = list(nx.all_simple_paths(
                contagion_graph, shock_market, market, cutoff=3
            ))
            
            if paths:
                # Calculate cumulative impact
                max_impact_path = max(paths, key=lambda p: self._calculate_path_impact(
                    contagion_graph, p
                ))
                
                impact = self._calculate_path_impact(contagion_graph, max_impact_path)
                
                # Time to impact (based on path length and correlations)
                time_to_impact = len(max_impact_path) * 2.0  # hours
                
                # Get affected markets in cascade
                affected = list(nx.descendants(contagion_graph, market))
                
                contagion_risks.append(ContagionRisk(
                    source_market=shock_market,
                    affected_markets=[market] + affected,
                    contagion_probability=contagion_graph[shock_market][market]['weight'],
                    expected_impact={market: impact},
                    propagation_path=max_impact_path,
                    time_to_impact={market: time_to_impact}
                ))
                
        return contagion_risks
        
    async def calculate_systemic_risk(self, 
                                    markets: List[str]) -> SystemicRiskMetrics:
        """
        Calculate overall systemic risk metrics.
        
        Args:
            markets: List of markets to analyze
            
        Returns:
            Systemic risk metrics
        """
        # Ensure correlation matrix is up to date
        await self.analyze_correlations(markets)
        
        # Build risk network
        self.risk_network = self._build_risk_network(markets)
        
        # Calculate network metrics
        
        # 1. Eigenvector centrality (systemic importance)
        try:
            centrality = nx.eigenvector_centrality(
                self.risk_network, max_iter=1000
            )
        except:
            centrality = nx.degree_centrality(self.risk_network)
            
        # 2. Clustering coefficient (interconnectedness)
        clustering = nx.average_clustering(self.risk_network, weight='weight')
        
        # 3. Market concentration (HHI)
        market_sizes = {m: self.risk_network.nodes[m].get('size', 1) 
                       for m in markets if m in self.risk_network}
        total_size = sum(market_sizes.values())
        
        if total_size > 0:
            market_shares = {m: s/total_size for m, s in market_sizes.items()}
            hhi = sum(share**2 for share in market_shares.values())
        else:
            hhi = 0
            
        # 4. Systemic risk score (composite)
        systemic_risk_score = self._calculate_systemic_risk_score(
            clustering, hhi, centrality
        )
        
        # 5. Identify vulnerable markets
        vulnerable_markets = self._identify_vulnerable_markets(
            centrality, self.correlation_matrix
        )
        
        # 6. Risk factor decomposition
        risk_factors = {
            'correlation_risk': clustering,
            'concentration_risk': hhi,
            'contagion_risk': np.mean(list(centrality.values())),
            'volatility_risk': self._calculate_volatility_risk(markets),
            'liquidity_risk': self._calculate_liquidity_risk(markets)
        }
        
        return SystemicRiskMetrics(
            systemic_risk_score=systemic_risk_score,
            market_concentration=hhi,
            correlation_clustering=clustering,
            eigenvector_centrality=centrality,
            vulnerable_markets=vulnerable_markets,
            risk_factors=risk_factors
        )
        
    async def detect_regime_changes(self, 
                                  markets: List[str]) -> Dict[str, Any]:
        """
        Detect regime changes in correlation structure.
        
        Returns:
            Regime change detection results
        """
        # Get historical correlations
        historical_corr = await self._get_historical_correlations(markets)
        
        if len(historical_corr) < 2:
            return {}
            
        regime_changes = {}
        
        # Analyze correlation stability over time
        for window in self.lookback_windows:
            window_data = historical_corr[-window:] if len(historical_corr) > window else historical_corr
            
            # Detect structural breaks
            breaks = self._detect_correlation_breaks(window_data)
            
            if breaks:
                regime_changes[f"{window}h"] = {
                    'break_points': breaks,
                    'current_regime': self._identify_regime(window_data[-1]),
                    'regime_duration': self._calculate_regime_duration(window_data, breaks)
                }
                
        # PCA analysis for dimension reduction
        if self.correlation_matrix is not None:
            pca_results = self._perform_pca_analysis(self.correlation_matrix)
            regime_changes['pca_analysis'] = pca_results
            
        return regime_changes
        
    async def find_arbitrage_opportunities(self,
                                         markets: List[str]) -> List[Dict[str, Any]]:
        """
        Find potential arbitrage opportunities based on correlation breaks.
        
        Returns:
            List of arbitrage opportunities
        """
        opportunities = []
        
        # Get recent correlations
        correlations = await self.analyze_correlations(markets, time_window=24)
        
        for pair_key, corr_result in correlations.items():
            if not corr_result.is_significant:
                continue
                
            market1, market2 = corr_result.market_pair
            
            # Check for correlation deviation
            if len(corr_result.rolling_correlation) > 0:
                current_corr = corr_result.rolling_correlation[-1]
                mean_corr = np.mean(corr_result.rolling_correlation)
                std_corr = np.std(corr_result.rolling_correlation)
                
                # Significant deviation from mean
                if abs(current_corr - mean_corr) > 2 * std_corr:
                    # Get current spreads
                    spread = await self._calculate_pair_spread(market1, market2)
                    
                    opportunities.append({
                        'type': 'correlation_arbitrage',
                        'markets': [market1, market2],
                        'current_correlation': current_corr,
                        'expected_correlation': mean_corr,
                        'correlation_zscore': (current_corr - mean_corr) / std_corr,
                        'current_spread': spread,
                        'confidence': corr_result.correlation_stability,
                        'suggested_action': 'convergence_trade' if abs(current_corr) < abs(mean_corr) else 'divergence_trade'
                    })
                    
        # Triangular arbitrage detection
        triangular_opps = await self._detect_triangular_arbitrage(markets)
        opportunities.extend(triangular_opps)
        
        return opportunities
        
    def _build_risk_network(self, markets: List[str]) -> nx.Graph:
        """Build network graph of market relationships."""
        G = nx.Graph()
        
        # Add nodes
        for market in markets:
            G.add_node(market)
            
        # Add edges based on correlations
        if self.correlation_matrix is not None:
            for i in range(len(markets)):
                for j in range(i + 1, len(markets)):
                    market1, market2 = markets[i], markets[j]
                    
                    if market1 in self.correlation_matrix.index and \
                       market2 in self.correlation_matrix.columns:
                        correlation = self.correlation_matrix.loc[market1, market2]
                        
                        if abs(correlation) > self.min_correlation_threshold:
                            G.add_edge(
                                market1, market2,
                                weight=abs(correlation),
                                correlation=correlation
                            )
                            
        return G
        
    def _calculate_systemic_risk_score(self,
                                     clustering: float,
                                     concentration: float,
                                     centrality: Dict[str, float]) -> float:
        """Calculate composite systemic risk score."""
        # Normalize components
        clustering_risk = clustering  # Already 0-1
        concentration_risk = concentration  # HHI is 0-1
        centrality_risk = np.mean(list(centrality.values())) if centrality else 0
        
        # Weighted average
        weights = {
            'clustering': 0.3,
            'concentration': 0.3,
            'centrality': 0.4
        }
        
        systemic_risk = (
            weights['clustering'] * clustering_risk +
            weights['concentration'] * concentration_risk +
            weights['centrality'] * centrality_risk
        )
        
        return float(systemic_risk)
        
    def _identify_vulnerable_markets(self,
                                   centrality: Dict[str, float],
                                   correlation_matrix: pd.DataFrame) -> List[str]:
        """Identify markets most vulnerable to contagion."""
        vulnerable = []
        
        # High centrality = high systemic importance
        high_centrality = sorted(centrality.items(), 
                               key=lambda x: x[1], 
                               reverse=True)[:5]
        
        for market, cent_score in high_centrality:
            # Check correlation exposure
            if market in correlation_matrix.index:
                avg_correlation = correlation_matrix.loc[market].abs().mean()
                
                if avg_correlation > 0.5 or cent_score > 0.7:
                    vulnerable.append(market)
                    
        return vulnerable
        
    def _calculate_path_impact(self, graph: nx.DiGraph, path: List[str]) -> float:
        """Calculate cumulative impact along a contagion path."""
        if len(path) < 2:
            return 0
            
        impact = 1.0
        for i in range(len(path) - 1):
            if graph.has_edge(path[i], path[i+1]):
                impact *= graph[path[i]][path[i+1]].get('impact', 0)
                
        return impact
        
    def _detect_correlation_breaks(self, 
                                 correlation_history: List[pd.DataFrame]) -> List[int]:
        """Detect structural breaks in correlation patterns."""
        if len(correlation_history) < 10:
            return []
            
        # Convert to matrix time series
        n_markets = len(correlation_history[0])
        corr_series = np.array([
            corr_df.values.flatten() for corr_df in correlation_history
        ])
        
        # Simple change point detection using variance
        breaks = []
        window = 5
        
        for i in range(window, len(corr_series) - window):
            before_var = np.var(corr_series[i-window:i])
            after_var = np.var(corr_series[i:i+window])
            
            # Significant variance change
            if abs(before_var - after_var) / (before_var + 1e-6) > 0.5:
                breaks.append(i)
                
        return breaks
        
    def _identify_regime(self, correlation_matrix: pd.DataFrame) -> str:
        """Identify current market regime based on correlation structure."""
        avg_correlation = correlation_matrix.abs().mean().mean()
        
        if avg_correlation > 0.7:
            return "high_correlation"
        elif avg_correlation > 0.4:
            return "moderate_correlation"
        else:
            return "low_correlation"
            
    def _perform_pca_analysis(self, correlation_matrix: pd.DataFrame) -> Dict[str, Any]:
        """Perform PCA on correlation matrix."""
        pca = PCA(n_components=min(5, len(correlation_matrix)))
        pca.fit(correlation_matrix)
        
        return {
            'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
            'n_components_90pct': np.argmax(
                np.cumsum(pca.explained_variance_ratio_) > 0.9
            ) + 1,
            'first_component_markets': dict(zip(
                correlation_matrix.columns,
                pca.components_[0]
            ))
        }
        
    async def _fetch_market_data(self,
                               markets: List[str],
                               hours: int) -> Dict[str, pd.DataFrame]:
        """Fetch historical market data."""
        market_data = {}
        
        cache = self.ignite.get_cache(self.market_data_cache)
        
        for market in markets:
            # Fetch from cache or external source
            data = cache.get(f"{market}_history_{hours}h")
            
            if data is not None:
                market_data[market] = pd.DataFrame(data)
                
        return market_data
        
    def _calculate_returns(self, market_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Calculate returns for all markets."""
        returns_dict = {}
        
        for market, data in market_data.items():
            if 'price' in data.columns:
                returns_dict[market] = data['price'].pct_change().dropna()
                
        return pd.DataFrame(returns_dict)
        
    def _update_correlation_matrix(self,
                                 markets: List[str],
                                 correlations: Dict[str, CorrelationResult]):
        """Update internal correlation matrix."""
        n = len(markets)
        matrix = pd.DataFrame(
            np.eye(n),
            index=markets,
            columns=markets
        )
        
        for corr_key, result in correlations.items():
            market1, market2 = result.market_pair
            corr_value = result.pearson_correlation
            
            if market1 in matrix.index and market2 in matrix.columns:
                matrix.loc[market1, market2] = corr_value
                matrix.loc[market2, market1] = corr_value
                
        self.correlation_matrix = matrix
        
    async def _store_correlations(self, correlations: Dict[str, CorrelationResult]):
        """Store correlation results in cache."""
        cache = self.ignite.get_cache(self.correlation_cache)
        
        for key, result in correlations.items():
            cache.put(key, result.__dict__)
            
    def _calculate_volatility_risk(self, markets: List[str]) -> float:
        """Calculate aggregate volatility risk."""
        # Placeholder - would calculate from market data
        return 0.5
        
    def _calculate_liquidity_risk(self, markets: List[str]) -> float:
        """Calculate aggregate liquidity risk."""
        # Placeholder - would calculate from market data
        return 0.3
        
    async def _detect_triangular_arbitrage(self,
                                         markets: List[str]) -> List[Dict[str, Any]]:
        """Detect triangular arbitrage opportunities."""
        # Placeholder for triangular arbitrage detection
        return []
        
    async def _calculate_pair_spread(self,
                                   market1: str,
                                   market2: str) -> float:
        """Calculate current spread between market pair."""
        # Placeholder - would fetch current prices and calculate spread
        return 0.01 