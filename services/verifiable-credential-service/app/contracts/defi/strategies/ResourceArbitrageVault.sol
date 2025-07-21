// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "../InfrastructureVault.sol";
import "../ResourceAMM.sol";
import "../FlashResourceProvider.sol";

/**
 * @title ResourceArbitrageVault
 * @notice Strategy that exploits price differences across regions and tiers
 * @dev Implements IStrategy interface for InfrastructureVault
 */
contract ResourceArbitrageVault is Ownable, ReentrancyGuard {
    
    // Constants
    uint256 public constant MAX_BPS = 10000;
    uint256 public constant MIN_PROFIT_BPS = 50; // 0.5% minimum profit
    
    // Immutable addresses
    address public immutable vault;
    address public immutable want; // ResourceToken address
    uint256 public immutable wantTokenId; // Resource token ID
    ResourceAMM public immutable resourceAMM;
    FlashResourceProvider public immutable flashProvider;
    
    // Strategy configuration
    struct ArbitrageRoute {
        uint256 poolIdA;      // First pool
        uint256 poolIdB;      // Second pool
        bool aToB;            // Direction: true = A->B->A, false = B->A->B
        uint256 minProfit;    // Minimum profit in basis points
        uint256 maxAmount;    // Maximum amount per arbitrage
        bool isActive;        // Whether route is active
    }
    
    // State variables
    mapping(uint256 => ArbitrageRoute) public routes;
    uint256 public nextRouteId = 1;
    uint256 public maxSlippage = 100; // 1% max slippage
    uint256 public performanceFee = 1000; // 10% of profits
    address public strategist;
    
    // Tracking
    uint256 public totalProfits;
    uint256 public totalArbitrages;
    
    // Events
    event RouteAdded(uint256 indexed routeId, uint256 poolIdA, uint256 poolIdB);
    event RouteUpdated(uint256 indexed routeId, bool isActive);
    event ArbitrageExecuted(
        uint256 indexed routeId,
        uint256 inputAmount,
        uint256 profit,
        uint256 timestamp
    );
    event StrategistUpdated(address newStrategist);
    event Harvested(uint256 profit, uint256 loss, uint256 debtPayment);
    
    modifier onlyVault() {
        require(msg.sender == vault, "Only vault");
        _;
    }
    
    modifier onlyAuthorized() {
        require(msg.sender == vault || msg.sender == strategist || msg.sender == owner(), "Not authorized");
        _;
    }
    
    /**
     * @dev Constructor
     * @param _vault Vault address
     * @param _want ResourceToken address
     * @param _wantTokenId Resource token ID
     * @param _resourceAMM ResourceAMM address
     * @param _flashProvider FlashResourceProvider address
     */
    constructor(
        address _vault,
        address _want,
        uint256 _wantTokenId,
        address _resourceAMM,
        address _flashProvider
    ) {
        require(_vault != address(0), "Invalid vault");
        require(_want != address(0), "Invalid want");
        
        vault = _vault;
        want = _want;
        wantTokenId = _wantTokenId;
        resourceAMM = ResourceAMM(_resourceAMM);
        flashProvider = FlashResourceProvider(_flashProvider);
        strategist = msg.sender;
    }
    
    /**
     * @notice Get the name of the strategy
     */
    function name() external pure returns (string memory) {
        return "ResourceArbitrageVault";
    }
    
    /**
     * @notice Get estimated total assets under management
     */
    function estimatedTotalAssets() external view returns (uint256) {
        return IERC1155(want).balanceOf(address(this), wantTokenId);
    }
    
    /**
     * @notice Add a new arbitrage route
     * @param poolIdA First pool ID
     * @param poolIdB Second pool ID
     * @param minProfit Minimum profit in basis points
     * @param maxAmount Maximum amount per arbitrage
     */
    function addRoute(
        uint256 poolIdA,
        uint256 poolIdB,
        uint256 minProfit,
        uint256 maxAmount
    ) external onlyAuthorized returns (uint256) {
        require(poolIdA != poolIdB, "Same pool");
        require(minProfit >= MIN_PROFIT_BPS, "Profit too low");
        require(maxAmount > 0, "Invalid amount");
        
        uint256 routeId = nextRouteId++;
        
        routes[routeId] = ArbitrageRoute({
            poolIdA: poolIdA,
            poolIdB: poolIdB,
            aToB: true,
            minProfit: minProfit,
            maxAmount: maxAmount,
            isActive: true
        });
        
        emit RouteAdded(routeId, poolIdA, poolIdB);
        return routeId;
    }
    
    /**
     * @notice Update route status
     * @param routeId Route ID
     * @param isActive New status
     */
    function updateRoute(uint256 routeId, bool isActive) external onlyAuthorized {
        routes[routeId].isActive = isActive;
        emit RouteUpdated(routeId, isActive);
    }
    
    /**
     * @notice Execute arbitrage on a specific route
     * @param routeId Route to arbitrage
     */
    function executeArbitrage(uint256 routeId) external onlyAuthorized {
        ArbitrageRoute memory route = routes[routeId];
        require(route.isActive, "Route not active");
        
        // Calculate optimal arbitrage amount
        (uint256 amount, uint256 expectedProfit) = _calculateOptimalArbitrage(route);
        
        if (amount == 0 || expectedProfit < route.minProfit) {
            return; // No profitable arbitrage
        }
        
        // Limit amount
        amount = amount > route.maxAmount ? route.maxAmount : amount;
        
        // Execute flash loan for arbitrage
        bytes memory data = abi.encode(routeId, route);
        flashProvider.flashLoan(
            IFlashResourceReceiver(address(this)),
            wantTokenId,
            amount,
            data
        );
    }
    
    /**
     * @notice Flash loan callback
     */
    function onFlashLoan(
        address initiator,
        uint256 tokenId,
        uint256 amount,
        uint256 fee,
        bytes calldata data
    ) external returns (bytes32) {
        require(msg.sender == address(flashProvider), "Invalid flash loan");
        require(initiator == address(this), "Invalid initiator");
        require(tokenId == wantTokenId, "Invalid token");
        
        (uint256 routeId, ArbitrageRoute memory route) = abi.decode(data, (uint256, ArbitrageRoute));
        
        // Execute arbitrage
        uint256 balanceBefore = IERC1155(want).balanceOf(address(this), wantTokenId);
        
        if (route.aToB) {
            // Swap A -> B
            _swap(route.poolIdA, amount);
            uint256 intermediateBalance = IERC1155(want).balanceOf(address(this), wantTokenId);
            
            // Swap B -> A
            _swap(route.poolIdB, intermediateBalance);
        } else {
            // Swap B -> A
            _swap(route.poolIdB, amount);
            uint256 intermediateBalance = IERC1155(want).balanceOf(address(this), wantTokenId);
            
            // Swap A -> B
            _swap(route.poolIdA, intermediateBalance);
        }
        
        uint256 balanceAfter = IERC1155(want).balanceOf(address(this), wantTokenId);
        
        // Ensure we have profit after fees
        require(balanceAfter >= balanceBefore + amount + fee, "Unprofitable arbitrage");
        
        uint256 profit = balanceAfter - balanceBefore - amount - fee;
        totalProfits += profit;
        totalArbitrages++;
        
        emit ArbitrageExecuted(routeId, amount, profit, block.timestamp);
        
        // Approve flash loan repayment
        IERC1155(want).setApprovalForAll(address(flashProvider), true);
        
        return keccak256("ERC3156FlashBorrower.onFlashLoan");
    }
    
    /**
     * @notice Harvest profits and report to vault
     */
    function harvest() external onlyAuthorized {
        // Execute all profitable arbitrages
        for (uint256 i = 1; i < nextRouteId; i++) {
            if (routes[i].isActive) {
                try this.executeArbitrage(i) {
                    // Success
                } catch {
                    // Skip failed arbitrages
                }
            }
        }
        
        // Report to vault
        uint256 totalAssets = IERC1155(want).balanceOf(address(this), wantTokenId);
        uint256 debt = InfrastructureVault(vault).strategies(address(this)).totalDebt;
        
        uint256 profit = 0;
        uint256 loss = 0;
        
        if (totalAssets > debt) {
            profit = totalAssets - debt;
        } else if (totalAssets < debt) {
            loss = debt - totalAssets;
        }
        
        // Take strategist fee from profits
        if (profit > 0 && performanceFee > 0) {
            uint256 strategistProfit = profit * performanceFee / MAX_BPS;
            IERC1155(want).safeTransferFrom(
                address(this),
                strategist,
                wantTokenId,
                strategistProfit,
                ""
            );
            profit -= strategistProfit;
        }
        
        // Report to vault
        uint256 debtPayment = InfrastructureVault(vault).report(address(this));
        
        emit Harvested(profit, loss, debtPayment);
    }
    
    /**
     * @notice Withdraw assets from strategy
     * @param _amount Amount to withdraw
     * @return withdrawn Actual amount withdrawn
     */
    function withdraw(uint256 _amount) external onlyVault returns (uint256) {
        uint256 balance = IERC1155(want).balanceOf(address(this), wantTokenId);
        uint256 withdrawn = _amount > balance ? balance : _amount;
        
        IERC1155(want).safeTransferFrom(address(this), vault, wantTokenId, withdrawn, "");
        
        return withdrawn;
    }
    
    /**
     * @notice Migrate to a new strategy
     * @param _newStrategy New strategy address
     */
    function migrate(address _newStrategy) external onlyVault {
        require(_newStrategy != address(0), "Invalid strategy");
        uint256 balance = IERC1155(want).balanceOf(address(this), wantTokenId);
        
        if (balance > 0) {
            IERC1155(want).safeTransferFrom(address(this), _newStrategy, wantTokenId, balance, "");
        }
    }
    
    /**
     * @notice Update strategist address
     * @param _strategist New strategist
     */
    function setStrategist(address _strategist) external onlyAuthorized {
        require(_strategist != address(0), "Invalid strategist");
        strategist = _strategist;
        emit StrategistUpdated(_strategist);
    }
    
    /**
     * @notice Update performance fee
     * @param _performanceFee New fee in basis points
     */
    function setPerformanceFee(uint256 _performanceFee) external onlyAuthorized {
        require(_performanceFee <= 5000, "Fee too high"); // Max 50%
        performanceFee = _performanceFee;
    }
    
    /**
     * @notice Update max slippage
     * @param _maxSlippage New max slippage in basis points
     */
    function setMaxSlippage(uint256 _maxSlippage) external onlyAuthorized {
        require(_maxSlippage <= 1000, "Slippage too high"); // Max 10%
        maxSlippage = _maxSlippage;
    }
    
    // Internal functions
    
    function _calculateOptimalArbitrage(
        ArbitrageRoute memory route
    ) internal view returns (uint256 optimalAmount, uint256 expectedProfit) {
        // Get pool states
        (uint256 reserveA0, uint256 reserveA1, ) = resourceAMM.getReserves(route.poolIdA);
        (uint256 reserveB0, uint256 reserveB1, ) = resourceAMM.getReserves(route.poolIdB);
        
        // Calculate price impact and optimal amount
        // Simplified calculation - in production would use more sophisticated math
        uint256 priceA = reserveA1 * 1e18 / reserveA0;
        uint256 priceB = reserveB1 * 1e18 / reserveB0;
        
        if (priceA > priceB) {
            // Buy from B, sell to A
            uint256 priceDiff = priceA - priceB;
            optimalAmount = _calculateOptimalSwapAmount(reserveB0, reserveB1, priceDiff);
            expectedProfit = (optimalAmount * priceDiff) / 1e18;
        } else {
            // Buy from A, sell to B
            uint256 priceDiff = priceB - priceA;
            optimalAmount = _calculateOptimalSwapAmount(reserveA0, reserveA1, priceDiff);
            expectedProfit = (optimalAmount * priceDiff) / 1e18;
        }
        
        // Convert profit to basis points
        if (optimalAmount > 0) {
            expectedProfit = expectedProfit * MAX_BPS / optimalAmount;
        }
    }
    
    function _calculateOptimalSwapAmount(
        uint256 reserveIn,
        uint256 reserveOut,
        uint256 priceDiff
    ) internal pure returns (uint256) {
        // Simplified optimal amount calculation
        // In production, would solve for the amount that maximizes profit
        // considering price impact on both pools
        
        // For now, use a conservative approach
        uint256 maxImpact = reserveIn / 100; // Max 1% of pool
        uint256 profitableAmount = (priceDiff * reserveIn) / (1e18 * 2); // Rough estimate
        
        return profitableAmount > maxImpact ? maxImpact : profitableAmount;
    }
    
    function _swap(uint256 poolId, uint256 amountIn) internal {
        // Get expected output
        (uint256 amountOut, , ) = resourceAMM.getSwapOutput(poolId, amountIn);
        
        // Apply slippage protection
        uint256 minAmountOut = amountOut * (MAX_BPS - maxSlippage) / MAX_BPS;
        
        // Approve AMM
        IERC1155(want).setApprovalForAll(address(resourceAMM), true);
        
        // Execute swap
        resourceAMM.swap(poolId, amountIn, minAmountOut, address(this));
    }
    
    // ERC1155 Receiver
    function onERC1155Received(
        address,
        address,
        uint256,
        uint256,
        bytes memory
    ) public virtual returns (bytes4) {
        return this.onERC1155Received.selector;
    }
}

// Interface for flash loan receiver
interface IFlashResourceReceiver {
    function onFlashLoan(
        address initiator,
        uint256 tokenId,
        uint256 amount,
        uint256 fee,
        bytes calldata data
    ) external returns (bytes32);
} 