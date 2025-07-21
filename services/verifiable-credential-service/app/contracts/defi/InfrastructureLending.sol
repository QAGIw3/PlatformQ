// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "./ResourceToken.sol";
import "./NFTLending.sol";

/**
 * @title InfrastructureLending
 * @notice Lending protocol that accepts infrastructure resource tokens as collateral
 * @dev Extends NFTLending to support ERC-1155 resource tokens with time decay
 */
contract InfrastructureLending is NFTLending {
    using SafeMath for uint256;
    
    // Infrastructure-specific configuration
    struct ResourceCollateral {
        uint256 tokenId;
        uint256 amount;
        ResourceToken.ResourceType resourceType;
        ResourceToken.ServiceTier tier;
        string region;
        uint256 validUntil;
        uint256 collateralValue;
        uint256 lastValuationTime;
    }
    
    // Loan-to-value ratios by resource type and tier
    mapping(ResourceToken.ResourceType => mapping(ResourceToken.ServiceTier => uint256)) public ltvRatios;
    
    // Volatility factors for different resources
    mapping(ResourceToken.ResourceType => uint256) public volatilityFactors;
    
    // Resource collateral tracking
    mapping(uint256 => ResourceCollateral) public resourceCollaterals; // loanId => collateral
    
    // Price oracle for resource valuation
    address public priceOracle;
    
    // Time decay factor (basis points per day)
    uint256 public constant TIME_DECAY_FACTOR = 100; // 1% per day
    
    // Events
    event ResourceCollateralDeposited(
        uint256 indexed loanId,
        uint256 indexed tokenId,
        uint256 amount,
        uint256 collateralValue
    );
    
    event CollateralRevalued(
        uint256 indexed loanId,
        uint256 oldValue,
        uint256 newValue
    );
    
    event LTVRatioUpdated(
        ResourceToken.ResourceType resourceType,
        ResourceToken.ServiceTier tier,
        uint256 newRatio
    );
    
    /**
     * @dev Constructor
     * @param _priceOracle Address of the price oracle contract
     */
    constructor(address _priceOracle) {
        require(_priceOracle != address(0), "Invalid oracle");
        priceOracle = _priceOracle;
        
        // Initialize default LTV ratios (basis points)
        // Standard tier
        ltvRatios[ResourceToken.ResourceType.CPU][ResourceToken.ServiceTier.STANDARD] = 5000; // 50%
        ltvRatios[ResourceToken.ResourceType.GPU][ResourceToken.ServiceTier.STANDARD] = 4000; // 40%
        ltvRatios[ResourceToken.ResourceType.STORAGE][ResourceToken.ServiceTier.STANDARD] = 6000; // 60%
        ltvRatios[ResourceToken.ResourceType.BANDWIDTH][ResourceToken.ServiceTier.STANDARD] = 4500; // 45%
        ltvRatios[ResourceToken.ResourceType.MEMORY][ResourceToken.ServiceTier.STANDARD] = 5500; // 55%
        
        // Premium tier (higher LTV due to better quality)
        ltvRatios[ResourceToken.ResourceType.CPU][ResourceToken.ServiceTier.PREMIUM] = 6000; // 60%
        ltvRatios[ResourceToken.ResourceType.GPU][ResourceToken.ServiceTier.PREMIUM] = 5000; // 50%
        ltvRatios[ResourceToken.ResourceType.STORAGE][ResourceToken.ServiceTier.PREMIUM] = 7000; // 70%
        ltvRatios[ResourceToken.ResourceType.BANDWIDTH][ResourceToken.ServiceTier.PREMIUM] = 5500; // 55%
        ltvRatios[ResourceToken.ResourceType.MEMORY][ResourceToken.ServiceTier.PREMIUM] = 6500; // 65%
        
        // Guaranteed tier (highest LTV)
        ltvRatios[ResourceToken.ResourceType.CPU][ResourceToken.ServiceTier.GUARANTEED] = 7000; // 70%
        ltvRatios[ResourceToken.ResourceType.GPU][ResourceToken.ServiceTier.GUARANTEED] = 6000; // 60%
        ltvRatios[ResourceToken.ResourceType.STORAGE][ResourceToken.ServiceTier.GUARANTEED] = 8000; // 80%
        ltvRatios[ResourceToken.ResourceType.BANDWIDTH][ResourceToken.ServiceTier.GUARANTEED] = 6500; // 65%
        ltvRatios[ResourceToken.ResourceType.MEMORY][ResourceToken.ServiceTier.GUARANTEED] = 7500; // 75%
        
        // Volatility factors (higher = more volatile)
        volatilityFactors[ResourceToken.ResourceType.CPU] = 100;
        volatilityFactors[ResourceToken.ResourceType.GPU] = 150; // Most volatile
        volatilityFactors[ResourceToken.ResourceType.STORAGE] = 50; // Least volatile
        volatilityFactors[ResourceToken.ResourceType.BANDWIDTH] = 120;
        volatilityFactors[ResourceToken.ResourceType.MEMORY] = 80;
    }
    
    /**
     * @notice Borrow against infrastructure resource tokens
     * @param resourceTokenAddress Address of ResourceToken contract
     * @param tokenId Resource token ID to use as collateral
     * @param amount Amount of resource tokens to collateralize
     * @param loanAmount Amount to borrow
     * @param duration Loan duration in seconds
     * @param paymentToken Token to borrow and repay in
     */
    function borrowWithResource(
        address resourceTokenAddress,
        uint256 tokenId,
        uint256 amount,
        uint256 loanAmount,
        uint256 duration,
        address paymentToken
    ) external nonReentrant whenNotPaused returns (uint256) {
        require(supportedTokens[paymentToken], "Unsupported payment token");
        require(amount > 0, "Invalid amount");
        require(loanAmount > 0, "Invalid loan amount");
        require(duration >= 86400, "Minimum duration 1 day");
        
        // Get resource details
        ResourceToken resourceToken = ResourceToken(resourceTokenAddress);
        ResourceToken.ResourceSpec memory spec = resourceToken.getResourceSpec(tokenId);
        
        require(spec.isActive, "Resource not active");
        require(block.timestamp < spec.validUntil, "Resource expired");
        require(spec.validUntil >= block.timestamp + duration, "Resource expires before loan");
        
        // Calculate collateral value
        uint256 collateralValue = calculateResourceValue(
            spec.resourceType,
            spec.tier,
            spec.region,
            amount,
            spec.validUntil
        );
        
        // Apply LTV ratio
        uint256 maxLoan = collateralValue
            .mul(ltvRatios[spec.resourceType][spec.tier])
            .div(10000);
        
        require(loanAmount <= maxLoan, "Loan exceeds LTV");
        
        // Transfer collateral
        IERC1155(resourceTokenAddress).safeTransferFrom(
            msg.sender,
            address(this),
            tokenId,
            amount,
            ""
        );
        
        // Create loan
        uint256 loanId = nextLoanId++;
        
        // Calculate interest based on resource volatility
        uint256 baseRate = 500; // 5% base
        uint256 volatilityPremium = volatilityFactors[spec.resourceType];
        uint256 interestRate = baseRate.add(volatilityPremium);
        
        uint256 interest = loanAmount.mul(interestRate).mul(duration).div(365 days).div(10000);
        
        loans[loanId] = Loan({
            loanId: loanId,
            borrower: msg.sender,
            lender: address(this), // Pool lending
            nftContract: resourceTokenAddress,
            tokenId: tokenId,
            principal: loanAmount,
            interest: interest,
            duration: duration,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            status: LoanStatus.Active,
            paymentToken: paymentToken
        });
        
        // Store collateral details
        resourceCollaterals[loanId] = ResourceCollateral({
            tokenId: tokenId,
            amount: amount,
            resourceType: spec.resourceType,
            tier: spec.tier,
            region: spec.region,
            validUntil: spec.validUntil,
            collateralValue: collateralValue,
            lastValuationTime: block.timestamp
        });
        
        // Track loan
        borrowerLoans[msg.sender].push(loanId);
        nftCollateral[resourceTokenAddress][tokenId] = loanId;
        
        // Transfer loan amount
        IERC20(paymentToken).transfer(msg.sender, loanAmount);
        
        emit LoanCreated(loanId, msg.sender, address(this));
        emit ResourceCollateralDeposited(loanId, tokenId, amount, collateralValue);
        
        return loanId;
    }
    
    /**
     * @notice Calculate current value of resource collateral
     * @param resourceType Type of resource
     * @param tier Service tier
     * @param region Resource region
     * @param amount Amount of resources
     * @param validUntil Expiry timestamp
     */
    function calculateResourceValue(
        ResourceToken.ResourceType resourceType,
        ResourceToken.ServiceTier tier,
        string memory region,
        uint256 amount,
        uint256 validUntil
    ) public view returns (uint256) {
        // Get base price from oracle
        uint256 basePrice = getResourcePrice(resourceType, tier, region);
        
        // Apply time decay
        uint256 timeRemaining = 0;
        if (validUntil > block.timestamp) {
            timeRemaining = validUntil - block.timestamp;
        }
        
        // Linear decay - lose 1% value per day
        uint256 daysRemaining = timeRemaining / 86400;
        uint256 timeValue = 10000; // 100% in basis points
        
        if (daysRemaining < 100) {
            uint256 decay = (100 - daysRemaining) * TIME_DECAY_FACTOR;
            timeValue = timeValue > decay ? timeValue - decay : 0;
        }
        
        // Calculate final value
        return basePrice.mul(amount).mul(timeValue).div(10000);
    }
    
    /**
     * @notice Revalue collateral for active loans
     * @param loanId Loan ID to revalue
     */
    function revalueCollateral(uint256 loanId) external {
        Loan storage loan = loans[loanId];
        require(loan.status == LoanStatus.Active, "Loan not active");
        
        ResourceCollateral storage collateral = resourceCollaterals[loanId];
        
        // Calculate new value
        uint256 newValue = calculateResourceValue(
            collateral.resourceType,
            collateral.tier,
            collateral.region,
            collateral.amount,
            collateral.validUntil
        );
        
        uint256 oldValue = collateral.collateralValue;
        collateral.collateralValue = newValue;
        collateral.lastValuationTime = block.timestamp;
        
        emit CollateralRevalued(loanId, oldValue, newValue);
        
        // Check if loan is undercollateralized
        uint256 maxLoan = newValue
            .mul(ltvRatios[collateral.resourceType][collateral.tier])
            .div(10000);
        
        if (loan.principal > maxLoan) {
            // Trigger liquidation
            _liquidateLoan(loanId);
        }
    }
    
    /**
     * @notice Liquidate undercollateralized loan
     * @param loanId Loan to liquidate
     */
    function _liquidateLoan(uint256 loanId) internal {
        Loan storage loan = loans[loanId];
        ResourceCollateral storage collateral = resourceCollaterals[loanId];
        
        loan.status = LoanStatus.Liquidated;
        
        // Calculate liquidation penalty (5%)
        uint256 penalty = loan.principal.mul(500).div(10000);
        uint256 totalDue = loan.principal.add(penalty);
        
        // Try to sell collateral to cover debt
        // In production, this would trigger an auction or market sale
        
        emit LoanLiquidated(loanId, loan.borrower, totalDue);
    }
    
    /**
     * @notice Get resource price from oracle (mock implementation)
     */
    function getResourcePrice(
        ResourceToken.ResourceType resourceType,
        ResourceToken.ServiceTier tier,
        string memory region
    ) internal view returns (uint256) {
        // In production, this would call the Infrastructure Oracle Service
        // Mock prices in USD with 18 decimals
        
        uint256 basePrice;
        
        if (resourceType == ResourceToken.ResourceType.CPU) {
            basePrice = 50 * 10**18; // $50 per CPU hour
        } else if (resourceType == ResourceToken.ResourceType.GPU) {
            basePrice = 500 * 10**18; // $500 per GPU hour
        } else if (resourceType == ResourceToken.ResourceType.STORAGE) {
            basePrice = 1 * 10**18; // $1 per GB hour
        } else if (resourceType == ResourceToken.ResourceType.BANDWIDTH) {
            basePrice = 10 * 10**18; // $10 per TB
        } else {
            basePrice = 20 * 10**18; // $20 per GB hour (memory)
        }
        
        // Adjust for tier
        if (tier == ResourceToken.ServiceTier.PREMIUM) {
            basePrice = basePrice.mul(150).div(100); // 50% premium
        } else if (tier == ResourceToken.ServiceTier.GUARANTEED) {
            basePrice = basePrice.mul(200).div(100); // 100% premium
        }
        
        return basePrice;
    }
    
    /**
     * @notice Update LTV ratio for resource type and tier
     */
    function updateLTVRatio(
        ResourceToken.ResourceType resourceType,
        ResourceToken.ServiceTier tier,
        uint256 newRatio
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(newRatio <= 9000, "LTV too high"); // Max 90%
        ltvRatios[resourceType][tier] = newRatio;
        emit LTVRatioUpdated(resourceType, tier, newRatio);
    }
    
    /**
     * @notice Update volatility factor for resource type
     */
    function updateVolatilityFactor(
        ResourceToken.ResourceType resourceType,
        uint256 newFactor
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(newFactor <= 1000, "Factor too high");
        volatilityFactors[resourceType] = newFactor;
    }
    
    /**
     * @notice ERC1155 receiver
     */
    function onERC1155Received(
        address,
        address,
        uint256,
        uint256,
        bytes memory
    ) public pure returns (bytes4) {
        return this.onERC1155Received.selector;
    }
    
    /**
     * @notice Batch ERC1155 receiver
     */
    function onERC1155BatchReceived(
        address,
        address,
        uint256[] memory,
        uint256[] memory,
        bytes memory
    ) public pure returns (bytes4) {
        return this.onERC1155BatchReceived.selector;
    }
} 