// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";

interface IComputeOracle {
    function getQualityScore(uint256 resourceId) external view returns (uint256 score, uint256 timestamp);
    function getResourcePrice(uint256 resourceId) external view returns (uint256 price);
}

/**
 * @title ComputeLendingPool
 * @notice Lending pool for compute resources with quality-based rates
 */
contract ComputeLendingPool is AccessControl, ReentrancyGuard, Pausable {
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant LIQUIDATOR_ROLE = keccak256("LIQUIDATOR_ROLE");
    
    // Resource and collateral tokens
    IERC1155 public immutable resourceToken;
    IERC20 public immutable collateralToken;
    IComputeOracle public immutable oracle;
    
    // Lending parameters
    uint256 public reserveFactor = 1000; // 10%
    uint256 public liquidationThreshold = 8000; // 80%
    uint256 public liquidationBonus = 500; // 5%
    uint256 public constant PRECISION = 10000;
    
    // Interest rate model parameters
    uint256 public baseRate = 300; // 3%
    uint256 public rateSlope1 = 800; // 8%
    uint256 public rateSlope2 = 10000; // 100%
    uint256 public optimalUtilization = 8000; // 80%
    
    // Loan structure
    struct Loan {
        address borrower;
        uint256[] resourceIds;
        uint256[] amounts;
        uint256 principal;
        uint256 collateral;
        uint256 borrowRate;
        uint256 startTime;
        uint256 duration;
        bool active;
    }
    
    // Pool state
    uint256 public totalLiquidity;
    uint256 public totalBorrowed;
    uint256 public totalReserves;
    uint256 public loanIdCounter;
    
    mapping(uint256 => Loan) public loans;
    mapping(address => uint256[]) public userLoans;
    mapping(address => uint256) public supplierBalances;
    
    // Events
    event LiquiditySupplied(address indexed supplier, uint256 amount);
    event LiquidityWithdrawn(address indexed supplier, uint256 amount);
    event LoanCreated(
        uint256 indexed loanId,
        address indexed borrower,
        uint256 principal,
        uint256 collateral
    );
    event LoanRepaid(uint256 indexed loanId, uint256 amount);
    event LoanLiquidated(
        uint256 indexed loanId,
        address indexed liquidator,
        uint256 collateralSeized
    );
    
    constructor(
        address _resourceToken,
        address _collateralToken,
        address _oracle
    ) {
        resourceToken = IERC1155(_resourceToken);
        collateralToken = IERC20(_collateralToken);
        oracle = IComputeOracle(_oracle);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Supply liquidity to the pool
     * @param amount Amount of collateral tokens to supply
     */
    function supply(uint256 amount) external nonReentrant whenNotPaused {
        require(amount > 0, "Zero amount");
        
        // Transfer collateral from supplier
        require(
            collateralToken.transferFrom(msg.sender, address(this), amount),
            "Transfer failed"
        );
        
        supplierBalances[msg.sender] += amount;
        totalLiquidity += amount;
        
        emit LiquiditySupplied(msg.sender, amount);
    }
    
    /**
     * @notice Withdraw liquidity from the pool
     * @param amount Amount to withdraw
     */
    function withdraw(uint256 amount) external nonReentrant {
        require(amount > 0, "Zero amount");
        require(supplierBalances[msg.sender] >= amount, "Insufficient balance");
        require(getAvailableLiquidity() >= amount, "Insufficient liquidity");
        
        supplierBalances[msg.sender] -= amount;
        totalLiquidity -= amount;
        
        require(
            collateralToken.transfer(msg.sender, amount),
            "Transfer failed"
        );
        
        emit LiquidityWithdrawn(msg.sender, amount);
    }
    
    /**
     * @notice Borrow compute resources
     * @param resourceIds Resource IDs to borrow
     * @param amounts Amounts to borrow
     * @param duration Loan duration in seconds
     * @param collateralAmount Collateral to provide
     * @return loanId Created loan ID
     */
    function borrow(
        uint256[] memory resourceIds,
        uint256[] memory amounts,
        uint256 duration,
        uint256 collateralAmount
    ) external nonReentrant whenNotPaused returns (uint256 loanId) {
        require(resourceIds.length == amounts.length, "Array mismatch");
        require(resourceIds.length > 0, "Empty borrow");
        require(duration > 0 && duration <= 365 days, "Invalid duration");
        
        // Calculate loan value
        uint256 loanValue = _calculateLoanValue(resourceIds, amounts);
        
        // Check collateral requirements
        uint256 requiredCollateral = loanValue * liquidationThreshold / PRECISION;
        require(collateralAmount >= requiredCollateral, "Insufficient collateral");
        
        // Check available liquidity
        require(getAvailableLiquidity() >= loanValue, "Insufficient liquidity");
        
        // Transfer collateral from borrower
        require(
            collateralToken.transferFrom(msg.sender, address(this), collateralAmount),
            "Collateral transfer failed"
        );
        
        // Calculate interest rate
        uint256 borrowRate = _calculateBorrowRate();
        
        // Create loan
        loanId = ++loanIdCounter;
        loans[loanId] = Loan({
            borrower: msg.sender,
            resourceIds: resourceIds,
            amounts: amounts,
            principal: loanValue,
            collateral: collateralAmount,
            borrowRate: borrowRate,
            startTime: block.timestamp,
            duration: duration,
            active: true
        });
        
        userLoans[msg.sender].push(loanId);
        totalBorrowed += loanValue;
        
        // Transfer resources to borrower
        resourceToken.safeBatchTransferFrom(
            address(this),
            msg.sender,
            resourceIds,
            amounts,
            ""
        );
        
        emit LoanCreated(loanId, msg.sender, loanValue, collateralAmount);
    }
    
    /**
     * @notice Repay a loan
     * @param loanId Loan to repay
     */
    function repay(uint256 loanId) external nonReentrant {
        Loan storage loan = loans[loanId];
        require(loan.active, "Loan not active");
        require(loan.borrower == msg.sender, "Not borrower");
        
        // Calculate repayment amount
        uint256 timeElapsed = block.timestamp - loan.startTime;
        uint256 interest = loan.principal * loan.borrowRate * timeElapsed / (365 days * PRECISION);
        uint256 totalRepayment = loan.principal + interest;
        
        // Update reserves
        uint256 reserveAmount = interest * reserveFactor / PRECISION;
        totalReserves += reserveAmount;
        
        // Return resources
        resourceToken.safeBatchTransferFrom(
            msg.sender,
            address(this),
            loan.resourceIds,
            loan.amounts,
            ""
        );
        
        // Return collateral
        require(
            collateralToken.transfer(msg.sender, loan.collateral),
            "Collateral return failed"
        );
        
        // Update state
        loan.active = false;
        totalBorrowed -= loan.principal;
        
        emit LoanRepaid(loanId, totalRepayment);
    }
    
    /**
     * @notice Liquidate an undercollateralized loan
     * @param loanId Loan to liquidate
     */
    function liquidate(uint256 loanId) external nonReentrant {
        Loan storage loan = loans[loanId];
        require(loan.active, "Loan not active");
        
        // Check if loan is liquidatable
        uint256 currentLoanValue = _calculateLoanValue(loan.resourceIds, loan.amounts);
        uint256 collateralRatio = loan.collateral * PRECISION / currentLoanValue;
        
        require(collateralRatio < liquidationThreshold, "Not liquidatable");
        
        // Calculate liquidation bonus
        uint256 liquidationAmount = loan.collateral * (PRECISION + liquidationBonus) / PRECISION;
        
        // Transfer resources from liquidator
        resourceToken.safeBatchTransferFrom(
            msg.sender,
            address(this),
            loan.resourceIds,
            loan.amounts,
            ""
        );
        
        // Transfer collateral to liquidator
        require(
            collateralToken.transfer(msg.sender, liquidationAmount),
            "Liquidation transfer failed"
        );
        
        // Update state
        loan.active = false;
        totalBorrowed -= loan.principal;
        
        emit LoanLiquidated(loanId, msg.sender, liquidationAmount);
    }
    
    /**
     * @notice Execute flash loan
     * @param resourceIds Resources to flash borrow
     * @param amounts Amounts to borrow
     * @param receiver Contract to receive resources
     * @param data Callback data
     */
    function flashLoan(
        uint256[] memory resourceIds,
        uint256[] memory amounts,
        address receiver,
        bytes calldata data
    ) external nonReentrant whenNotPaused {
        require(resourceIds.length == amounts.length, "Array mismatch");
        
        // Calculate flash loan fee (0.1%)
        uint256 loanValue = _calculateLoanValue(resourceIds, amounts);
        uint256 flashFee = loanValue * 10 / PRECISION; // 0.1%
        
        // Record balances before
        uint256[] memory balancesBefore = new uint256[](resourceIds.length);
        for (uint256 i = 0; i < resourceIds.length; i++) {
            balancesBefore[i] = resourceToken.balanceOf(address(this), resourceIds[i]);
        }
        
        // Transfer resources to receiver
        resourceToken.safeBatchTransferFrom(
            address(this),
            receiver,
            resourceIds,
            amounts,
            ""
        );
        
        // Execute callback
        IFlashLoanReceiver(receiver).executeOperation(
            resourceIds,
            amounts,
            flashFee,
            msg.sender,
            data
        );
        
        // Verify resources returned plus fee
        for (uint256 i = 0; i < resourceIds.length; i++) {
            uint256 balanceAfter = resourceToken.balanceOf(address(this), resourceIds[i]);
            require(
                balanceAfter >= balancesBefore[i],
                "Flash loan not repaid"
            );
        }
        
        // Collect flash fee
        require(
            collateralToken.transferFrom(msg.sender, address(this), flashFee),
            "Flash fee transfer failed"
        );
        
        totalReserves += flashFee;
    }
    
    /**
     * @notice Get current borrow rate
     * @return rate Current borrow rate
     */
    function getBorrowRate() external view returns (uint256 rate) {
        return _calculateBorrowRate();
    }
    
    /**
     * @notice Get current supply rate
     * @return rate Current supply rate
     */
    function getSupplyRate() external view returns (uint256 rate) {
        uint256 borrowRate = _calculateBorrowRate();
        uint256 utilization = getUtilizationRate();
        
        return borrowRate * utilization * (PRECISION - reserveFactor) / (PRECISION * PRECISION);
    }
    
    /**
     * @notice Get pool utilization rate
     * @return utilization Utilization rate in basis points
     */
    function getUtilizationRate() public view returns (uint256 utilization) {
        if (totalLiquidity == 0) return 0;
        return totalBorrowed * PRECISION / totalLiquidity;
    }
    
    /**
     * @notice Get available liquidity
     * @return available Available liquidity
     */
    function getAvailableLiquidity() public view returns (uint256 available) {
        return totalLiquidity - totalBorrowed - totalReserves;
    }
    
    /**
     * @notice Update interest rate parameters
     */
    function updateInterestRateModel(
        uint256 _baseRate,
        uint256 _rateSlope1,
        uint256 _rateSlope2,
        uint256 _optimalUtilization
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        baseRate = _baseRate;
        rateSlope1 = _rateSlope1;
        rateSlope2 = _rateSlope2;
        optimalUtilization = _optimalUtilization;
    }
    
    /**
     * @notice Withdraw reserves
     * @param amount Amount to withdraw
     * @param to Recipient address
     */
    function withdrawReserves(
        uint256 amount,
        address to
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(amount <= totalReserves, "Insufficient reserves");
        
        totalReserves -= amount;
        require(
            collateralToken.transfer(to, amount),
            "Reserve transfer failed"
        );
    }
    
    // Internal functions
    
    function _calculateLoanValue(
        uint256[] memory resourceIds,
        uint256[] memory amounts
    ) internal view returns (uint256 value) {
        for (uint256 i = 0; i < resourceIds.length; i++) {
            uint256 price = oracle.getResourcePrice(resourceIds[i]);
            (uint256 quality, ) = oracle.getQualityScore(resourceIds[i]);
            
            // Apply quality adjustment
            uint256 qualityMultiplier = PRECISION + (quality - 80) * 20; // ±2% per quality point from 80
            value += price * amounts[i] * qualityMultiplier / PRECISION;
        }
    }
    
    function _calculateBorrowRate() internal view returns (uint256) {
        uint256 utilization = getUtilizationRate();
        
        if (utilization <= optimalUtilization) {
            // Below optimal: base + utilization * slope1
            return baseRate + utilization * rateSlope1 / optimalUtilization;
        } else {
            // Above optimal: base + slope1 + excess * slope2
            uint256 excess = utilization - optimalUtilization;
            return baseRate + rateSlope1 + excess * rateSlope2 / (PRECISION - optimalUtilization);
        }
    }
    
    // Required for ERC-1155 receiver
    function onERC1155Received(
        address,
        address,
        uint256,
        uint256,
        bytes calldata
    ) external pure returns (bytes4) {
        return this.onERC1155Received.selector;
    }
    
    function onERC1155BatchReceived(
        address,
        address,
        uint256[] calldata,
        uint256[] calldata,
        bytes calldata
    ) external pure returns (bytes4) {
        return this.onERC1155BatchReceived.selector;
    }
}

interface IFlashLoanReceiver {
    function executeOperation(
        uint256[] memory resourceIds,
        uint256[] memory amounts,
        uint256 fee,
        address initiator,
        bytes calldata data
    ) external;
} 