// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC1155/utils/ERC1155Holder.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "./ResourceToken.sol";
import "./ResourceAMM.sol";

/**
 * @title FlashResourceProvider
 * @notice Enables flash loans of infrastructure resources for instant provisioning
 * @dev Implements ERC-3156 flash loan standard adapted for ERC-1155 resource tokens
 */
contract FlashResourceProvider is ERC1155Holder, ReentrancyGuard, AccessControl, Pausable {
    
    // Roles
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant FEE_MANAGER_ROLE = keccak256("FEE_MANAGER_ROLE");
    
    // Constants
    uint256 public constant MAX_FLASH_LOAN_AMOUNT = 10000; // Max resources per flash loan
    uint256 public constant FLASH_LOAN_FEE = 10; // 0.1% fee in basis points
    
    // State variables
    ResourceToken public immutable resourceToken;
    ResourceAMM public immutable resourceAMM;
    
    // Flash loan fees by resource type (basis points)
    mapping(ResourceToken.ResourceType => uint256) public flashFees;
    
    // Trusted flash loan receivers
    mapping(address => bool) public trustedReceivers;
    
    // Flash loan statistics
    struct FlashLoanStats {
        uint256 totalLoans;
        uint256 totalVolume;
        uint256 totalFeesCollected;
        uint256 lastLoanTimestamp;
    }
    
    mapping(uint256 => FlashLoanStats) public tokenStats; // tokenId => stats
    
    // Events
    event FlashLoan(
        address indexed borrower,
        uint256 indexed tokenId,
        uint256 amount,
        uint256 fee
    );
    
    event FlashSwap(
        address indexed swapper,
        uint256 indexed fromTokenId,
        uint256 indexed toTokenId,
        uint256 fromAmount,
        uint256 toAmount
    );
    
    event TrustedReceiverUpdated(address indexed receiver, bool trusted);
    event FlashFeeUpdated(ResourceToken.ResourceType resourceType, uint256 fee);
    
    // Interfaces
    interface IFlashResourceReceiver {
        function onFlashLoan(
            address initiator,
            uint256 tokenId,
            uint256 amount,
            uint256 fee,
            bytes calldata data
        ) external returns (bytes32);
    }
    
    /**
     * @dev Constructor
     * @param _resourceToken Address of ResourceToken contract
     * @param _resourceAMM Address of ResourceAMM contract
     */
    constructor(address _resourceToken, address _resourceAMM) {
        require(_resourceToken != address(0), "Invalid resource token");
        require(_resourceAMM != address(0), "Invalid AMM");
        
        resourceToken = ResourceToken(_resourceToken);
        resourceAMM = ResourceAMM(_resourceAMM);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        _grantRole(FEE_MANAGER_ROLE, msg.sender);
        
        // Initialize default fees
        flashFees[ResourceToken.ResourceType.CPU] = 10; // 0.1%
        flashFees[ResourceToken.ResourceType.GPU] = 20; // 0.2%
        flashFees[ResourceToken.ResourceType.STORAGE] = 5; // 0.05%
        flashFees[ResourceToken.ResourceType.BANDWIDTH] = 15; // 0.15%
        flashFees[ResourceToken.ResourceType.MEMORY] = 10; // 0.1%
    }
    
    /**
     * @notice Execute a flash loan of resource tokens
     * @param receiver The receiver of the flash loan
     * @param tokenId Resource token ID to borrow
     * @param amount Amount to borrow
     * @param data Arbitrary data to pass to the receiver
     */
    function flashLoan(
        IFlashResourceReceiver receiver,
        uint256 tokenId,
        uint256 amount,
        bytes calldata data
    ) external nonReentrant whenNotPaused {
        require(amount > 0 && amount <= MAX_FLASH_LOAN_AMOUNT, "Invalid amount");
        
        // Get resource details
        ResourceToken.ResourceSpec memory spec = resourceToken.getResourceSpec(tokenId);
        require(spec.isActive, "Resource not active");
        require(block.timestamp < spec.validUntil, "Resource expired");
        
        // Calculate fee
        uint256 fee = calculateFlashFee(spec.resourceType, amount);
        
        // Check available balance
        uint256 balanceBefore = resourceToken.balanceOf(address(this), tokenId);
        require(balanceBefore >= amount, "Insufficient liquidity");
        
        // Transfer resources to receiver
        resourceToken.safeTransferFrom(address(this), address(receiver), tokenId, amount, "");
        
        // Execute receiver callback
        require(
            receiver.onFlashLoan(msg.sender, tokenId, amount, fee, data) == keccak256("ERC3156FlashBorrower.onFlashLoan"),
            "Invalid response"
        );
        
        // Verify repayment with fee
        uint256 balanceAfter = resourceToken.balanceOf(address(this), tokenId);
        require(balanceAfter >= balanceBefore + fee, "Flash loan not repaid");
        
        // Update statistics
        tokenStats[tokenId].totalLoans++;
        tokenStats[tokenId].totalVolume += amount;
        tokenStats[tokenId].totalFeesCollected += fee;
        tokenStats[tokenId].lastLoanTimestamp = block.timestamp;
        
        emit FlashLoan(msg.sender, tokenId, amount, fee);
    }
    
    /**
     * @notice Execute an atomic swap between resource types using flash loans
     * @param fromTokenId Source resource token ID
     * @param toTokenId Target resource token ID
     * @param amount Amount of source tokens to swap
     * @param poolId AMM pool ID for the swap
     */
    function flashSwap(
        uint256 fromTokenId,
        uint256 toTokenId,
        uint256 amount,
        uint256 poolId
    ) external nonReentrant whenNotPaused {
        require(amount > 0, "Invalid amount");
        require(fromTokenId != toTokenId, "Same token");
        
        // Verify both resources are active
        ResourceToken.ResourceSpec memory fromSpec = resourceToken.getResourceSpec(fromTokenId);
        ResourceToken.ResourceSpec memory toSpec = resourceToken.getResourceSpec(toTokenId);
        require(fromSpec.isActive && toSpec.isActive, "Inactive resource");
        
        // Flash borrow the source tokens
        uint256 borrowedAmount = amount;
        uint256 fee = calculateFlashFee(fromSpec.resourceType, borrowedAmount);
        
        uint256 balanceBefore = resourceToken.balanceOf(address(this), fromTokenId);
        require(balanceBefore >= borrowedAmount, "Insufficient liquidity");
        
        // Execute swap through AMM
        resourceToken.setApprovalForAll(address(resourceAMM), true);
        
        // Get quote from AMM
        (uint256 outputAmount, , ) = resourceAMM.getSwapOutput(poolId, borrowedAmount);
        require(outputAmount > 0, "Invalid swap output");
        
        // Perform the swap
        resourceAMM.swap(poolId, borrowedAmount, outputAmount, address(this));
        
        // Verify we received the target tokens
        uint256 receivedAmount = resourceToken.balanceOf(address(this), toTokenId);
        require(receivedAmount >= outputAmount, "Swap failed");
        
        // Transfer swapped tokens to user
        resourceToken.safeTransferFrom(address(this), msg.sender, toTokenId, outputAmount, "");
        
        // User must have deposited source tokens + fee
        resourceToken.safeTransferFrom(msg.sender, address(this), fromTokenId, borrowedAmount + fee, "");
        
        // Verify flash loan repayment
        uint256 balanceAfter = resourceToken.balanceOf(address(this), fromTokenId);
        require(balanceAfter >= balanceBefore + fee, "Flash swap not repaid");
        
        emit FlashSwap(msg.sender, fromTokenId, toTokenId, borrowedAmount, outputAmount);
    }
    
    /**
     * @notice Provision resources instantly using flash loans
     * @param requests Array of resource provisioning requests
     * @param receiver Contract that will handle the provisioned resources
     * @param data Arbitrary data for the receiver
     */
    function flashProvision(
        ProvisionRequest[] calldata requests,
        address receiver,
        bytes calldata data
    ) external nonReentrant whenNotPaused {
        require(trustedReceivers[receiver] || receiver == msg.sender, "Untrusted receiver");
        
        uint256[] memory tokenIds = new uint256[](requests.length);
        uint256[] memory amounts = new uint256[](requests.length);
        uint256[] memory fees = new uint256[](requests.length);
        
        // Calculate total fees and prepare batch transfer
        for (uint256 i = 0; i < requests.length; i++) {
            ProvisionRequest memory req = requests[i];
            
            // Validate resource
            ResourceToken.ResourceSpec memory spec = resourceToken.getResourceSpec(req.tokenId);
            require(spec.isActive, "Inactive resource");
            require(spec.resourceType == req.resourceType, "Type mismatch");
            require(spec.tier >= req.minTier, "Tier too low");
            
            tokenIds[i] = req.tokenId;
            amounts[i] = req.amount;
            fees[i] = calculateFlashFee(spec.resourceType, req.amount);
        }
        
        // Batch transfer resources to receiver
        resourceToken.safeBatchTransferFrom(address(this), receiver, tokenIds, amounts, data);
        
        // Execute receiver callback
        IFlashProvisionReceiver(receiver).onFlashProvision(
            msg.sender,
            tokenIds,
            amounts,
            fees,
            data
        );
        
        // Verify repayment with fees
        for (uint256 i = 0; i < requests.length; i++) {
            uint256 expectedBalance = resourceToken.balanceOf(address(this), tokenIds[i]) + fees[i];
            require(
                resourceToken.balanceOf(address(this), tokenIds[i]) >= expectedBalance,
                "Provision not repaid"
            );
            
            // Update stats
            tokenStats[tokenIds[i]].totalLoans++;
            tokenStats[tokenIds[i]].totalVolume += amounts[i];
            tokenStats[tokenIds[i]].totalFeesCollected += fees[i];
        }
    }
    
    /**
     * @notice Calculate flash loan fee for a resource type
     * @param resourceType Type of resource
     * @param amount Amount being borrowed
     * @return fee Fee amount in resource tokens
     */
    function calculateFlashFee(
        ResourceToken.ResourceType resourceType,
        uint256 amount
    ) public view returns (uint256) {
        uint256 feeRate = flashFees[resourceType];
        if (feeRate == 0) {
            feeRate = FLASH_LOAN_FEE; // Default fee
        }
        return (amount * feeRate) / 10000;
    }
    
    /**
     * @notice Update flash loan fee for a resource type
     * @param resourceType Type of resource
     * @param fee New fee in basis points
     */
    function setFlashFee(
        ResourceToken.ResourceType resourceType,
        uint256 fee
    ) external onlyRole(FEE_MANAGER_ROLE) {
        require(fee <= 100, "Fee too high"); // Max 1%
        flashFees[resourceType] = fee;
        emit FlashFeeUpdated(resourceType, fee);
    }
    
    /**
     * @notice Update trusted receiver status
     * @param receiver Receiver address
     * @param trusted Whether the receiver is trusted
     */
    function setTrustedReceiver(
        address receiver,
        bool trusted
    ) external onlyRole(OPERATOR_ROLE) {
        trustedReceivers[receiver] = trusted;
        emit TrustedReceiverUpdated(receiver, trusted);
    }
    
    /**
     * @notice Deposit resources to provide flash loan liquidity
     * @param tokenId Resource token ID
     * @param amount Amount to deposit
     */
    function depositLiquidity(uint256 tokenId, uint256 amount) external {
        resourceToken.safeTransferFrom(msg.sender, address(this), tokenId, amount, "");
    }
    
    /**
     * @notice Withdraw resources (admin only)
     * @param tokenId Resource token ID
     * @param amount Amount to withdraw
     * @param to Recipient address
     */
    function withdrawLiquidity(
        uint256 tokenId,
        uint256 amount,
        address to
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        resourceToken.safeTransferFrom(address(this), to, tokenId, amount, "");
    }
    
    /**
     * @notice Get flash loan statistics for a token
     * @param tokenId Resource token ID
     */
    function getTokenStats(uint256 tokenId) external view returns (
        uint256 totalLoans,
        uint256 totalVolume,
        uint256 totalFeesCollected,
        uint256 lastLoanTimestamp,
        uint256 availableLiquidity
    ) {
        FlashLoanStats memory stats = tokenStats[tokenId];
        uint256 liquidity = resourceToken.balanceOf(address(this), tokenId);
        
        return (
            stats.totalLoans,
            stats.totalVolume,
            stats.totalFeesCollected,
            stats.lastLoanTimestamp,
            liquidity
        );
    }
    
    /**
     * @notice Pause flash loans (emergency)
     */
    function pause() external onlyRole(OPERATOR_ROLE) {
        _pause();
    }
    
    /**
     * @notice Unpause flash loans
     */
    function unpause() external onlyRole(OPERATOR_ROLE) {
        _unpause();
    }
}

// Structs
struct ProvisionRequest {
    uint256 tokenId;
    uint256 amount;
    ResourceToken.ResourceType resourceType;
    ResourceToken.ServiceTier minTier;
}

// Interfaces
interface IFlashProvisionReceiver {
    function onFlashProvision(
        address initiator,
        uint256[] memory tokenIds,
        uint256[] memory amounts,
        uint256[] memory fees,
        bytes calldata data
    ) external;
} 