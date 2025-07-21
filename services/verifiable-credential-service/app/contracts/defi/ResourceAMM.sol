// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "./ResourceToken.sol";

/**
 * @title ResourceAMM
 * @notice Automated Market Maker for Infrastructure Resource Tokens
 * @dev Specialized AMM with time decay for expiring resources and quality tiers
 */
contract ResourceAMM is ReentrancyGuard, Ownable {
    using SafeMath for uint256;
    
    // Pool information
    struct ResourcePool {
        uint256 poolId;
        uint256 resourceTokenId;    // ResourceToken NFT ID
        address quoteToken;          // USDC, ETH, etc.
        uint256 resourceReserve;     // Amount of resource tokens
        uint256 quoteReserve;        // Amount of quote tokens
        uint256 totalLiquidity;      // Total LP tokens
        uint256 feePercentage;       // Trading fee (basis points)
        bool isActive;
        
        // Resource-specific parameters
        ResourceToken.ResourceType resourceType;
        ResourceToken.ServiceTier tier;
        string region;
        uint256 expiryTime;          // When resources expire
        uint256 timeDecayFactor;     // Price decay as expiry approaches
    }
    
    // LP token for each pool
    contract LPToken is ERC20 {
        constructor(string memory name, string memory symbol) ERC20(name, symbol) {}
        
        function mint(address to, uint256 amount) external {
            require(msg.sender == address(ResourceAMM(owner()).getAddress()), "Unauthorized");
            _mint(to, amount);
        }
        
        function burn(address from, uint256 amount) external {
            require(msg.sender == address(ResourceAMM(owner()).getAddress()), "Unauthorized");
            _burn(from, amount);
        }
    }
    
    // State variables
    ResourceToken public immutable resourceToken;
    uint256 public nextPoolId;
    mapping(uint256 => ResourcePool) public pools;
    mapping(uint256 => LPToken) public lpTokens;
    mapping(address => mapping(uint256 => uint256)) public userLiquidity;
    
    // Fee configuration
    uint256 public constant MAX_FEE = 1000; // 10% max fee
    uint256 public protocolFeePercentage = 50; // 0.5% protocol fee
    address public feeRecipient;
    
    // Constants
    uint256 private constant PRECISION = 1e18;
    uint256 private constant MIN_LIQUIDITY = 1000;
    
    // Events
    event PoolCreated(
        uint256 indexed poolId,
        uint256 indexed resourceTokenId,
        address indexed quoteToken,
        uint256 feePercentage
    );
    
    event LiquidityAdded(
        uint256 indexed poolId,
        address indexed provider,
        uint256 resourceAmount,
        uint256 quoteAmount,
        uint256 liquidity
    );
    
    event LiquidityRemoved(
        uint256 indexed poolId,
        address indexed provider,
        uint256 resourceAmount,
        uint256 quoteAmount,
        uint256 liquidity
    );
    
    event Swap(
        uint256 indexed poolId,
        address indexed trader,
        bool isResourceToQuote,
        uint256 amountIn,
        uint256 amountOut,
        uint256 fee
    );
    
    /**
     * @dev Constructor
     * @param _resourceToken Address of the ResourceToken contract
     */
    constructor(address _resourceToken) {
        require(_resourceToken != address(0), "Invalid resource token");
        resourceToken = ResourceToken(_resourceToken);
        feeRecipient = msg.sender;
    }
    
    /**
     * @notice Create a new resource liquidity pool
     * @param resourceTokenId ID of the resource token
     * @param quoteToken Address of the quote token (e.g., USDC)
     * @param feePercentage Trading fee in basis points
     * @return poolId The ID of the created pool
     */
    function createPool(
        uint256 resourceTokenId,
        address quoteToken,
        uint256 feePercentage
    ) external onlyOwner returns (uint256) {
        require(quoteToken != address(0), "Invalid quote token");
        require(feePercentage <= MAX_FEE, "Fee too high");
        
        // Get resource details
        ResourceToken.ResourceSpec memory spec = resourceToken.getResourceSpec(resourceTokenId);
        require(spec.isActive, "Resource not active");
        
        // Create pool
        uint256 poolId = nextPoolId++;
        pools[poolId] = ResourcePool({
            poolId: poolId,
            resourceTokenId: resourceTokenId,
            quoteToken: quoteToken,
            resourceReserve: 0,
            quoteReserve: 0,
            totalLiquidity: 0,
            feePercentage: feePercentage,
            isActive: true,
            resourceType: spec.resourceType,
            tier: spec.tier,
            region: spec.region,
            expiryTime: spec.validUntil,
            timeDecayFactor: 9500 // 5% decay factor
        });
        
        // Create LP token
        string memory lpName = string(abi.encodePacked("Resource LP #", _toString(poolId)));
        string memory lpSymbol = string(abi.encodePacked("RLP", _toString(poolId)));
        lpTokens[poolId] = new LPToken(lpName, lpSymbol);
        
        emit PoolCreated(poolId, resourceTokenId, quoteToken, feePercentage);
        
        return poolId;
    }
    
    /**
     * @notice Add liquidity to a pool
     * @param poolId Pool ID
     * @param resourceAmount Amount of resource tokens to add
     * @param quoteAmount Amount of quote tokens to add
     * @param minLiquidity Minimum LP tokens to receive
     * @return liquidity Amount of LP tokens minted
     */
    function addLiquidity(
        uint256 poolId,
        uint256 resourceAmount,
        uint256 quoteAmount,
        uint256 minLiquidity
    ) external nonReentrant returns (uint256 liquidity) {
        ResourcePool storage pool = pools[poolId];
        require(pool.isActive, "Pool not active");
        require(resourceAmount > 0 && quoteAmount > 0, "Invalid amounts");
        
        // Check resource validity
        require(block.timestamp < pool.expiryTime, "Resources expired");
        
        // Transfer tokens
        IERC1155(address(resourceToken)).safeTransferFrom(
            msg.sender,
            address(this),
            pool.resourceTokenId,
            resourceAmount,
            ""
        );
        IERC20(pool.quoteToken).transferFrom(msg.sender, address(this), quoteAmount);
        
        // Calculate liquidity
        if (pool.totalLiquidity == 0) {
            // First liquidity provider
            liquidity = _sqrt(resourceAmount.mul(quoteAmount)).sub(MIN_LIQUIDITY);
            require(liquidity > 0, "Insufficient initial liquidity");
            
            // Lock minimum liquidity
            lpTokens[poolId].mint(address(0), MIN_LIQUIDITY);
            pool.totalLiquidity = MIN_LIQUIDITY;
        } else {
            // Subsequent providers
            uint256 resourceLiquidity = resourceAmount.mul(pool.totalLiquidity).div(pool.resourceReserve);
            uint256 quoteLiquidity = quoteAmount.mul(pool.totalLiquidity).div(pool.quoteReserve);
            liquidity = resourceLiquidity < quoteLiquidity ? resourceLiquidity : quoteLiquidity;
        }
        
        require(liquidity >= minLiquidity, "Insufficient liquidity minted");
        
        // Update reserves
        pool.resourceReserve = pool.resourceReserve.add(resourceAmount);
        pool.quoteReserve = pool.quoteReserve.add(quoteAmount);
        pool.totalLiquidity = pool.totalLiquidity.add(liquidity);
        
        // Mint LP tokens
        lpTokens[poolId].mint(msg.sender, liquidity);
        userLiquidity[msg.sender][poolId] = userLiquidity[msg.sender][poolId].add(liquidity);
        
        emit LiquidityAdded(poolId, msg.sender, resourceAmount, quoteAmount, liquidity);
    }
    
    /**
     * @notice Remove liquidity from a pool
     * @param poolId Pool ID
     * @param liquidity Amount of LP tokens to burn
     * @param minResourceAmount Minimum resource tokens to receive
     * @param minQuoteAmount Minimum quote tokens to receive
     */
    function removeLiquidity(
        uint256 poolId,
        uint256 liquidity,
        uint256 minResourceAmount,
        uint256 minQuoteAmount
    ) external nonReentrant {
        ResourcePool storage pool = pools[poolId];
        require(liquidity > 0, "Invalid liquidity");
        require(userLiquidity[msg.sender][poolId] >= liquidity, "Insufficient liquidity");
        
        // Calculate amounts
        uint256 resourceAmount = liquidity.mul(pool.resourceReserve).div(pool.totalLiquidity);
        uint256 quoteAmount = liquidity.mul(pool.quoteReserve).div(pool.totalLiquidity);
        
        require(resourceAmount >= minResourceAmount, "Insufficient resource amount");
        require(quoteAmount >= minQuoteAmount, "Insufficient quote amount");
        
        // Update state
        pool.resourceReserve = pool.resourceReserve.sub(resourceAmount);
        pool.quoteReserve = pool.quoteReserve.sub(quoteAmount);
        pool.totalLiquidity = pool.totalLiquidity.sub(liquidity);
        userLiquidity[msg.sender][poolId] = userLiquidity[msg.sender][poolId].sub(liquidity);
        
        // Burn LP tokens
        lpTokens[poolId].burn(msg.sender, liquidity);
        
        // Transfer tokens
        IERC1155(address(resourceToken)).safeTransferFrom(
            address(this),
            msg.sender,
            pool.resourceTokenId,
            resourceAmount,
            ""
        );
        IERC20(pool.quoteToken).transfer(msg.sender, quoteAmount);
        
        emit LiquidityRemoved(poolId, msg.sender, resourceAmount, quoteAmount, liquidity);
    }
    
    /**
     * @notice Swap tokens in a pool
     * @param poolId Pool ID
     * @param isResourceToQuote True if swapping resource for quote token
     * @param amountIn Amount of tokens to swap
     * @param minAmountOut Minimum amount to receive
     * @return amountOut Amount of tokens received
     */
    function swap(
        uint256 poolId,
        bool isResourceToQuote,
        uint256 amountIn,
        uint256 minAmountOut
    ) external nonReentrant returns (uint256 amountOut) {
        ResourcePool storage pool = pools[poolId];
        require(pool.isActive, "Pool not active");
        require(amountIn > 0, "Invalid amount");
        
        // Apply time decay for expiring resources
        uint256 adjustedPrice = _applyTimeDecay(pool);
        
        // Calculate output amount
        uint256 amountInWithFee = amountIn.mul(10000 - pool.feePercentage).div(10000);
        uint256 protocolFee = amountIn.mul(protocolFeePercentage).div(10000);
        
        if (isResourceToQuote) {
            // Selling resources for quote tokens
            amountOut = _getAmountOut(
                amountInWithFee,
                pool.resourceReserve.mul(adjustedPrice).div(PRECISION),
                pool.quoteReserve
            );
            require(amountOut >= minAmountOut, "Insufficient output");
            
            // Transfer tokens
            IERC1155(address(resourceToken)).safeTransferFrom(
                msg.sender,
                address(this),
                pool.resourceTokenId,
                amountIn,
                ""
            );
            IERC20(pool.quoteToken).transfer(msg.sender, amountOut);
            IERC20(pool.quoteToken).transfer(feeRecipient, protocolFee);
            
            // Update reserves
            pool.resourceReserve = pool.resourceReserve.add(amountIn);
            pool.quoteReserve = pool.quoteReserve.sub(amountOut).sub(protocolFee);
        } else {
            // Buying resources with quote tokens
            amountOut = _getAmountOut(
                amountInWithFee,
                pool.quoteReserve,
                pool.resourceReserve.mul(adjustedPrice).div(PRECISION)
            );
            require(amountOut >= minAmountOut, "Insufficient output");
            
            // Transfer tokens
            IERC20(pool.quoteToken).transferFrom(msg.sender, address(this), amountIn);
            IERC1155(address(resourceToken)).safeTransferFrom(
                address(this),
                msg.sender,
                pool.resourceTokenId,
                amountOut,
                ""
            );
            IERC20(pool.quoteToken).transfer(feeRecipient, protocolFee);
            
            // Update reserves
            pool.quoteReserve = pool.quoteReserve.add(amountIn).sub(protocolFee);
            pool.resourceReserve = pool.resourceReserve.sub(amountOut);
        }
        
        emit Swap(poolId, msg.sender, isResourceToQuote, amountIn, amountOut, protocolFee);
    }
    
    /**
     * @notice Get output amount for a swap
     * @param poolId Pool ID
     * @param isResourceToQuote Direction of swap
     * @param amountIn Input amount
     * @return amountOut Output amount
     */
    function getAmountOut(
        uint256 poolId,
        bool isResourceToQuote,
        uint256 amountIn
    ) external view returns (uint256 amountOut) {
        ResourcePool memory pool = pools[poolId];
        require(pool.isActive, "Pool not active");
        
        uint256 adjustedPrice = _applyTimeDecay(pool);
        uint256 amountInWithFee = amountIn.mul(10000 - pool.feePercentage).div(10000);
        
        if (isResourceToQuote) {
            amountOut = _getAmountOut(
                amountInWithFee,
                pool.resourceReserve.mul(adjustedPrice).div(PRECISION),
                pool.quoteReserve
            );
        } else {
            amountOut = _getAmountOut(
                amountInWithFee,
                pool.quoteReserve,
                pool.resourceReserve.mul(adjustedPrice).div(PRECISION)
            );
        }
    }
    
    /**
     * @dev Apply time decay to resource price
     */
    function _applyTimeDecay(ResourcePool memory pool) private view returns (uint256) {
        if (block.timestamp >= pool.expiryTime) {
            return 0; // Resources expired
        }
        
        uint256 timeRemaining = pool.expiryTime - block.timestamp;
        uint256 totalDuration = pool.expiryTime - block.timestamp; // Simplified
        
        // Linear decay: price reduces as expiry approaches
        return PRECISION.mul(timeRemaining).div(totalDuration).mul(pool.timeDecayFactor).div(10000);
    }
    
    /**
     * @dev Calculate output amount using constant product formula
     */
    function _getAmountOut(
        uint256 amountIn,
        uint256 reserveIn,
        uint256 reserveOut
    ) private pure returns (uint256) {
        require(amountIn > 0, "Insufficient input");
        require(reserveIn > 0 && reserveOut > 0, "Insufficient liquidity");
        
        uint256 numerator = amountIn.mul(reserveOut);
        uint256 denominator = reserveIn.add(amountIn);
        
        return numerator.div(denominator);
    }
    
    /**
     * @dev Square root function
     */
    function _sqrt(uint256 x) private pure returns (uint256 y) {
        uint256 z = (x + 1) / 2;
        y = x;
        while (z < y) {
            y = z;
            z = (x / z + z) / 2;
        }
    }
    
    /**
     * @dev Convert uint to string
     */
    function _toString(uint256 value) private pure returns (string memory) {
        if (value == 0) {
            return "0";
        }
        uint256 temp = value;
        uint256 digits;
        while (temp != 0) {
            digits++;
            temp /= 10;
        }
        bytes memory buffer = new bytes(digits);
        while (value != 0) {
            digits -= 1;
            buffer[digits] = bytes1(uint8(48 + uint256(value % 10)));
            value /= 10;
        }
        return string(buffer);
    }
    
    /**
     * @notice Get contract address (for LP token minting)
     */
    function getAddress() external view returns (address) {
        return address(this);
    }
    
    /**
     * @notice Update protocol fee
     */
    function setProtocolFee(uint256 _fee) external onlyOwner {
        require(_fee <= 100, "Fee too high"); // Max 1%
        protocolFeePercentage = _fee;
    }
    
    /**
     * @notice Update fee recipient
     */
    function setFeeRecipient(address _recipient) external onlyOwner {
        require(_recipient != address(0), "Invalid recipient");
        feeRecipient = _recipient;
    }
    
    /**
     * @notice Emergency pause pool
     */
    function pausePool(uint256 poolId) external onlyOwner {
        pools[poolId].isActive = false;
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
} 