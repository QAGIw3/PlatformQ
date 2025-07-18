// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/ERC20Snapshot.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/draft-ERC20Permit.sol";

/**
 * @title ComputeToken
 * @dev ERC20 token representing fractional ownership of compute hours
 * 
 * Features:
 * - Fractional ownership of compute hours
 * - Burnable for redemption
 * - Snapshot for governance
 * - Pausable for emergency
 * - Permit for gasless transactions
 */
contract ComputeToken is ERC20, ERC20Burnable, ERC20Snapshot, Ownable, Pausable, ERC20Permit {
    // Compute offering details
    string public offeringId;
    string public resourceType;
    string public provider;
    uint256 public totalComputeHours;
    uint256 public minFractionSize;
    
    // Redemption tracking
    mapping(address => uint256) public redeemedHours;
    uint256 public totalRedeemedHours;
    
    // Events
    event ComputeRedeemed(address indexed redeemer, uint256 hoursRedeemed, uint256 tokensUsed);
    event MinFractionSizeUpdated(uint256 newSize);
    
    constructor(
        string memory _name,
        string memory _symbol,
        uint256 _totalSupply,
        uint256 _totalComputeHours,
        uint256 _minFractionSize,
        string memory _offeringId,
        string memory _resourceType,
        string memory _provider
    ) ERC20(_name, _symbol) ERC20Permit(_name) {
        offeringId = _offeringId;
        resourceType = _resourceType;
        provider = _provider;
        totalComputeHours = _totalComputeHours;
        minFractionSize = _minFractionSize;
        
        _mint(msg.sender, _totalSupply);
    }
    
    /**
     * @dev Redeem tokens for compute hours
     * @param amount Number of tokens to redeem
     */
    function redeemCompute(uint256 amount) external whenNotPaused {
        require(amount >= minFractionSize, "Amount below minimum fraction");
        require(balanceOf(msg.sender) >= amount, "Insufficient balance");
        
        uint256 hoursToRedeem = (amount * totalComputeHours) / totalSupply();
        require(totalRedeemedHours + hoursToRedeem <= totalComputeHours, "Exceeds available hours");
        
        // Burn the tokens
        _burn(msg.sender, amount);
        
        // Track redemption
        redeemedHours[msg.sender] += hoursToRedeem;
        totalRedeemedHours += hoursToRedeem;
        
        emit ComputeRedeemed(msg.sender, hoursToRedeem, amount);
    }
    
    /**
     * @dev Update minimum fraction size
     * @param newSize New minimum fraction size
     */
    function updateMinFractionSize(uint256 newSize) external onlyOwner {
        minFractionSize = newSize;
        emit MinFractionSizeUpdated(newSize);
    }
    
    /**
     * @dev Create snapshot for governance
     */
    function snapshot() external onlyOwner {
        _snapshot();
    }
    
    /**
     * @dev Pause token transfers
     */
    function pause() external onlyOwner {
        _pause();
    }
    
    /**
     * @dev Unpause token transfers
     */
    function unpause() external onlyOwner {
        _unpause();
    }
    
    /**
     * @dev Get remaining compute hours
     */
    function remainingComputeHours() external view returns (uint256) {
        return totalComputeHours - totalRedeemedHours;
    }
    
    /**
     * @dev Convert tokens to compute hours
     */
    function tokensToHours(uint256 tokenAmount) external view returns (uint256) {
        return (tokenAmount * totalComputeHours) / totalSupply();
    }
    
    /**
     * @dev Convert compute hours to tokens
     */
    function hoursToTokens(uint256 hours) external view returns (uint256) {
        return (hours * totalSupply()) / totalComputeHours;
    }
    
    // Override required functions
    function _beforeTokenTransfer(address from, address to, uint256 amount)
        internal
        whenNotPaused
        override(ERC20, ERC20Snapshot)
    {
        super._beforeTokenTransfer(from, to, amount);
    }
}