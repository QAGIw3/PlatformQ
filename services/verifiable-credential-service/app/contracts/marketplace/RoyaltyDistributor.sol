// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/Ownable.sol";
import "./PlatformAsset.sol";

contract RoyaltyDistributor is Ownable {
    PlatformAsset private _platformAsset;
    mapping(address => uint256) public escrowedRoyalties;

    event RoyaltyPaid(
        uint256 indexed tokenId,
        address indexed recipient,
        uint256 amount
    );
    
    event RoyaltyEscrowed(
        address indexed recipient,
        uint256 amount
    );

    constructor(address platformAssetAddress) {
        _platformAsset = PlatformAsset(platformAssetAddress);
    }

    function distributeRoyalty(uint256 tokenId, uint256 salePrice) public payable {
        (address recipient, uint256 royaltyAmount) = _platformAsset.royaltyInfo(
            tokenId,
            salePrice
        );

        require(royaltyAmount > 0, "No royalty for this token");
        require(msg.value >= royaltyAmount, "Insufficient payment for royalty");

        (bool success, ) = recipient.call{value: royaltyAmount}("");
        
        if (success) {
            emit RoyaltyPaid(tokenId, recipient, royaltyAmount);
        } else {
            // Escrow the royalty for later withdrawal
            escrowedRoyalties[recipient] += royaltyAmount;
            emit RoyaltyEscrowed(recipient, royaltyAmount);
        }

        if (msg.value > royaltyAmount) {
            (success, ) = payable(msg.sender).call{
                value: msg.value - royaltyAmount
            }("");
            require(success, "Refund for overpayment failed");
        }
    }
    
    function withdrawEscrowedRoyalties() public {
        uint256 amount = escrowedRoyalties[msg.sender];
        require(amount > 0, "No escrowed royalties");
        
        escrowedRoyalties[msg.sender] = 0;
        
        (bool success, ) = msg.sender.call{value: amount}("");
        require(success, "Withdrawal failed");
    }
} 