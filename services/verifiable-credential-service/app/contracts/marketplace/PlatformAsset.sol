// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/token/ERC721/extensions/ERC721URIStorage.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

contract PlatformAsset is ERC721, ERC721URIStorage, Ownable {
    using Counters for Counters.Counter;
    Counters.Counter private _tokenIdCounter;

    struct RoyaltyInfo {
        address recipient;
        uint256 royaltyFraction;
    }

    mapping(uint256 => RoyaltyInfo) private _royaltyInfo;

    constructor() ERC721("PlatformAsset", "PQA") {}

    function safeMint(address to, string memory uri, address royaltyRecipient, uint256 royaltyFraction)
        public
        onlyOwner
    {
        uint256 tokenId = _tokenIdCounter.current();
        _tokenIdCounter.increment();
        _safeMint(to, tokenId);
        _setTokenURI(tokenId, uri);
        _setRoyaltyInfo(tokenId, royaltyRecipient, royaltyFraction);
    }

    function _setRoyaltyInfo(uint256 tokenId, address recipient, uint256 fraction) internal {
        require(fraction <= 10000, "Royalty fraction cannot exceed 100%");
        _royaltyInfo[tokenId] = RoyaltyInfo(recipient, fraction);
    }

    function royaltyInfo(uint256 tokenId, uint256 salePrice)
        external
        view
        returns (address, uint256)
    {
        RoyaltyInfo memory info = _royaltyInfo[tokenId];
        return (info.recipient, (salePrice * info.royaltyFraction) / 10000);
    }

    // The following functions are overrides required by Solidity.

    function _burn(uint256 tokenId) internal override(ERC721, ERC721URIStorage) {
        super._burn(tokenId);
    }

    function _transfer(address from, address to, uint256 tokenId) internal override {
        (address recipient, uint256 royaltyAmount) = royaltyInfo(tokenId, 1);  // Assume sale price of 1 for simplicity
        if (royaltyAmount > 0) {
            // Call RoyaltyDistributor or handle distribution
        }
        super._transfer(from, to, tokenId);
    }


    function tokenURI(uint256 tokenId)
        public
        view
        override(ERC721, ERC721URIStorage)
        returns (string memory)
    {
        return super.tokenURI(tokenId);
    }
} 