// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC721/IERC721.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract NFTLending is ReentrancyGuard, Ownable {
    using SafeMath for uint256;
    
    struct Loan {
        uint256 loanId;
        address borrower;
        address lender;
        address nftContract;
        uint256 tokenId;
        uint256 principal;
        uint256 interest;
        uint256 duration;
        uint256 startTime;
        uint256 endTime;
        LoanStatus status;
        address paymentToken;
    }
    
    struct LoanOffer {
        uint256 offerId;
        address lender;
        address nftContract;
        uint256 maxLoanAmount;
        uint256 interestRate; // Basis points (10000 = 100%)
        uint256 minDuration;
        uint256 maxDuration;
        address paymentToken;
        bool isActive;
    }
    
    enum LoanStatus {
        Active,
        Repaid,
        Defaulted,
        Liquidated
    }
    
    mapping(uint256 => Loan) public loans;
    mapping(uint256 => LoanOffer) public loanOffers;
    mapping(address => mapping(uint256 => uint256)) public nftCollateral; // nftContract => tokenId => loanId
    mapping(address => uint256[]) public borrowerLoans;
    mapping(address => uint256[]) public lenderLoans;
    
    uint256 public nextLoanId;
    uint256 public nextOfferId;
    uint256 public platformFeePercentage = 250; // 2.5%
    uint256 public liquidationThreshold = 12000; // 120% of loan value
    
    // Supported payment tokens
    mapping(address => bool) public supportedTokens;
    
    event LoanOfferCreated(uint256 indexed offerId, address indexed lender);
    event LoanCreated(uint256 indexed loanId, address indexed borrower, address indexed lender);
    event LoanRepaid(uint256 indexed loanId, uint256 totalRepaid);
    event LoanDefaulted(uint256 indexed loanId);
    event LoanLiquidated(uint256 indexed loanId, address liquidator);
    
    constructor() {
        // Add default supported tokens (e.g., USDC, DAI)
    }
    
    function createLoanOffer(
        address nftContract,
        uint256 maxLoanAmount,
        uint256 interestRate,
        uint256 minDuration,
        uint256 maxDuration,
        address paymentToken
    ) external nonReentrant {
        require(supportedTokens[paymentToken], "Token not supported");
        require(maxLoanAmount > 0, "Invalid loan amount");
        require(interestRate > 0 && interestRate <= 10000, "Invalid interest rate");
        require(minDuration > 0 && minDuration <= maxDuration, "Invalid duration");
        
        uint256 offerId = nextOfferId++;
        
        loanOffers[offerId] = LoanOffer({
            offerId: offerId,
            lender: msg.sender,
            nftContract: nftContract,
            maxLoanAmount: maxLoanAmount,
            interestRate: interestRate,
            minDuration: minDuration,
            maxDuration: maxDuration,
            paymentToken: paymentToken,
            isActive: true
        });
        
        emit LoanOfferCreated(offerId, msg.sender);
    }
    
    function borrowAgainstNFT(
        uint256 offerId,
        uint256 tokenId,
        uint256 loanAmount,
        uint256 duration
    ) external nonReentrant {
        LoanOffer memory offer = loanOffers[offerId];
        require(offer.isActive, "Offer not active");
        require(loanAmount <= offer.maxLoanAmount, "Amount exceeds max");
        require(duration >= offer.minDuration && duration <= offer.maxDuration, "Invalid duration");
        
        // Transfer NFT to contract as collateral
        IERC721(offer.nftContract).transferFrom(msg.sender, address(this), tokenId);
        
        // Calculate interest
        uint256 interest = loanAmount.mul(offer.interestRate).mul(duration).div(10000).div(365 days);
        
        uint256 loanId = nextLoanId++;
        
        loans[loanId] = Loan({
            loanId: loanId,
            borrower: msg.sender,
            lender: offer.lender,
            nftContract: offer.nftContract,
            tokenId: tokenId,
            principal: loanAmount,
            interest: interest,
            duration: duration,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            status: LoanStatus.Active,
            paymentToken: offer.paymentToken
        });
        
        nftCollateral[offer.nftContract][tokenId] = loanId;
        borrowerLoans[msg.sender].push(loanId);
        lenderLoans[offer.lender].push(loanId);
        
        // Transfer loan amount to borrower
        IERC20(offer.paymentToken).transferFrom(offer.lender, msg.sender, loanAmount);
        
        emit LoanCreated(loanId, msg.sender, offer.lender);
    }
    
    function createDirectLoan(
        address borrower,
        address nftContract,
        uint256 tokenId,
        uint256 loanAmount,
        uint256 interestRate,
        uint256 duration,
        address paymentToken
    ) external nonReentrant {
        require(supportedTokens[paymentToken], "Token not supported");
        require(loanAmount > 0, "Invalid loan amount");
        require(duration > 0, "Invalid duration");
        
        // Transfer NFT from borrower to contract
        IERC721(nftContract).transferFrom(borrower, address(this), tokenId);
        
        // Calculate interest
        uint256 interest = loanAmount.mul(interestRate).mul(duration).div(10000).div(365 days);
        
        uint256 loanId = nextLoanId++;
        
        loans[loanId] = Loan({
            loanId: loanId,
            borrower: borrower,
            lender: msg.sender,
            nftContract: nftContract,
            tokenId: tokenId,
            principal: loanAmount,
            interest: interest,
            duration: duration,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            status: LoanStatus.Active,
            paymentToken: paymentToken
        });
        
        nftCollateral[nftContract][tokenId] = loanId;
        borrowerLoans[borrower].push(loanId);
        lenderLoans[msg.sender].push(loanId);
        
        // Transfer loan amount to borrower
        IERC20(paymentToken).transferFrom(msg.sender, borrower, loanAmount);
        
        emit LoanCreated(loanId, borrower, msg.sender);
    }
    
    function repayLoan(uint256 loanId) external nonReentrant {
        Loan storage loan = loans[loanId];
        require(loan.status == LoanStatus.Active, "Loan not active");
        require(msg.sender == loan.borrower, "Not borrower");
        
        uint256 totalRepayment = loan.principal.add(loan.interest);
        
        // Calculate platform fee
        uint256 platformFee = loan.interest.mul(platformFeePercentage).div(10000);
        uint256 lenderAmount = totalRepayment.sub(platformFee);
        
        // Transfer repayment
        IERC20(loan.paymentToken).transferFrom(msg.sender, loan.lender, lenderAmount);
        if (platformFee > 0) {
            IERC20(loan.paymentToken).transferFrom(msg.sender, owner(), platformFee);
        }
        
        // Return NFT to borrower
        IERC721(loan.nftContract).transferFrom(address(this), loan.borrower, loan.tokenId);
        
        loan.status = LoanStatus.Repaid;
        delete nftCollateral[loan.nftContract][loan.tokenId];
        
        emit LoanRepaid(loanId, totalRepayment);
    }
    
    function liquidateLoan(uint256 loanId) external nonReentrant {
        Loan storage loan = loans[loanId];
        require(loan.status == LoanStatus.Active, "Loan not active");
        require(block.timestamp > loan.endTime, "Loan not expired");
        
        loan.status = LoanStatus.Defaulted;
        
        // Lender can claim the NFT
        IERC721(loan.nftContract).transferFrom(address(this), loan.lender, loan.tokenId);
        
        delete nftCollateral[loan.nftContract][loan.tokenId];
        
        emit LoanDefaulted(loanId);
    }
    
    function extendLoan(uint256 loanId, uint256 additionalDuration) external nonReentrant {
        Loan storage loan = loans[loanId];
        require(loan.status == LoanStatus.Active, "Loan not active");
        require(msg.sender == loan.borrower, "Not borrower");
        require(block.timestamp < loan.endTime, "Loan expired");
        
        // Calculate additional interest
        uint256 additionalInterest = loan.principal.mul(loan.interest).mul(additionalDuration)
            .div(loan.duration).div(loan.principal);
        
        // Pay additional interest upfront
        IERC20(loan.paymentToken).transferFrom(msg.sender, loan.lender, additionalInterest);
        
        loan.interest = loan.interest.add(additionalInterest);
        loan.duration = loan.duration.add(additionalDuration);
        loan.endTime = loan.endTime.add(additionalDuration);
    }
    
    function getLoanDetails(uint256 loanId) external view returns (
        address borrower,
        address lender,
        address nftContract,
        uint256 tokenId,
        uint256 principal,
        uint256 interest,
        uint256 dueDate,
        LoanStatus status
    ) {
        Loan memory loan = loans[loanId];
        return (
            loan.borrower,
            loan.lender,
            loan.nftContract,
            loan.tokenId,
            loan.principal,
            loan.interest,
            loan.endTime,
            loan.status
        );
    }
    
    function getBorrowerLoans(address borrower) external view returns (uint256[] memory) {
        return borrowerLoans[borrower];
    }
    
    function getLenderLoans(address lender) external view returns (uint256[] memory) {
        return lenderLoans[lender];
    }
    
    function addSupportedToken(address token) external onlyOwner {
        supportedTokens[token] = true;
    }
    
    function removeSupportedToken(address token) external onlyOwner {
        supportedTokens[token] = false;
    }
    
    function setPlatformFee(uint256 newFee) external onlyOwner {
        require(newFee <= 1000, "Fee too high"); // Max 10%
        platformFeePercentage = newFee;
    }
    
    function emergencyWithdrawNFT(address nftContract, uint256 tokenId) external onlyOwner {
        // Only for emergencies when loan data is corrupted
        require(nftCollateral[nftContract][tokenId] == 0, "NFT has active loan");
        IERC721(nftContract).transferFrom(address(this), owner(), tokenId);
    }
} 