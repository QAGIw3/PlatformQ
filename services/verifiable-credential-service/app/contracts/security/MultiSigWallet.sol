// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/**
 * @title MultiSigWallet
 * @dev Multi-signature wallet for high-value marketplace transactions
 */
contract MultiSigWallet is ReentrancyGuard {
    using ECDSA for bytes32;

    // Events
    event OwnerAdded(address indexed owner);
    event OwnerRemoved(address indexed owner);
    event RequirementChanged(uint256 required);
    event TransactionSubmitted(uint256 indexed txId, address indexed owner);
    event TransactionConfirmed(uint256 indexed txId, address indexed owner);
    event TransactionRevoked(uint256 indexed txId, address indexed owner);
    event TransactionExecuted(uint256 indexed txId);
    event TransactionFailed(uint256 indexed txId);
    event DepositReceived(address indexed from, uint256 value);
    
    // Transaction types
    enum TransactionType {
        TRANSFER,
        NFT_MINT,
        LICENSE_PURCHASE,
        ROYALTY_DISTRIBUTION,
        CONTRACT_CALL,
        OWNER_MANAGEMENT
    }
    
    // Transaction struct
    struct Transaction {
        address destination;
        uint256 value;
        bytes data;
        TransactionType txType;
        bool executed;
        uint256 confirmations;
        uint256 submittedAt;
        uint256 executedAt;
        string description;
    }
    
    // State variables
    mapping(uint256 => Transaction) public transactions;
    mapping(uint256 => mapping(address => bool)) public confirmations;
    mapping(address => bool) public isOwner;
    address[] public owners;
    uint256 public required;
    uint256 public transactionCount;
    
    // Configuration
    uint256 public constant MAX_OWNERS = 10;
    uint256 public constant TRANSACTION_TIMEOUT = 7 days;
    uint256 public immutable HIGH_VALUE_THRESHOLD;
    
    // Modifiers
    modifier onlyOwner() {
        require(isOwner[msg.sender], "Not an owner");
        _;
    }
    
    modifier ownerExists(address owner) {
        require(isOwner[owner], "Owner does not exist");
        _;
    }
    
    modifier ownerDoesNotExist(address owner) {
        require(!isOwner[owner], "Owner already exists");
        _;
    }
    
    modifier transactionExists(uint256 txId) {
        require(transactions[txId].destination != address(0), "Transaction does not exist");
        _;
    }
    
    modifier notExecuted(uint256 txId) {
        require(!transactions[txId].executed, "Transaction already executed");
        _;
    }
    
    modifier notConfirmed(uint256 txId) {
        require(!confirmations[txId][msg.sender], "Transaction already confirmed");
        _;
    }
    
    modifier validRequirement(uint256 ownerCount, uint256 _required) {
        require(
            ownerCount <= MAX_OWNERS &&
            _required <= ownerCount &&
            _required > 0 &&
            ownerCount > 0,
            "Invalid requirement"
        );
        _;
    }
    
    /**
     * @dev Constructor
     * @param _owners Initial owners
     * @param _required Number of confirmations required
     * @param _highValueThreshold Threshold for high-value transactions
     */
    constructor(
        address[] memory _owners,
        uint256 _required,
        uint256 _highValueThreshold
    ) validRequirement(_owners.length, _required) {
        for (uint256 i = 0; i < _owners.length; i++) {
            require(_owners[i] != address(0), "Invalid owner");
            require(!isOwner[_owners[i]], "Duplicate owner");
            isOwner[_owners[i]] = true;
        }
        owners = _owners;
        required = _required;
        HIGH_VALUE_THRESHOLD = _highValueThreshold;
    }
    
    /**
     * @dev Receive function to accept ETH
     */
    receive() external payable {
        emit DepositReceived(msg.sender, msg.value);
    }
    
    /**
     * @dev Submit a new transaction
     * @param destination Target address
     * @param value ETH value to send
     * @param data Transaction data
     * @param txType Type of transaction
     * @param description Human-readable description
     */
    function submitTransaction(
        address destination,
        uint256 value,
        bytes memory data,
        TransactionType txType,
        string memory description
    ) public onlyOwner returns (uint256 txId) {
        require(destination != address(0), "Invalid destination");
        
        // Check if transaction requires multi-sig
        if (value < HIGH_VALUE_THRESHOLD && txType == TransactionType.TRANSFER) {
            // Direct execution for low-value transfers
            (bool success, ) = destination.call{value: value}(data);
            require(success, "Transfer failed");
            return type(uint256).max; // Special value indicating direct execution
        }
        
        txId = transactionCount++;
        transactions[txId] = Transaction({
            destination: destination,
            value: value,
            data: data,
            txType: txType,
            executed: false,
            confirmations: 0,
            submittedAt: block.timestamp,
            executedAt: 0,
            description: description
        });
        
        emit TransactionSubmitted(txId, msg.sender);
        
        // Auto-confirm from submitter
        confirmTransaction(txId);
    }
    
    /**
     * @dev Confirm a transaction
     * @param txId Transaction ID
     */
    function confirmTransaction(uint256 txId)
        public
        onlyOwner
        transactionExists(txId)
        notExecuted(txId)
        notConfirmed(txId)
    {
        // Check timeout
        require(
            block.timestamp <= transactions[txId].submittedAt + TRANSACTION_TIMEOUT,
            "Transaction expired"
        );
        
        confirmations[txId][msg.sender] = true;
        transactions[txId].confirmations++;
        
        emit TransactionConfirmed(txId, msg.sender);
        
        // Auto-execute if threshold reached
        if (transactions[txId].confirmations >= required) {
            executeTransaction(txId);
        }
    }
    
    /**
     * @dev Revoke a confirmation
     * @param txId Transaction ID
     */
    function revokeConfirmation(uint256 txId)
        public
        onlyOwner
        transactionExists(txId)
        notExecuted(txId)
    {
        require(confirmations[txId][msg.sender], "Not confirmed");
        
        confirmations[txId][msg.sender] = false;
        transactions[txId].confirmations--;
        
        emit TransactionRevoked(txId, msg.sender);
    }
    
    /**
     * @dev Execute a confirmed transaction
     * @param txId Transaction ID
     */
    function executeTransaction(uint256 txId)
        public
        onlyOwner
        transactionExists(txId)
        notExecuted(txId)
        nonReentrant
    {
        Transaction storage txn = transactions[txId];
        
        require(txn.confirmations >= required, "Insufficient confirmations");
        require(
            block.timestamp <= txn.submittedAt + TRANSACTION_TIMEOUT,
            "Transaction expired"
        );
        
        txn.executed = true;
        txn.executedAt = block.timestamp;
        
        (bool success, ) = txn.destination.call{value: txn.value}(txn.data);
        
        if (success) {
            emit TransactionExecuted(txId);
        } else {
            txn.executed = false; // Revert execution status
            emit TransactionFailed(txId);
            revert("Transaction execution failed");
        }
    }
    
    /**
     * @dev Add a new owner
     * @param owner New owner address
     */
    function addOwner(address owner)
        public
        onlyOwner
        ownerDoesNotExist(owner)
        validRequirement(owners.length + 1, required)
    {
        isOwner[owner] = true;
        owners.push(owner);
        emit OwnerAdded(owner);
    }
    
    /**
     * @dev Remove an owner
     * @param owner Owner to remove
     */
    function removeOwner(address owner)
        public
        onlyOwner
        ownerExists(owner)
    {
        require(owners.length > required, "Cannot remove: would break requirement");
        
        isOwner[owner] = false;
        for (uint256 i = 0; i < owners.length; i++) {
            if (owners[i] == owner) {
                owners[i] = owners[owners.length - 1];
                owners.pop();
                break;
            }
        }
        
        emit OwnerRemoved(owner);
    }
    
    /**
     * @dev Change the number of required confirmations
     * @param _required New requirement
     */
    function changeRequirement(uint256 _required)
        public
        onlyOwner
        validRequirement(owners.length, _required)
    {
        required = _required;
        emit RequirementChanged(_required);
    }
    
    /**
     * @dev Get pending transactions
     * @return pendingTxIds Array of pending transaction IDs
     */
    function getPendingTransactions() public view returns (uint256[] memory pendingTxIds) {
        uint256 count = 0;
        
        // Count pending transactions
        for (uint256 i = 0; i < transactionCount; i++) {
            if (!transactions[i].executed && 
                block.timestamp <= transactions[i].submittedAt + TRANSACTION_TIMEOUT) {
                count++;
            }
        }
        
        pendingTxIds = new uint256[](count);
        uint256 index = 0;
        
        // Populate array
        for (uint256 i = 0; i < transactionCount; i++) {
            if (!transactions[i].executed && 
                block.timestamp <= transactions[i].submittedAt + TRANSACTION_TIMEOUT) {
                pendingTxIds[index++] = i;
            }
        }
    }
    
    /**
     * @dev Get confirmation status for a transaction
     * @param txId Transaction ID
     * @return confirmedBy Array of addresses that confirmed
     */
    function getConfirmations(uint256 txId) 
        public 
        view 
        returns (address[] memory confirmedBy) 
    {
        uint256 count = 0;
        
        // Count confirmations
        for (uint256 i = 0; i < owners.length; i++) {
            if (confirmations[txId][owners[i]]) {
                count++;
            }
        }
        
        confirmedBy = new address[](count);
        uint256 index = 0;
        
        // Populate array
        for (uint256 i = 0; i < owners.length; i++) {
            if (confirmations[txId][owners[i]]) {
                confirmedBy[index++] = owners[i];
            }
        }
    }
    
    /**
     * @dev Check if transaction is confirmed by specific owner
     * @param txId Transaction ID
     * @param owner Owner address
     * @return True if confirmed
     */
    function isConfirmed(uint256 txId, address owner) public view returns (bool) {
        return confirmations[txId][owner];
    }
    
    /**
     * @dev Get transaction details
     * @param txId Transaction ID
     */
    function getTransaction(uint256 txId) 
        public 
        view 
        returns (
            address destination,
            uint256 value,
            bytes memory data,
            TransactionType txType,
            bool executed,
            uint256 confirmationCount,
            string memory description
        ) 
    {
        Transaction storage txn = transactions[txId];
        return (
            txn.destination,
            txn.value,
            txn.data,
            txn.txType,
            txn.executed,
            txn.confirmations,
            txn.description
        );
    }
    
    /**
     * @dev Get all owners
     * @return Array of owner addresses
     */
    function getOwners() public view returns (address[] memory) {
        return owners;
    }
    
    /**
     * @dev Emergency pause - requires all owners
     */
    function emergencyPause() public onlyOwner {
        uint256 confirmCount = 0;
        for (uint256 i = 0; i < owners.length; i++) {
            if (confirmations[type(uint256).max][owners[i]]) {
                confirmCount++;
            }
        }
        
        require(confirmCount == owners.length, "Requires all owners");
        
        // Implement emergency pause logic
        // This could pause connected contracts, disable functions, etc.
    }
} 