// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/access/Ownable.sol";

/**
 * @title Treasury
 * @dev A simple treasury contract for holding and disbursing funds.
 * The owner of this contract will be the Governor contract.
 */
contract Treasury is Ownable {
    event FundsDeposited(address indexed from, uint256 amount);
    event TransactionExecuted(address indexed target, uint256 value, bytes data);

    constructor() {
        // The deployer is the initial owner, but ownership will be transferred
        // to the Governor contract during deployment.
    }

    /**
     * @dev Executes a transaction from the treasury.
     * Can only be called by the owner (the Governor contract).
     * @param target The address to call.
     * @param value The amount of ETH to send.
     * @param data The data to include in the call.
     */
    function executeTransaction(address target, uint256 value, bytes calldata data)
        external
        onlyOwner
    {
        (bool success, ) = target.call{value: value}(data);
        require(success, "Treasury: Transaction failed");
        emit TransactionExecuted(target, value, data);
    }

    /**
     * @dev Receive function to accept ETH deposits.
     */
    receive() external payable {
        emit FundsDeposited(msg.sender, msg.value);
    }

    /**
     * @dev Fallback function to accept ETH deposits.
     */
    fallback() external payable {
        emit FundsDeposited(msg.sender, msg.value);
    }
} 