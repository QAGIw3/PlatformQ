// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "./CredentialRegistry.sol";

/**
 * @title CredentialVerifier
 * @dev Automated verification of credentials with configurable rules
 */
contract CredentialVerifier {
    CredentialRegistry public registry;
    
    // Verification rules
    struct VerificationRule {
        bool checkExpiry;
        uint256 maxAge; // Maximum age in seconds
        bool checkIssuer;
        address[] allowedIssuers;
        bool checkTenant;
        string[] allowedTenants;
        bool active;
    }
    
    // Verification result
    struct VerificationResult {
        bool valid;
        string reason;
        uint256 timestamp;
        address verifier;
    }
    
    // Mapping from rule ID to verification rule
    mapping(string => VerificationRule) public rules;
    
    // Mapping from credential ID to verification results
    mapping(string => VerificationResult[]) public verificationHistory;
    
    // Access control
    mapping(address => bool) public verifiers;
    address public owner;
    
    // Events
    event RuleCreated(string indexed ruleId, address creator);
    event RuleUpdated(string indexed ruleId, address updater);
    event CredentialVerified(
        string indexed credentialId,
        string indexed ruleId,
        bool valid,
        string reason
    );
    
    modifier onlyOwner() {
        require(msg.sender == owner, "Only owner can perform this action");
        _;
    }
    
    modifier onlyVerifier() {
        require(verifiers[msg.sender] || msg.sender == owner, "Not authorized");
        _;
    }
    
    constructor(address _registry) {
        registry = CredentialRegistry(_registry);
        owner = msg.sender;
        verifiers[msg.sender] = true;
    }
    
    /**
     * @dev Add or remove a verifier
     */
    function setVerifier(address verifier, bool status) public onlyOwner {
        verifiers[verifier] = status;
    }
    
    /**
     * @dev Create a new verification rule
     */
    function createRule(
        string memory ruleId,
        bool checkExpiry,
        uint256 maxAge,
        bool checkIssuer,
        address[] memory allowedIssuers,
        bool checkTenant,
        string[] memory allowedTenants
    ) public onlyVerifier {
        rules[ruleId] = VerificationRule({
            checkExpiry: checkExpiry,
            maxAge: maxAge,
            checkIssuer: checkIssuer,
            allowedIssuers: allowedIssuers,
            checkTenant: checkTenant,
            allowedTenants: allowedTenants,
            active: true
        });
        
        emit RuleCreated(ruleId, msg.sender);
    }
    
    /**
     * @dev Verify a credential against a rule
     */
    function verifyCredential(
        string memory credentialId,
        string memory ruleId
    ) public onlyVerifier returns (bool valid, string memory reason) {
        VerificationRule memory rule = rules[ruleId];
        require(rule.active, "Rule not active");
        
        // Get credential from registry
        (
            string memory credentialHash,
            uint256 timestamp,
            address issuer,
            bool revoked,
            
        ) = registry.getCredential(credentialId);
        
        // Check if credential exists
        if (bytes(credentialHash).length == 0) {
            return _recordResult(credentialId, false, "Credential not found");
        }
        
        // Check if revoked
        if (revoked) {
            return _recordResult(credentialId, false, "Credential revoked");
        }
        
        // Check expiry
        if (rule.checkExpiry) {
            if (block.timestamp > timestamp + rule.maxAge) {
                return _recordResult(credentialId, false, "Credential expired");
            }
        }
        
        // Check issuer
        if (rule.checkIssuer) {
            bool issuerAllowed = false;
            for (uint i = 0; i < rule.allowedIssuers.length; i++) {
                if (rule.allowedIssuers[i] == issuer) {
                    issuerAllowed = true;
                    break;
                }
            }
            if (!issuerAllowed) {
                return _recordResult(credentialId, false, "Issuer not allowed");
            }
        }
        
        // Check tenant
        if (rule.checkTenant) {
            (,,, string memory tenantId,,,) = _getFullCredential(credentialId);
            bool tenantAllowed = false;
            for (uint i = 0; i < rule.allowedTenants.length; i++) {
                if (keccak256(bytes(rule.allowedTenants[i])) == keccak256(bytes(tenantId))) {
                    tenantAllowed = true;
                    break;
                }
            }
            if (!tenantAllowed) {
                return _recordResult(credentialId, false, "Tenant not allowed");
            }
        }
        
        return _recordResult(credentialId, true, "Verification passed");
    }
    
    /**
     * @dev Batch verify multiple credentials
     */
    function batchVerify(
        string[] memory credentialIds,
        string memory ruleId
    ) public onlyVerifier returns (bool[] memory results) {
        results = new bool[](credentialIds.length);
        
        for (uint i = 0; i < credentialIds.length; i++) {
            (bool valid,) = verifyCredential(credentialIds[i], ruleId);
            results[i] = valid;
        }
        
        return results;
    }
    
    /**
     * @dev Get verification history for a credential
     */
    function getVerificationHistory(string memory credentialId)
        public
        view
        returns (VerificationResult[] memory)
    {
        return verificationHistory[credentialId];
    }
    
    /**
     * @dev Get the last verification result
     */
    function getLastVerification(string memory credentialId)
        public
        view
        returns (bool valid, string memory reason, uint256 timestamp)
    {
        VerificationResult[] memory history = verificationHistory[credentialId];
        if (history.length == 0) {
            return (false, "No verification history", 0);
        }
        
        VerificationResult memory last = history[history.length - 1];
        return (last.valid, last.reason, last.timestamp);
    }
    
    /**
     * @dev Update a verification rule
     */
    function updateRule(
        string memory ruleId,
        bool active
    ) public onlyVerifier {
        rules[ruleId].active = active;
        emit RuleUpdated(ruleId, msg.sender);
    }
    
    /**
     * @dev Internal function to record verification result
     */
    function _recordResult(
        string memory credentialId,
        bool valid,
        string memory reason
    ) internal returns (bool, string memory) {
        verificationHistory[credentialId].push(VerificationResult({
            valid: valid,
            reason: reason,
            timestamp: block.timestamp,
            verifier: msg.sender
        }));
        
        emit CredentialVerified(credentialId, "", valid, reason);
        return (valid, reason);
    }
    
    /**
     * @dev Get full credential details (internal helper)
     */
    function _getFullCredential(string memory credentialId)
        internal
        view
        returns (
            string memory hash,
            uint256 timestamp,
            address issuer,
            string memory tenantId,
            bool revoked,
            string memory revocationReason,
            uint256 revocationTimestamp
        )
    {
        // This would need to be implemented based on the actual registry structure
        // For now, returning placeholder
        return ("", 0, address(0), "", false, "", 0);
    }
} 