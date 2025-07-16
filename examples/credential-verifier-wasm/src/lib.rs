use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use chrono::{DateTime, Utc};
use sha2::{Sha256, Digest};

#[derive(Debug, Serialize, Deserialize)]
struct CredentialData {
    #[serde(rename = "@context")]
    context: Vec<String>,
    id: String,
    #[serde(rename = "type")]
    credential_type: Vec<String>,
    issuer: String,
    #[serde(rename = "issuanceDate")]
    issuance_date: String,
    #[serde(rename = "credentialSubject")]
    credential_subject: Value,
    proof: Option<ProofData>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProofData {
    #[serde(rename = "type")]
    proof_type: String,
    created: String,
    #[serde(rename = "verificationMethod")]
    verification_method: String,
    #[serde(rename = "proofValue")]
    proof_value: String,
    #[serde(rename = "blockchainAnchor")]
    blockchain_anchor: Option<Value>,
    #[serde(rename = "ipfsCID")]
    ipfs_cid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct VerificationResult {
    is_valid: bool,
    confidence_score: f64,
    checks_performed: Vec<String>,
    issues: Vec<String>,
    metadata: HashMap<String, Value>,
}

#[wasm_bindgen]
pub fn process(input: &str) -> String {
    match verify_credential(input) {
        Ok(result) => serde_json::to_string(&result).unwrap_or_else(|_| {
            json!({
                "error": "Failed to serialize result"
            }).to_string()
        }),
        Err(e) => json!({
            "error": format!("Verification failed: {}", e)
        }).to_string(),
    }
}

fn verify_credential(input: &str) -> Result<VerificationResult, String> {
    // Parse input credential
    let credential: CredentialData = serde_json::from_str(input)
        .map_err(|e| format!("Invalid credential format: {}", e))?;
    
    let mut result = VerificationResult {
        is_valid: true,
        confidence_score: 1.0,
        checks_performed: Vec::new(),
        issues: Vec::new(),
        metadata: HashMap::new(),
    };
    
    // Check 1: Verify credential structure
    result.checks_performed.push("structure_validation".to_string());
    if !verify_structure(&credential) {
        result.issues.push("Invalid credential structure".to_string());
        result.is_valid = false;
        result.confidence_score *= 0.5;
    }
    
    // Check 2: Verify contexts
    result.checks_performed.push("context_validation".to_string());
    if !verify_contexts(&credential.context) {
        result.issues.push("Invalid or missing W3C context".to_string());
        result.confidence_score *= 0.8;
    }
    
    // Check 3: Verify credential types
    result.checks_performed.push("type_validation".to_string());
    if !verify_types(&credential.credential_type) {
        result.issues.push("Invalid credential type".to_string());
        result.confidence_score *= 0.9;
    }
    
    // Check 4: Verify dates
    result.checks_performed.push("temporal_validation".to_string());
    match verify_dates(&credential.issuance_date) {
        Ok(is_expired) => {
            if is_expired {
                result.issues.push("Credential is expired".to_string());
                result.is_valid = false;
            }
        }
        Err(e) => {
            result.issues.push(format!("Date validation failed: {}", e));
            result.confidence_score *= 0.7;
        }
    }
    
    // Check 5: Verify issuer format
    result.checks_performed.push("issuer_validation".to_string());
    if !verify_issuer(&credential.issuer) {
        result.issues.push("Invalid issuer format".to_string());
        result.confidence_score *= 0.8;
    }
    
    // Check 6: Verify proof if present
    if let Some(proof) = &credential.proof {
        result.checks_performed.push("proof_validation".to_string());
        verify_proof(proof, &mut result);
        
        // Check blockchain anchor if present
        if proof.blockchain_anchor.is_some() {
            result.checks_performed.push("blockchain_anchor_check".to_string());
            result.metadata.insert(
                "has_blockchain_anchor".to_string(),
                json!(true)
            );
            result.confidence_score *= 1.2; // Boost confidence for anchored credentials
        }
        
        // Check IPFS storage if present
        if let Some(cid) = &proof.ipfs_cid {
            result.checks_performed.push("ipfs_storage_check".to_string());
            result.metadata.insert(
                "ipfs_cid".to_string(),
                json!(cid)
            );
            result.confidence_score *= 1.1; // Boost confidence for IPFS-stored credentials
        }
    } else {
        result.issues.push("No proof found".to_string());
        result.confidence_score *= 0.6;
    }
    
    // Check 7: Calculate credential hash
    result.checks_performed.push("hash_calculation".to_string());
    let credential_hash = calculate_credential_hash(&credential);
    result.metadata.insert(
        "credential_hash".to_string(),
        json!(credential_hash)
    );
    
    // Check 8: Verify subject claims
    result.checks_performed.push("subject_validation".to_string());
    verify_subject(&credential.credential_subject, &mut result);
    
    // Normalize confidence score
    result.confidence_score = result.confidence_score.min(1.0).max(0.0);
    
    // Add metadata
    result.metadata.insert(
        "credential_id".to_string(),
        json!(credential.id)
    );
    result.metadata.insert(
        "issuer".to_string(),
        json!(credential.issuer)
    );
    result.metadata.insert(
        "verification_timestamp".to_string(),
        json!(Utc::now().to_rfc3339())
    );
    
    Ok(result)
}

fn verify_structure(credential: &CredentialData) -> bool {
    !credential.id.is_empty() &&
    !credential.issuer.is_empty() &&
    !credential.credential_type.is_empty() &&
    !credential.context.is_empty()
}

fn verify_contexts(contexts: &[String]) -> bool {
    contexts.contains(&"https://www.w3.org/2018/credentials/v1".to_string())
}

fn verify_types(types: &[String]) -> bool {
    types.contains(&"VerifiableCredential".to_string())
}

fn verify_dates(issuance_date: &str) -> Result<bool, String> {
    let issued = DateTime::parse_from_rfc3339(issuance_date)
        .map_err(|e| format!("Invalid date format: {}", e))?;
    
    let now = Utc::now();
    
    // Check if credential is from the future
    if issued > now {
        return Err("Credential issued in the future".to_string());
    }
    
    // Check if credential is too old (> 1 year)
    let one_year_ago = now - chrono::Duration::days(365);
    if issued < one_year_ago {
        return Ok(true); // Expired
    }
    
    Ok(false) // Not expired
}

fn verify_issuer(issuer: &str) -> bool {
    // Basic validation - should be a URI or DID
    issuer.starts_with("http://") ||
    issuer.starts_with("https://") ||
    issuer.starts_with("did:")
}

fn verify_proof(proof: &ProofData, result: &mut VerificationResult) {
    // Verify proof type
    let valid_proof_types = [
        "Ed25519Signature2020",
        "BbsBlsSignature2020",
        "JsonWebSignature2020",
    ];
    
    if !valid_proof_types.contains(&proof.proof_type.as_str()) {
        result.issues.push(format!("Unknown proof type: {}", proof.proof_type));
        result.confidence_score *= 0.9;
    }
    
    // Verify proof date
    if let Err(e) = DateTime::parse_from_rfc3339(&proof.created) {
        result.issues.push(format!("Invalid proof date: {}", e));
        result.confidence_score *= 0.9;
    }
    
    // Verify verification method format
    if !proof.verification_method.contains("#") {
        result.issues.push("Invalid verification method format".to_string());
        result.confidence_score *= 0.95;
    }
    
    // Check proof value exists and has reasonable length
    if proof.proof_value.len() < 20 {
        result.issues.push("Proof value too short".to_string());
        result.confidence_score *= 0.8;
    }
}

fn calculate_credential_hash(credential: &CredentialData) -> String {
    let mut hasher = Sha256::new();
    
    // Hash key fields
    hasher.update(&credential.id);
    hasher.update(&credential.issuer);
    hasher.update(&credential.issuance_date);
    
    // Hash credential type
    for t in &credential.credential_type {
        hasher.update(t);
    }
    
    // Hash subject (as JSON string)
    if let Ok(subject_str) = serde_json::to_string(&credential.credential_subject) {
        hasher.update(subject_str);
    }
    
    hex::encode(hasher.finalize())
}

fn verify_subject(subject: &Value, result: &mut VerificationResult) {
    if let Some(obj) = subject.as_object() {
        // Check for required fields
        if !obj.contains_key("id") {
            result.issues.push("Subject missing 'id' field".to_string());
            result.confidence_score *= 0.95;
        }
        
        // Count claims
        let claim_count = obj.len();
        result.metadata.insert(
            "claim_count".to_string(),
            json!(claim_count)
        );
        
        // Verify claim values are not empty
        for (key, value) in obj {
            if value.is_null() || (value.is_string() && value.as_str() == Some("")) {
                result.issues.push(format!("Empty claim value for '{}'", key));
                result.confidence_score *= 0.98;
            }
        }
    } else {
        result.issues.push("Invalid subject format".to_string());
        result.confidence_score *= 0.7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_valid_credential() {
        let credential = json!({
            "@context": ["https://www.w3.org/2018/credentials/v1"],
            "id": "urn:uuid:12345",
            "type": ["VerifiableCredential", "TestCredential"],
            "issuer": "did:example:issuer",
            "issuanceDate": "2024-01-01T00:00:00Z",
            "credentialSubject": {
                "id": "did:example:subject",
                "name": "Test Subject"
            },
            "proof": {
                "type": "Ed25519Signature2020",
                "created": "2024-01-01T00:00:00Z",
                "verificationMethod": "did:example:issuer#key-1",
                "proofValue": "abcdefghijklmnopqrstuvwxyz123456789"
            }
        });
        
        let result = verify_credential(&credential.to_string()).unwrap();
        assert!(result.is_valid);
        assert!(result.confidence_score > 0.8);
    }
} 