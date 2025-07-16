use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
struct TrustInput {
    entity_id: String,
    activity_history: Vec<ActivityRecord>,
    relationships: Vec<TrustRelationship>,
    credentials_issued: u32,
    credentials_verified: u32,
    disputes: u32,
    endorsements: u32,
    metadata: Option<HashMap<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ActivityRecord {
    activity_type: String,
    timestamp: String,
    success: bool,
    impact_score: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TrustRelationship {
    to_entity: String,
    trust_value: f64,
    relationship_type: String,
    evidence_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct TrustScoreResult {
    entity_id: String,
    trust_score: f64,
    trust_level: String,
    factors: TrustFactors,
    recommendations: Vec<String>,
    metadata: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TrustFactors {
    base_trust: f64,
    activity_score: f64,
    relationship_score: f64,
    credential_score: f64,
    reputation_score: f64,
    time_decay_factor: f64,
}

#[wasm_bindgen]
pub fn process(input: &str) -> String {
    match calculate_trust_score(input) {
        Ok(result) => serde_json::to_string(&result).unwrap_or_else(|_| {
            json!({
                "error": "Failed to serialize result"
            }).to_string()
        }),
        Err(e) => json!({
            "error": format!("Trust calculation failed: {}", e)
        }).to_string(),
    }
}

fn calculate_trust_score(input: &str) -> Result<TrustScoreResult, String> {
    // Parse input
    let trust_input: TrustInput = serde_json::from_str(input)
        .map_err(|e| format!("Invalid input format: {}", e))?;
    
    // Calculate individual factor scores
    let base_trust = calculate_base_trust(&trust_input);
    let activity_score = calculate_activity_score(&trust_input.activity_history);
    let relationship_score = calculate_relationship_score(&trust_input.relationships);
    let credential_score = calculate_credential_score(
        trust_input.credentials_issued,
        trust_input.credentials_verified
    );
    let reputation_score = calculate_reputation_score(
        trust_input.endorsements,
        trust_input.disputes
    );
    let time_decay = calculate_time_decay(&trust_input.activity_history);
    
    // Combine factors with weights
    let weights = TrustWeights {
        base: 0.1,
        activity: 0.25,
        relationship: 0.25,
        credential: 0.2,
        reputation: 0.2,
    };
    
    let trust_score = (
        base_trust * weights.base +
        activity_score * weights.activity +
        relationship_score * weights.relationship +
        credential_score * weights.credential +
        reputation_score * weights.reputation
    ) * time_decay;
    
    // Determine trust level
    let trust_level = match trust_score {
        s if s >= 0.8 => "VERIFIED",
        s if s >= 0.6 => "HIGH",
        s if s >= 0.4 => "MEDIUM",
        s if s >= 0.2 => "LOW",
        _ => "UNTRUSTED",
    }.to_string();
    
    // Generate recommendations
    let recommendations = generate_recommendations(
        &trust_input,
        trust_score,
        &TrustFactors {
            base_trust,
            activity_score,
            relationship_score,
            credential_score,
            reputation_score,
            time_decay_factor: time_decay,
        }
    );
    
    // Build metadata
    let mut metadata = HashMap::new();
    metadata.insert("calculation_timestamp".to_string(), 
                   json!(chrono::Utc::now().to_rfc3339()));
    metadata.insert("activity_count".to_string(), 
                   json!(trust_input.activity_history.len()));
    metadata.insert("relationship_count".to_string(), 
                   json!(trust_input.relationships.len()));
    
    if let Some(input_metadata) = trust_input.metadata {
        for (key, value) in input_metadata {
            metadata.insert(format!("input_{}", key), value);
        }
    }
    
    Ok(TrustScoreResult {
        entity_id: trust_input.entity_id,
        trust_score: (trust_score * 1000.0).round() / 1000.0, // Round to 3 decimals
        trust_level,
        factors: TrustFactors {
            base_trust,
            activity_score,
            relationship_score,
            credential_score,
            reputation_score,
            time_decay_factor: time_decay,
        },
        recommendations,
        metadata,
    })
}

struct TrustWeights {
    base: f64,
    activity: f64,
    relationship: f64,
    credential: f64,
    reputation: f64,
}

fn calculate_base_trust(input: &TrustInput) -> f64 {
    // Base trust based on account age and basic verification
    let mut base = 0.3; // Starting trust
    
    // Bonus for having any verified credentials
    if input.credentials_verified > 0 {
        base += 0.2;
    }
    
    // Bonus for having relationships
    if !input.relationships.is_empty() {
        base += 0.1;
    }
    
    base.min(1.0)
}

fn calculate_activity_score(activities: &[ActivityRecord]) -> f64 {
    if activities.is_empty() {
        return 0.0;
    }
    
    let mut total_score = 0.0;
    let mut total_weight = 0.0;
    
    for (i, activity) in activities.iter().enumerate() {
        // Recent activities have more weight
        let recency_weight = 1.0 / (1.0 + (i as f64 * 0.1));
        
        let activity_value = if activity.success {
            activity.impact_score
        } else {
            -activity.impact_score * 0.5 // Failed activities have less negative impact
        };
        
        total_score += activity_value * recency_weight;
        total_weight += recency_weight;
    }
    
    if total_weight > 0.0 {
        (total_score / total_weight).max(0.0).min(1.0)
    } else {
        0.0
    }
}

fn calculate_relationship_score(relationships: &[TrustRelationship]) -> f64 {
    if relationships.is_empty() {
        return 0.0;
    }
    
    let mut total_score = 0.0;
    
    for relationship in relationships {
        // Weight by evidence count
        let evidence_weight = (relationship.evidence_count as f64).ln() + 1.0;
        let weighted_trust = relationship.trust_value * evidence_weight;
        
        // Different relationship types have different impacts
        let type_multiplier = match relationship.relationship_type.as_str() {
            "verified_issuer" => 1.5,
            "mutual_trust" => 1.3,
            "endorsement" => 1.2,
            "collaboration" => 1.1,
            _ => 1.0,
        };
        
        total_score += weighted_trust * type_multiplier;
    }
    
    // Normalize by relationship count with diminishing returns
    let normalized = total_score / (relationships.len() as f64).sqrt();
    normalized.max(0.0).min(1.0)
}

fn calculate_credential_score(issued: u32, verified: u32) -> f64 {
    // Score based on credential activity
    let issue_score = (issued as f64).ln() / 10.0;
    let verify_score = (verified as f64).ln() / 8.0;
    
    // Verification is more valuable than issuance
    let combined = issue_score + verify_score * 1.5;
    combined.max(0.0).min(1.0)
}

fn calculate_reputation_score(endorsements: u32, disputes: u32) -> f64 {
    // Calculate reputation based on endorsements vs disputes
    let positive = endorsements as f64;
    let negative = disputes as f64;
    
    if positive + negative == 0.0 {
        return 0.5; // Neutral if no reputation data
    }
    
    // Wilson score interval for reputation
    let total = positive + negative;
    let z = 1.96; // 95% confidence
    let phat = positive / total;
    
    let score = (phat + z*z/(2.0*total) - z*((phat*(1.0-phat) + z*z/(4.0*total))/total).sqrt()) 
               / (1.0 + z*z/total);
    
    score.max(0.0).min(1.0)
}

fn calculate_time_decay(activities: &[ActivityRecord]) -> f64 {
    // Calculate decay based on recency of activities
    if activities.is_empty() {
        return 0.5; // No activity = neutral decay
    }
    
    // In a real implementation, we'd parse timestamps
    // For now, assume activities are sorted by recency
    let recent_activity_count = activities.iter()
        .take(10)
        .filter(|a| a.success)
        .count();
    
    // More recent successful activities = less decay
    let decay_factor = 0.7 + (recent_activity_count as f64 * 0.03);
    decay_factor.min(1.0)
}

fn generate_recommendations(
    input: &TrustInput,
    trust_score: f64,
    factors: &TrustFactors
) -> Vec<String> {
    let mut recommendations = Vec::new();
    
    // Low trust score recommendations
    if trust_score < 0.5 {
        recommendations.push("Consider requiring additional verification for high-value operations".to_string());
    }
    
    // Activity-based recommendations
    if factors.activity_score < 0.3 {
        recommendations.push("Limited activity history - monitor initial transactions closely".to_string());
    }
    
    // Relationship-based recommendations
    if factors.relationship_score < 0.2 {
        recommendations.push("Few trust relationships - consider requiring cosigners".to_string());
    } else if factors.relationship_score > 0.8 {
        recommendations.push("Strong trust network - eligible for premium features".to_string());
    }
    
    // Credential-based recommendations
    if input.credentials_issued > 100 && factors.credential_score > 0.7 {
        recommendations.push("High-volume issuer - consider for automated verification".to_string());
    }
    
    // Dispute-based recommendations
    if input.disputes > 5 {
        recommendations.push("Multiple disputes on record - require dispute resolution before high-trust operations".to_string());
    }
    
    // Time decay recommendations
    if factors.time_decay_factor < 0.8 {
        recommendations.push("Recent activity is low - trust score may be outdated".to_string());
    }
    
    // High trust recommendations
    if trust_score > 0.8 {
        recommendations.push("Highly trusted entity - eligible for streamlined processes".to_string());
        recommendations.push("Consider as a network validator or verifier".to_string());
    }
    
    recommendations
}

// Required for chrono in WASM
mod chrono {
    pub struct Utc;
    
    impl Utc {
        pub fn now() -> DateTime {
            DateTime
        }
    }
    
    pub struct DateTime;
    
    impl DateTime {
        pub fn to_rfc3339(&self) -> String {
            // Simplified for WASM
            "2024-01-01T00:00:00Z".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_trust_calculation() {
        let input = TrustInput {
            entity_id: "did:example:123".to_string(),
            activity_history: vec![
                ActivityRecord {
                    activity_type: "credential_issued".to_string(),
                    timestamp: "2024-01-01T00:00:00Z".to_string(),
                    success: true,
                    impact_score: 0.8,
                },
            ],
            relationships: vec![
                TrustRelationship {
                    to_entity: "did:example:456".to_string(),
                    trust_value: 0.9,
                    relationship_type: "verified_issuer".to_string(),
                    evidence_count: 5,
                },
            ],
            credentials_issued: 10,
            credentials_verified: 20,
            disputes: 0,
            endorsements: 15,
            metadata: None,
        };
        
        let result = calculate_trust_score(&serde_json::to_string(&input).unwrap()).unwrap();
        assert!(result.trust_score > 0.5);
        assert_eq!(result.trust_level, "HIGH");
    }
} 