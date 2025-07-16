import React, { useState, useEffect } from 'react';
import './TrustPage.css';

// Mock API client function
const mockApi = {
  getUserCredentials: async (did) => {
    console.log(`Fetching credentials for DID: ${did}`);
    // This mocks the data structure returned from the search service
    return [
      {
        credential_id: "urn:uuid:1",
        type: "PeerReviewCredential",
        issuer: "did:web:platformq.com:tenant:default",
        issuance_date: new Date().toISOString(),
        credential_subject: {
          id: did,
          awardedFor: "AssetPeerReviewed",
          activity: "urn:platformq:asset_id:asset-abc-123"
        },
        proof_type: "Ed25519VerificationKey2020",
        blockchain_anchor: "0x123..."
      },
      {
        credential_id: "urn:uuid:2",
        type: "ProjectMilestoneCredential",
        issuer: "did:web:platformq.com:tenant:default",
        issuance_date: new Date().toISOString(),
        credential_subject: {
          id: did,
          awardedFor: "ProjectMilestoneCompleted",
          activity: "urn:platformq:milestone_id:milestone-def-456"
        },
        proof_type: "Ed25519VerificationKey2020",
        blockchain_anchor: "0x456..."
      },
       {
        credential_id: "urn:uuid:3",
        type: "DAOProposalApprovalCredential",
        issuer: "did:web:platformq.com:tenant:default",
        issuance_date: new Date().toISOString(),
        credential_subject: {
          id: did,
          awardedFor: "DAOProposalApproved",
          activity: "urn:platformq:proposal_id:proposal-ghi-789"
        },
        proof_type: "Ed25519VerificationKey2020",
        blockchain_anchor: "0x789..."
      }
    ];
  },
  getUserReputation: async (userId) => {
    console.log(`Fetching reputation for User ID: ${userId}`);
    // This mocks the data structure returned from the new endpoint
    return {
      user_id: userId,
      user_address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
      reputation_score: 135
    };
  }
};

const CredentialCard = ({ credential }) => {
  const { type, issuance_date, credential_subject, issuer } = credential;
  const friendlyName = type.replace("Credential", " Credential");

  return (
    <div className="credential-card">
      <div className="credential-card-header">
        <h3>{friendlyName}</h3>
      </div>
      <div className="credential-card-body">
        <p><strong>Issued on:</strong> {new Date(issuance_date).toLocaleDateString()}</p>
        <p><strong>Awarded for:</strong> {credential_subject.awardedFor}</p>
        <p><strong>Activity:</strong> <span className="code-text">{credential_subject.activity}</span></p>
        <p><strong>Issuer:</strong> <span className="code-text">{issuer}</span></p>
      </div>
    </div>
  );
};

const ReputationScore = ({ score }) => {
  let level = "Novice";
  let color = "#6c757d";

  if (score > 50) {
    level = "Contributor";
    color = "#007bff";
  }
  if (score > 100) {
    level = "Pillar";
    color = "#28a745";
  }
  if (score > 250) {
    level = "Vanguard";
    color = "#dc3545";
  }

  return (
    <div className="reputation-score-widget">
      <div className="score-display" style={{ borderColor: color }}>
        <span className="score-value">{score}</span>
        <span className="score-label">Reputation</span>
      </div>
      <div className="score-level" style={{ backgroundColor: color }}>
        {level}
      </div>
    </div>
  );
}

const TrustPage = () => {
  const [credentials, setCredentials] = useState([]);
  const [reputation, setReputation] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Placeholder for the current user's ID and DID
  const userId = "user_id_1";
  const userDid = "did:web:platformq.com:user:12345";

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        // Fetch both credentials and reputation score
        const [userCredentials, userReputation] = await Promise.all([
          mockApi.getUserCredentials(userDid),
          mockApi.getUserReputation(userId)
        ]);

        setCredentials(userCredentials);
        setReputation(userReputation.reputation_score);
        setError(null);
      } catch (err) {
        setError("Failed to fetch credentials.");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [userId, userDid]);

  return (
    <div className="trust-page">
      <header className="trust-page-header">
        <div className="header-content">
          <h1>Reputation Wallet</h1>
          <p>These are the Verifiable Credentials you have earned through your contributions to the platform.</p>
          <div className="did-display">
            <strong>Your Decentralized Identifier (DID):</strong>
            <span className="code-text">{userDid}</span>
          </div>
        </div>
        <ReputationScore score={reputation} />
      </header>
      
      <main className="wallet-content">
        {loading && <p>Loading credentials...</p>}
        {error && <p className="error-message">{error}</p>}
        {!loading && !error && (
          <div className="credentials-grid">
            {credentials.length > 0 ? (
              credentials.map((cred) => (
                <CredentialCard key={cred.credential_id} credential={cred} />
              ))
            ) : (
              <p>You have not earned any credentials yet.</p>
            )}
          </div>
        )}
      </main>
    </div>
  );
};

export default TrustPage; 