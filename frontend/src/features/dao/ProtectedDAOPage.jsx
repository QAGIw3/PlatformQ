import React, { useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8001'; // Assuming vc-service runs on 8001

export const ProtectedDAOPage = () => {
    const [isVerified, setIsVerified] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    const handlePresentCredential = async () => {
        setIsLoading(true);
        setError(null);

        try {
            // In a real app, this would come from the user's wallet (e.g., via a browser extension).
            const mockPresentation = {
                "@context": ["https://www.w3.org/2018/credentials/v1"],
                "type": ["VerifiablePresentation"],
                "verifiableCredential": [{
                    "@context": ["https://www.w3.org/2018/credentials/v1"],
                    "id": "urn:uuid:1234",
                    "type": ["VerifiableCredential", "DAOContributorCredential"],
                    "issuer": "did:example:123",
                    "issuanceDate": new Date().toISOString(),
                    "credentialSubject": {
                        "id": "did:example:456",
                        "daoName": "platformQ DAO"
                    },
                    "proof": { /* ... */ }
                }]
            };

            const response = await fetch(`${API_URL}/api/v1/presentations/verify`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    verifiable_presentation: mockPresentation,
                    required_credential_type: 'DAOContributorCredential',
                }),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to verify presentation.');
            }
            
            const data = await response.json();
            if (data.verified) {
                setIsVerified(true);
            } else {
                setError(data.error || 'Presentation was not valid.');
            }

        } catch (err) {
            setError(err.message);
        } finally {
            setIsLoading(false);
        }
    };

    if (!isVerified) {
        return (
            <div>
                <h2>DAO Workspace</h2>
                <p>You must present a valid DAO Contributor Credential to access this page.</p>
                <button onClick={handlePresentCredential} disabled={isLoading}>
                    {isLoading ? 'Verifying...' : 'Present Credential'}
                </button>
                {error && <p style={{ color: 'red', marginTop: '5px' }}>Error: {error}</p>}
            </div>
        );
    }

    return (
        <div>
            <h2>Welcome to the DAO Workspace</h2>
            <p>Your credential has been verified. You now have access to this protected area.</p>
        </div>
    );
}; 