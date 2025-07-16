import React, { useState } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8001'; // Assuming vc-service runs on 8001

export const CredentialIssuer = ({ userDid }) => {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [presentationRequest, setPresentationRequest] = useState(null);

    const handleIssue = async () => {
        setLoading(true);
        setError(null);
        setPresentationRequest(null);

        try {
            const response = await fetch(`${API_URL}/api/v1/issue-direct`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    subject_did: userDid,
                    credential_type: 'DAOContributorCredential',
                    credential_data: {
                        daoName: 'platformQ DAO',
                        contributionDate: new Date().toISOString(),
                    },
                }),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to initiate issuance.');
            }

            const data = await response.json();
            setPresentationRequest(data);

        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div style={{ border: '1px solid #ccc', padding: '1rem', marginTop: '1rem' }}>
            <h3>Claim Your DAO Contributor Credential</h3>
            <button onClick={handleIssue} disabled={loading}>
                {loading ? 'Requesting...' : 'Claim Credential'}
            </button>
            {error && <p style={{ color: 'red', marginTop: '5px' }}>Error: {error}</p>}
            {presentationRequest && (
                <div style={{ marginTop: '1rem' }}>
                    <h4>Issuance Request Sent!</h4>
                    <p>Your wallet should now handle this request:</p>
                    <pre style={{ background: '#f4f4f4', padding: '1rem' }}>
                        <code>{JSON.stringify(presentationRequest, null, 2)}</code>
                    </pre>
                </div>
            )}
        </div>
    );
}; 