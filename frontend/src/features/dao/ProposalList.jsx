import React, { useState, useEffect } from 'react';
import { getProposals } from '../../api/client';

function ProposalList({ refreshKey }) {
    const [proposals, setProposals] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        setLoading(true);
        getProposals()
            .then(response => {
                setProposals(response.data);
                setLoading(false);
            })
            .catch(error => {
                setError(error);
                setLoading(false);
            });
    }, [refreshKey]);

    if (loading) return <p>Loading proposals...</p>;
    if (error) return <p>Error loading proposals: {error.message}</p>;

    return (
        <div>
            <h2>Proposals</h2>
            <ul>
                {proposals.map((proposal) => (
                    <li key={proposal.id}>
                        {proposal.title} - <strong>{proposal.status}</strong>
                    </li>
                ))}
            </ul>
        </div>
    );
}

export default ProposalList; 