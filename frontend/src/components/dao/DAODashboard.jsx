import React, { useState, useEffect } from 'react';
import './DAODashboard.css'; // Assuming we'll create this for styling

const DAODashboard = () => {
    const [daos, setDaos] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        // Simulate fetching DAO data from a backend API
        const fetchDaos = async () => {
            try {
                // Replace with actual API call to your backend services
                // const response = await fetch('/api/daos'); 
                // const data = await response.json();

                // Mock data for now
                const mockData = [
                    { id: 'dao-1', name: 'PlatformQ Core DAO', description: 'Governs core platform features.', members: 150, proposals: 25, status: 'active' },
                    { id: 'dao-2', name: 'Open Source Initiative DAO', description: 'Funds and directs open-source projects.', members: 75, proposals: 10, status: 'active' },
                    { id: 'dao-3', name: 'Community Grants DAO', description: 'Distributes grants to community projects.', members: 200, proposals: 30, status: 'paused' },
                ];
                setDaos(mockData);
            } catch (err) {
                setError('Failed to fetch DAOs');
                console.error(err);
            } finally {
                setLoading(false);
            }
        };

        fetchDaos();
    }, []);

    if (loading) {
        return <div className="dao-dashboard-container">Loading DAOs...</div>;
    }

    if (error) {
        return <div className="dao-dashboard-container error">Error: {error}</div>;
    }

    return (
        <div className="dao-dashboard-container">
            <h2>Decentralized Autonomous Organizations (DAOs)</h2>
            <button className="create-dao-button">+ Create New DAO</button>

            <div className="dao-list">
                {daos.length === 0 ? (
                    <p>No DAOs found. Create one to get started!</p>
                ) : (
                    daos.map(dao => (
                        <div key={dao.id} className="dao-card">
                            <h3>{dao.name}</h3>
                            <p>{dao.description}</p>
                            <div className="dao-stats">
                                <span>Members: {dao.members}</span>
                                <span>Proposals: {dao.proposals}</span>
                                <span className={`status ${dao.status}`}>{dao.status}</span>
                            </div>
                            <div className="dao-actions">
                                <button>View Details</button>
                                <button>Propose</button>
                                <button>Vote</button>
                            </div>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
};

export default DAODashboard; 