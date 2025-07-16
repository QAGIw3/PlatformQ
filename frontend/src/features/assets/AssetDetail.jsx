import React, { useState, useEffect } from 'react';
import { useAssetStore } from './assetStore';
import { getAssetById, migrateAssetStorage } from './api';

const AssetDetail = ({ accessToken }) => {
    const selectedAssetId = useAssetStore((state) => state.selectedAssetId);
    const [asset, setAsset] = useState(null);
    const [error, setError] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    const [isMigrating, setIsMigrating] = useState(false);
    const [migrationError, setMigrationError] = useState(null);

    useEffect(() => {
        if (!selectedAssetId || !accessToken) {
            setAsset(null);
            return;
        }

        setIsLoading(true);
        getAssetById(selectedAssetId, accessToken)
            .then(data => {
                setAsset(data);
                setIsLoading(false);
            })
            .catch(err => {
                setError(err.message);
                setIsLoading(false);
            });
    }, [selectedAssetId, accessToken]);

    const handleMigrate = async () => {
        if (!selectedAssetId || !accessToken) return;
        setIsMigrating(true);
        setMigrationError(null);
        try {
            await migrateAssetStorage(selectedAssetId, accessToken);
            // Optionally, refresh asset details to show new URI
        } catch (err) {
            setMigrationError(err.message);
        } finally {
            setIsMigrating(false);
        }
    };

    if (!selectedAssetId) {
        return <div>Select an asset to see its details.</div>;
    }

    if (isLoading) {
        return <div>Loading details...</div>;
    }

    if (error) {
        return <div style={{ color: 'red' }}>Error: {error}</div>;
    }

    if (!asset) {
        return null;
    }

    return (
        <div>
            <h4>Asset Details</h4>
            <button onClick={handleMigrate} disabled={isMigrating} style={{ marginBottom: '1rem' }}>
                {isMigrating ? 'Migrating...' : 'Migrate Storage to My Preference'}
            </button>
            {migrationError && <p style={{ color: 'red' }}>Migration Error: {migrationError}</p>}
            <pre style={{ background: '#f4f4f4', padding: '1rem', borderRadius: '5px' }}>
                <code>{JSON.stringify(asset, null, 2)}</code>
            </pre>
        </div>
    );
};

export default AssetDetail; 