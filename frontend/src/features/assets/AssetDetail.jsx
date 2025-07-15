import React, { useState, useEffect } from 'react';
import { useAssetStore } from './assetStore';
import { getAssetById } from './api';

const AssetDetail = ({ accessToken }) => {
    const selectedAssetId = useAssetStore((state) => state.selectedAssetId);
    const [asset, setAsset] = useState(null);
    const [error, setError] = useState(null);
    const [isLoading, setIsLoading] = useState(false);

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
            <pre style={{ background: '#f4f4f4', padding: '1rem', borderRadius: '5px' }}>
                <code>{JSON.stringify(asset, null, 2)}</code>
            </pre>
        </div>
    );
};

export default AssetDetail; 