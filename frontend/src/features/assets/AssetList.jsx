import React, { useState, useEffect } from 'react';
import { getAssets } from './api';
import { useAssetStore } from './assetStore';

// Custom hook for debouncing
function useDebounce(value, delay) {
    const [debouncedValue, setDebouncedValue] = useState(value);
    useEffect(() => {
        const handler = setTimeout(() => {
            setDebouncedValue(value);
        }, delay);
        return () => {
            clearTimeout(handler);
        };
    }, [value, delay]);
    return debouncedValue;
}

const AssetList = ({ accessToken }) => {
    const [assets, setAssets] = useState([]);
    const [error, setError] = useState(null);
    const [isLoading, setIsLoading] = useState(true);
    const { selectAsset, selectedAssetId, searchTerm, filterType } = useAssetStore();
    const debouncedSearchTerm = useDebounce(searchTerm, 500); // 500ms delay

    useEffect(() => {
        if (!accessToken) return;

        setIsLoading(true);
        getAssets(accessToken, { searchTerm: debouncedSearchTerm, filterType })
            .then(data => {
                setAssets(data);
                setIsLoading(false);
            })
            .catch(err => {
                setError(err.message);
                setIsLoading(false);
            });
    }, [accessToken, debouncedSearchTerm, filterType]);

    if (isLoading) {
        return <div>Loading assets...</div>;
    }

    if (error) {
        return <div style={{ color: 'red' }}>Error: {error}</div>;
    }

    return (
        <div>
            <h3>Available Assets</h3>
            <ul style={{ listStyle: 'none', padding: 0 }}>
                {assets.length > 0 ? (
                    assets.map(asset => (
                        <li 
                            key={asset.asset_id} 
                            onClick={() => selectAsset(asset.asset_id)}
                            style={{ 
                                padding: '0.5rem', 
                                cursor: 'pointer', 
                                backgroundColor: selectedAssetId === asset.asset_id ? '#e0e0e0' : 'transparent' 
                            }}
                        >
                            {asset.asset_name} ({asset.asset_type})
                        </li>
                    ))
                ) : (
                    <li>No assets found.</li>
                )}
            </ul>
        </div>
    );
};

export default AssetList;
