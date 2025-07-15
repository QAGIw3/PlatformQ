import React from 'react';
import { useAssetStore } from './assetStore';

// A mock list of asset types. In a real app, this might be fetched from an API.
const ASSET_TYPES = ["", "CRM_CONTACT", "3D_MODEL", "IMAGE", "AUDIO", "VIDEO", "ERP_BPARTNER", "WEBHOOK_PAYLOAD"];

const AssetFilterBar = () => {
    const { searchTerm, setSearchTerm, filterType, setFilterType } = useAssetStore();

    return (
        <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
            <input
                type="text"
                placeholder="Search by name..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                style={{ padding: '0.5rem', width: '200px' }}
            />
            <select
                value={filterType}
                onChange={(e) => setFilterType(e.target.value)}
                style={{ padding: '0.5rem' }}
            >
                {ASSET_TYPES.map(type => (
                    <option key={type} value={type}>
                        {type || "All Types"}
                    </option>
                ))}
            </select>
        </div>
    );
};

export default AssetFilterBar; 