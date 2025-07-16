import React, { useState } from 'react';
import CreateAsset from './CreateAsset';
import AssetList from './AssetList';

function AssetsPage() {
    const [refreshKey, setRefreshKey] = useState(0);

    const handleAssetCreated = () => {
        setRefreshKey(prevKey => prevKey + 1);
    };

    return (
        <div>
            <h1>Digital Assets</h1>
            <hr />
            <CreateAsset onAssetCreated={handleAssetCreated} />
            <hr />
            <AssetList key={refreshKey} />
        </div>
    );
}

export default AssetsPage; 