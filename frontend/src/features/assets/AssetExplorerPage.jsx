import React from 'react';
import AssetList from './AssetList';
import AssetDetail from './AssetDetail';
import AssetFilterBar from './AssetFilterBar';

const AssetExplorerPage = ({ user }) => {
    const pageStyle = {
        display: 'flex',
        gap: '2rem',
    };
    const listStyle = {
        flex: '1 1 30%', // Flex-grow | Flex-shrink | Flex-basis
    };
    const detailStyle = {
        flex: '1 1 70%',
    };

    return (
        <div>
            <h2>Digital Asset Explorer</h2>
            <p>Browse and manage all digital assets ingested by the platform.</p>
            <AssetFilterBar />
            <hr />
            <div style={pageStyle}>
                <div style={listStyle}>
                    <AssetList accessToken={user?.access_token} />
                </div>
                <div style={detailStyle}>
                    <AssetDetail accessToken={user?.access_token} />
                </div>
            </div>
        </div>
    );
};

export default AssetExplorerPage;
