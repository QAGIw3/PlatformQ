// In a real app, this would be configured, possibly via environment variables.
const API_BASE_URL = 'http://localhost:8000/api/v1/assets';

/**
 * Fetches a list of digital assets from the API.
 * @param {string} accessToken - The OIDC access token for authorization.
 * @param {string} searchTerm - The text to search for in asset names.
 * @param {string} filterType - The asset type to filter by.
 * @returns {Promise<Array>} A promise that resolves to an array of assets.
 */
export const getAssets = async (accessToken, { searchTerm, filterType }) => {
    const params = new URLSearchParams();
    if (searchTerm) {
        params.append('search', searchTerm);
    }
    if (filterType) {
        params.append('asset_type', filterType);
    }

    const response = await fetch(`${API_BASE_URL}?${params.toString()}`, {
        headers: {
            'Authorization': `Bearer ${accessToken}`,
        },
    });

    if (!response.ok) {
        throw new Error(`Failed to fetch assets: ${response.statusText}`);
    }

    return response.json();
};

/**
 * Fetches a single digital asset by its ID.
 * @param {string} assetId - The UUID of the asset.
 * @param {string} accessToken - The OIDC access token for authorization.
 * @returns {Promise<Object>} A promise that resolves to the asset object.
 */
export const getAssetById = async (assetId, accessToken) => {
    const response = await fetch(`${API_BASE_URL}/${assetId}`, {
        headers: {
            'Authorization': `Bearer ${accessToken}`,
        },
    });

    if (!response.ok) {
        throw new Error(`Failed to fetch asset ${assetId}: ${response.statusText}`);
    }

    return response.json();
};

/**
 * Sends a request to migrate an asset's storage.
 * @param {string} assetId - The UUID of the asset.
 * @param {string} accessToken - The OIDC access token for authorization.
 * @returns {Promise<void>}
 */
export const migrateAssetStorage = async (cid, accessToken) => {
    const response = await fetch(`${API_BASE_URL}/internal/digital-assets/${cid}/migrate`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${accessToken}`,
        },
    });

    if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(`Failed to migrate asset ${cid}: ${errorData.detail}`);
    }
};
