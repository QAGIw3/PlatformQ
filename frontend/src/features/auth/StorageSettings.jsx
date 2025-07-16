import React, { useState, useEffect } from 'react';

// In a real app, you'd get this from your API client
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const StorageSettings = ({ user, authToken }) => {
  const [backend, setBackend] = useState('minio');
  const [config, setConfig] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    // Pre-fill with user's current settings
    if (user?.profile?.storage_backend) {
      setBackend(user.profile.storage_backend);
    }
    if (user?.profile?.storage_config) {
      setConfig(user.profile.storage_config);
    }
  }, [user]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setSuccess(false);

    try {
        const response = await fetch(`${API_URL}/api/v1/users/me/storage`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${authToken}`,
            },
            body: JSON.stringify({
                storage_backend: backend,
                storage_config: config,
            }),
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to save settings.');
        }

        setSuccess(true);
    } catch (err) {
        setError(err.message);
    } finally {
        setLoading(false);
    }
  };

  return (
    <div style={{ border: '1px solid #ccc', padding: '1rem', marginTop: '1rem' }}>
      <h3>Configure Your Storage</h3>
      <form onSubmit={handleSubmit}>
        <div>
          <label htmlFor="storage-backend">Storage Provider:</label>
          <select 
            id="storage-backend"
            value={backend} 
            onChange={(e) => setBackend(e.target.value)}
            style={{ marginLeft: '10px' }}
          >
            <option value="minio">MinIO (Platform Default)</option>
            <option value="ipfs">IPFS</option>
            <option value="arweave">Arweave</option>
          </select>
        </div>
        <div style={{ marginTop: '10px' }}>
          <label htmlFor="storage-config">Provider Configuration (JSON):</label>
          <textarea
            id="storage-config"
            value={config}
            onChange={(e) => setConfig(e.target.value)}
            placeholder='e.g., {"host": "my-minio.com", "access_key": "...", "secret_key": "..."}'
            rows={5}
            style={{ width: '100%', marginTop: '5px' }}
          />
        </div>
        <button type="submit" disabled={loading} style={{ marginTop: '10px' }}>
            {loading ? 'Saving...' : 'Save Storage Settings'}
        </button>
        {error && <p style={{ color: 'red', marginTop: '5px' }}>Error: {error}</p>}
        {success && <p style={{ color: 'green', marginTop: '5px' }}>Settings saved successfully!</p>}
      </form>
    </div>
  );
}; 