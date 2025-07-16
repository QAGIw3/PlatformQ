import React from 'react';
import { useWallet } from './WalletProvider';
import { StorageSettings } from './StorageSettings';
import { CredentialIssuer } from './CredentialIssuer';

const SettingsPage = ({ user, authToken }) => {
  const { address: walletAddress } = useWallet();

  if (!user) {
    return <div>Loading user data...</div>;
  }

  return (
    <div>
      <h2>User Settings</h2>
      
      <div style={{ marginBottom: '2rem' }}>
        <h3>Profile</h3>
        <p><strong>Email:</strong> {user.profile.email}</p>
        <p><strong>Full Name:</strong> {user.profile.full_name}</p>
        <p><strong>DID:</strong> {user.profile.did}</p>
      </div>

      <div style={{ marginBottom: '2rem' }}>
        <h3>Connected Wallet</h3>
        {walletAddress ? (
          <p><strong>Address:</strong> {walletAddress}</p>
        ) : (
          <p>No wallet connected. Please connect your wallet using the button in the sidebar.</p>
        )}
      </div>

      <StorageSettings user={user} authToken={authToken} />

      {user?.profile?.did && <CredentialIssuer userDid={user.profile.did} />}

    </div>
  );
};

export default SettingsPage; 