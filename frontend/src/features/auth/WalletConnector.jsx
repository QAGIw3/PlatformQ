import React from 'react';
import { useWallet } from './WalletProvider';

export const WalletConnector = () => {
  const { address, loading, error, connectWallet, disconnectWallet } = useWallet();

  return (
    <div>
      {address ? (
        <div>
          <span>Connected: {`${address.substring(0, 6)}...${address.substring(address.length - 4)}`}</span>
          <button onClick={disconnectWallet} style={{ marginLeft: '10px' }}>Disconnect</button>
        </div>
      ) : (
        <button onClick={connectWallet} disabled={loading}>
          {loading ? 'Connecting...' : 'Connect Wallet'}
        </button>
      )}
      {error && <p style={{ color: 'red', marginTop: '5px' }}>Error: {error}</p>}
    </div>
  );
}; 