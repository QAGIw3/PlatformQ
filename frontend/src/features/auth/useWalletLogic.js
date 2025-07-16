import { useState, useCallback, useEffect } from 'react';
import { ethers } from 'ethers';
import { SiweMessage } from 'siwe';

// In a real app, you'd get this from your API client
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function createSiweMessage(address, nonce) {
  const domain = window.location.host;
  const origin = window.location.origin;
  const statement = 'Sign in with Ethereum to the app.';
  return new SiweMessage({
    domain,
    address,
    statement,
    uri: origin,
    version: '1',
    chainId: '1', // The chain ID of the network your application is using
    nonce,
  });
}

export const useWalletLogic = (oidcUser) => {
  const [address, setAddress] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Pre-fill address if it's already on the user object
    if (oidcUser?.profile?.wallet_address) {
      setAddress(oidcUser.profile.wallet_address);
    }
  }, [oidcUser]);

  const connectWallet = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (typeof window.ethereum === 'undefined') {
        throw new Error('MetaMask is not installed.');
      }
      
      const provider = new ethers.BrowserProvider(window.ethereum);
      const signer = await provider.getSigner();
      const walletAddress = await signer.getAddress();
      
      // Get nonce from the backend
      const nonceRes = await fetch(`${API_URL}/api/v1/siwe/nonce`);
      const { nonce } = await nonceRes.json();
      
      // Create SIWE message
      const message = createSiweMessage(walletAddress, nonce);
      const messageToSign = message.prepareMessage();
      
      // Get signature from the user
      const signature = await signer.signMessage(messageToSign);
      
      const authToken = oidcUser?.access_token;
      if (!authToken) {
        throw new Error("User is not authenticated. Cannot link wallet.");
      }

      const verifyRes = await fetch(`${API_URL}/api/v1/users/me/link-wallet`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${authToken}` // Use the real token
        },
        body: JSON.stringify({ message: message.toMessage(), signature }),
      });

      if (!verifyRes.ok) {
        const errorData = await verifyRes.json();
        throw new Error(errorData.detail || 'Failed to link wallet.');
      }

      const user = await verifyRes.json();
      setAddress(user.wallet_address);

    } catch (err) {
      setError(err.message);
      console.error('Wallet connection error:', err);
    } finally {
      setLoading(false);
    }
  }, [oidcUser]);

  const disconnectWallet = useCallback(() => {
    setAddress(null);
    // Here you might also want to clear any related user state
  }, []);

  return { address, loading, error, connectWallet, disconnectWallet };
}; 