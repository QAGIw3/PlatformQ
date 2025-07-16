import React, { createContext, useContext } from 'react';
import { useWalletLogic } from './useWalletLogic';

const WalletContext = createContext(null);

export const WalletProvider = ({ user, children }) => {
  // We pass the OIDC user object to the underlying hook
  const wallet = useWalletLogic(user);
  return (
    <WalletContext.Provider value={wallet}>
      {children}
    </WalletContext.Provider>
  );
};

export const useWallet = () => {
  const context = useContext(WalletContext);
  if (context === null) {
    throw new Error('useWallet must be used within a WalletProvider');
  }
  return context;
}; 