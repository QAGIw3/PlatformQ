import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { UserManager, WebStorageStateStore } from 'oidc-client-ts';
import { ethers } from 'ethers';
import { SiweMessage } from 'siwe';

const AuthContext = createContext(null);

const oidcSettings = {
    authority: 'http://<KONG_IP>/auth/api/v1',
    client_id: 'nextcloud',
    redirect_uri: window.location.origin,
    response_type: 'code',
    scope: 'openid profile email',
    userStore: new WebStorageStateStore({ store: window.localStorage }),
};
const userManager = new UserManager(oidcSettings);

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(null);
    const [loading, setLoading] = useState(true);

    const handleOidcLogin = () => userManager.signinRedirect();
    const handleLogout = () => userManager.signoutRedirect();

    const handleSiweLogin = useCallback(async () => {
        setLoading(true);
        try {
            if (typeof window.ethereum === 'undefined') throw new Error('MetaMask is not installed.');
            
            const provider = new ethers.BrowserProvider(window.ethereum);
            const signer = await provider.getSigner();
            const walletAddress = await signer.getAddress();
            
            const nonceRes = await fetch(`${API_URL}/api/v1/siwe/nonce`);
            const { nonce } = await nonceRes.json();
            
            const message = new SiweMessage({
                domain: window.location.host,
                address: walletAddress,
                statement: 'Sign in with Ethereum to the app.',
                uri: window.location.origin,
                version: '1',
                chainId: '1',
                nonce,
            });
            const messageToSign = message.prepareMessage();
            const signature = await signer.signMessage(messageToSign);
            
            const loginRes = await fetch(`${API_URL}/api/v1/siwe/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: message.toMessage(), signature }),
            });

            if (!loginRes.ok) throw new Error('SIWE login failed.');
            
            const { access_token } = await loginRes.json();
            // We have a token, now we need the user profile
            const userRes = await fetch(`${API_URL}/api/v1/users/me`, {
                headers: { 'Authorization': `Bearer ${access_token}` }
            });

            if (!userRes.ok) throw new Error('Failed to fetch user profile after SIWE login.');
            
            const profile = await userRes.json();
            setUser({ access_token, profile });

        } catch (error) {
            console.error("SIWE Login Error:", error);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        // OIDC flow
        if (window.location.href.includes('code=')) {
            userManager.signinRedirectCallback().then(oidcUser => {
                setUser(oidcUser);
                window.history.replaceState({}, '', '/');
            });
        } else {
            // Check for existing OIDC session
            userManager.getUser().then(oidcUser => {
                if (oidcUser && !oidcUser.expired) {
                    setUser(oidcUser);
                }
                setLoading(false);
            });
        }
    }, []);

    const value = {
        user,
        loading,
        loginOidc: handleOidcLogin,
        loginSiwe: handleSiweLogin,
        logout: handleLogout
    };

    return (
        <AuthContext.Provider value={value}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
}; 