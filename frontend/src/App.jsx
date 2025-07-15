import React, { useState, useEffect } from 'react';
import { UserManager, WebStorageStateStore } from 'oidc-client-ts';

// --- OIDC Configuration ---
const oidcSettings = {
  authority: 'http://<KONG_IP>/auth/api/v1', // Replace with your Kong IP
  client_id: 'nextcloud', // We can reuse the client for now
  redirect_uri: window.location.origin,
  response_type: 'code',
  scope: 'openid profile email',
  userStore: new WebStorageStateStore({ store: window.localStorage }),
};

const userManager = new UserManager(oidcSettings);

function App() {
  const [user, setUser] = useState(null);
  const [userInfo, setUserInfo] = useState(null);

  useEffect(() => {
    // Check if the user is returning from the OIDC provider
    if (window.location.href.includes('code=')) {
      userManager.signinRedirectCallback().then(user => {
        setUser(user);
        window.history.replaceState({}, '', '/');
      });
    } else {
      // Check if user is already logged in
      userManager.getUser().then(user => {
        if (user && !user.expired) {
          setUser(user);
        }
      });
    }
  }, []);

  useEffect(() => {
    // Fetch user info from the protected endpoint when we have a user
    if (user) {
      fetch('http://<KONG_IP>/auth/api/v1/users/me', {
        headers: {
          'Authorization': `Bearer ${user.access_token}`
        }
      })
      .then(res => res.json())
      .then(data => setUserInfo(data))
      .catch(console.error);
    }
  }, [user]);

  const handleLogin = () => {
    userManager.signinRedirect();
  };

  const handleLogout = () => {
    userManager.signoutRedirect();
  };

  if (user) {
    return (
      <div>
        <h1>Welcome, platformQ</h1>
        <p>You are logged in!</p>
        {userInfo ? (
          <pre>{JSON.stringify(userInfo, null, 2)}</pre>
        ) : (
          <p>Loading user info...</p>
        )}
        <button onClick={handleLogout}>Logout</button>
      </div>
    );
  }

  return (
    <div>
      <h1>Welcome to platformQ</h1>
      <p>Please log in.</p>
      <button onClick={handleLogin}>Login with platformQ</button>
    </div>
  );
}

export default App; 