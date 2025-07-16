import React, { useState, useEffect } from 'react';
import { UserManager, WebStorageStateStore } from 'oidc-client-ts';
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Link,
  Outlet
} from "react-router-dom";
import SimulationViewer from './components/SimulationViewer';
import TrustPage from './features/TrustPage';

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

// New Components (files would be created for these)
const Dashboard = () => <h2>Dashboard</h2>;
const SimulationManager = () => <h2>Simulation Manager</h2>;
import AssetExplorerPage from './features/assets/AssetExplorerPage';
import DAODashboard from './components/dao/DAODashboard';
import DAOPage from './features/dao/DAOPage';
import ProjectsPage from './features/projects/ProjectsPage';
import AssetsPage from './features/assets/AssetsPage';
import TrustWallet from './features/TrustWallet/TrustWallet';
import Marketplace from './features/Marketplace/Marketplace';

function Layout() {
  return (
    <div style={{ display: 'flex' }}>
      <nav style={{ width: '200px', borderRight: '1px solid #ccc', padding: '1rem' }}>
        <h2>platformQ</h2>
        <ul>
          <li><Link to="/">Dashboard</Link></li>
          <li><Link to="/simulations">Simulations</Link></li>
          <li><Link to="/assets">Asset Explorer</Link></li>
          <li><Link to="/daos">DAO Dashboard</Link></li>
          <li><Link to="/dao">DAO</Link></li>
          <li><Link to="/projects">Projects</Link></li>
          <li><Link to="/marketplace">Marketplace</Link></li>
          <li><Link to="/trust-wallet">Trust Wallet</Link></li>
          <li><button onClick={() => userManager.signoutRedirect()}>Logout</button></li>
        </ul>
      </nav>
      <main style={{ padding: '1rem', flexGrow: 1 }}>
        <Outlet />
      </main>
    </div>
  );
}

function App() {
  const [user, setUser] = useState(null);
  const [userInfo, setUserInfo] = useState(null);
  const [selectedUserId, setSelectedUserId] = useState('user1'); // demo user

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
      <Router>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="simulations" element={<SimulationManager />} />
            <Route path="assets" element={<AssetExplorerPage user={user} />} />
            <Route path="daos" element={<DAODashboard />} />
            <Route path="dao" element={<DAOPage />} />
            <Route path="projects" element={<ProjectsPage />} />
            <Route path="marketplace" element={<Marketplace />} />
            <Route path="trust-wallet" element={<TrustWallet />} />
          </Route>
        </Routes>
      </Router>
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