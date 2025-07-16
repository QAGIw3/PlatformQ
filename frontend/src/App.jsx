import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, Outlet } from "react-router-dom";
import { AuthProvider, useAuth } from './features/auth/AuthProvider';
import SimulationViewer from './components/SimulationViewer';
import TrustPage from './features/TrustPage';
import AssetExplorerPage from './features/assets/AssetExplorerPage';
import DAODashboard from './components/dao/DAODashboard';
import DAOPage from './features/dao/DAOPage';
import ProjectsPage from './features/projects/ProjectsPage';
import AssetsPage from './features/assets/AssetsPage';
import TrustWallet from './features/TrustWallet/TrustWallet';
import Marketplace from './features/Marketplace/Marketplace';
import { WalletConnector } from './features/auth/WalletConnector';
import { WalletProvider } from './features/auth/WalletProvider';
import SettingsPage from './features/auth/SettingsPage';
import { ProtectedDAOPage } from './features/dao/ProtectedDAOPage';

const Dashboard = () => <h2>Dashboard</h2>;
const SimulationManager = () => <h2>Simulation Manager</h2>;

function OnboardingPrompt() {
  const { user } = useAuth();
  
  // A simple check to see if the user has a default profile after SIWE signup
  const needsOnboarding = user?.profile?.full_name === user?.profile?.wallet_address;

  if (!needsOnboarding) {
    return null;
  }

  return (
    <div style={{ background: '#eef', padding: '1rem', textAlign: 'center' }}>
      Welcome! Please visit your <Link to="/settings">settings</Link> to complete your profile.
    </div>
  );
}

function Layout() {
  const { logout } = useAuth();
  return (
    <div style={{ display: 'flex' }}>
      <nav style={{ width: '200px', borderRight: '1px solid #ccc', padding: '1rem' }}>
        <h2>platformQ</h2>
        <ul>
          <li><Link to="/">Dashboard</Link></li>
          <li><Link to="/settings">Settings</Link></li>
          <li><Link to="/simulations">Simulations</Link></li>
          <li><Link to="/assets">Asset Explorer</Link></li>
          <li><Link to="/daos">DAO Dashboard</Link></li>
          <li><Link to="/dao">DAO</Link></li>
          <li><Link to="/protected-dao">Protected DAO</Link></li>
          <li><Link to="/projects">Projects</Link></li>
          <li><Link to="/marketplace">Marketplace</Link></li>
          <li><Link to="/trust-wallet">Trust Wallet</Link></li>
          <li><button onClick={logout}>Logout</button></li>
        </ul>
        <div style={{ marginTop: 'auto' }}>
          <WalletConnector />
        </div>
      </nav>
      <main style={{ padding: '1rem', flexGrow: 1 }}>
        <OnboardingPrompt />
        <Outlet />
      </main>
    </div>
  );
}

function LoginPage() {
    const { loginOidc, loginSiwe } = useAuth();
    return (
        <div>
            <h1>Welcome to platformQ</h1>
            <p>Please log in to continue.</p>
            <button onClick={loginOidc}>Login with platformQ (OIDC)</button>
            <button onClick={loginSiwe} style={{ marginLeft: '10px' }}>Sign-In with Ethereum</button>
        </div>
    );
}

function AppContent() {
  const { user, loading } = useAuth();

  if (loading) {
    return <div>Loading...</div>;
  }

  if (user) {
    return (
      <WalletProvider user={user}>
        <Router>
          <Routes>
            <Route path="/" element={<Layout />}>
              <Route index element={<Dashboard />} />
              <Route path="settings" element={<SettingsPage user={user} authToken={user.access_token} />} />
              <Route path="simulations" element={<SimulationManager />} />
              <Route path="assets" element={<AssetExplorerPage user={user} />} />
              <Route path="daos" element={<DAODashboard />} />
              <Route path="dao" element={<DAOPage />} />
              <Route path="protected-dao" element={<ProtectedDAOPage />} />
              <Route path="projects" element={<ProjectsPage />} />
              <Route path="marketplace" element={<Marketplace />} />
              <Route path="trust-wallet" element={<TrustWallet />} />
            </Route>
          </Routes>
        </Router>
      </WalletProvider>
    );
  }

  return <LoginPage />;
}

function App() {
  return (
    <AuthProvider>
      <AppContent />
    </AuthProvider>
  );
}

export default App; 