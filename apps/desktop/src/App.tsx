import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import Settings from './pages/Settings';
import SyncStatus from './pages/SyncStatus';
import FolderManager from './pages/FolderManager';
import Logs from './pages/Logs';

const navItems = [
  { to: '/', label: 'Sync Status' },
  { to: '/folders', label: 'Folders' },
  { to: '/settings', label: 'Settings' },
  { to: '/logs', label: 'Logs' },
];

export default function App() {
  return (
    <BrowserRouter>
      <div style={{ display: 'flex', height: '100vh' }}>
        {/* Sidebar */}
        <nav style={{
          width: 200,
          background: '#1e293b',
          padding: '20px 0',
          display: 'flex',
          flexDirection: 'column',
          gap: 4,
        }}>
          <div style={{
            padding: '0 16px 16px',
            borderBottom: '1px solid #334155',
            marginBottom: 8,
          }}>
            <h1 style={{ fontSize: 16, fontWeight: 600, color: '#f1f5f9' }}>
              Universal BI
            </h1>
            <span style={{ fontSize: 12, color: '#94a3b8' }}>Desktop Agent</span>
          </div>
          {navItems.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              style={({ isActive }) => ({
                display: 'block',
                padding: '8px 16px',
                color: isActive ? '#38bdf8' : '#94a3b8',
                textDecoration: 'none',
                fontSize: 14,
                background: isActive ? '#0f172a' : 'transparent',
                borderLeft: isActive ? '3px solid #38bdf8' : '3px solid transparent',
              })}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        {/* Main content */}
        <main style={{ flex: 1, padding: 24, overflowY: 'auto' }}>
          <Routes>
            <Route path="/" element={<SyncStatus />} />
            <Route path="/folders" element={<FolderManager />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/logs" element={<Logs />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
