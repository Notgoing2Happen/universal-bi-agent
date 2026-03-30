import { useEffect, useState } from 'react';
import { HashRouter, Routes, Route, NavLink, useNavigate } from 'react-router-dom';
import Settings from './pages/Settings';
import SyncStatus from './pages/SyncStatus';
import FolderManager from './pages/FolderManager';
import Logs from './pages/Logs';

const isTauri = typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window;

const navItems = [
  { to: '/', label: 'Sync Status' },
  { to: '/folders', label: 'Folders' },
  { to: '/settings', label: 'Settings' },
  { to: '/logs', label: 'Logs' },
];

function decodeSetupToken(token: string): { platformUrl: string; apiKey: string } | null {
  try {
    const json = atob(token.trim());
    const data = JSON.parse(json);
    if (!data.u || !data.k) return null;
    // Check expiry
    if (data.e && Date.now() / 1000 > data.e) {
      console.error('[DeepLink] Setup token expired');
      return null;
    }
    return { platformUrl: data.u, apiKey: data.k };
  } catch {
    return null;
  }
}

async function applySetupFromUrl(url: string): Promise<boolean> {
  const prefix = 'universal-bi://setup/';
  if (!url.startsWith(prefix)) return false;

  const token = url.slice(prefix.length);
  const decoded = decodeSetupToken(token);
  if (!decoded) return false;

  try {
    const { invoke } = await import('@tauri-apps/api/core');
    await invoke('sidecar_rpc', {
      method: 'config.set',
      params: { platformUrl: decoded.platformUrl, apiKey: decoded.apiKey },
      id: Date.now(),
    });
    console.log('[DeepLink] Config applied successfully');
    return true;
  } catch (err) {
    console.error('[DeepLink] Failed to apply config:', err);
    return false;
  }
}

function AppContent() {
  const navigate = useNavigate();
  const [setupBanner, setSetupBanner] = useState<string | null>(null);

  useEffect(() => {
    if (!isTauri) return;

    let cancelled = false;

    async function initDeepLinks() {
      try {
        const { onOpenUrl, getCurrent } = await import('@tauri-apps/plugin-deep-link');

        // Check if app was launched via deep link (cold start)
        try {
          const urls = await getCurrent();
          if (urls?.length && !cancelled) {
            const success = await applySetupFromUrl(urls[0]);
            if (success) {
              setSetupBanner('Connected to platform! Configuration applied automatically.');
              navigate('/settings');
              setTimeout(() => setSetupBanner(null), 8000);
            }
          }
        } catch {
          // No deep link on startup — normal launch
        }

        // Listen for deep links while app is running
        await onOpenUrl(async (urls: string[]) => {
          if (urls.length && !cancelled) {
            const success = await applySetupFromUrl(urls[0]);
            if (success) {
              setSetupBanner('Connected to platform! Configuration applied automatically.');
              navigate('/settings');
              setTimeout(() => setSetupBanner(null), 8000);
            }
          }
        });
      } catch (err) {
        console.error('[DeepLink] Plugin not available:', err);
      }
    }

    initDeepLinks();
    return () => { cancelled = true; };
  }, [navigate]);

  return (
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
        {setupBanner && (
          <div style={{
            padding: 12,
            background: '#14532d',
            borderRadius: 8,
            marginBottom: 16,
            fontSize: 14,
            color: '#4ade80',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}>
            {setupBanner}
            <button
              onClick={() => setSetupBanner(null)}
              style={{
                background: 'none',
                border: 'none',
                color: '#4ade80',
                cursor: 'pointer',
                fontSize: 16,
              }}
            >
              &times;
            </button>
          </div>
        )}
        <Routes>
          <Route path="/" element={<SyncStatus />} />
          <Route path="/folders" element={<FolderManager />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="/logs" element={<Logs />} />
        </Routes>
      </main>
    </div>
  );
}

export default function App() {
  return (
    <HashRouter>
      <AppContent />
    </HashRouter>
  );
}
