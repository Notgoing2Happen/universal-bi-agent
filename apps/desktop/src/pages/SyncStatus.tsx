import { useState, useEffect, useCallback } from 'react';
import { useSidecar } from '../hooks/useSidecar';

interface SyncFile {
  path: string;
  size: number;
  lastSynced: string | null;
  status: 'synced' | 'pending' | 'error' | 'watching';
}

interface SyncStatusData {
  watching: boolean;
  files: SyncFile[];
  totalFiles: number;
  syncedFiles: number;
  errorFiles: number;
}

export default function SyncStatus() {
  const { call, connected, onEvent } = useSidecar();
  const [status, setStatus] = useState<SyncStatusData | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [recentActivity, setRecentActivity] = useState<{ name: string; stage: string; time: string }[]>([]);

  const refresh = useCallback(() => {
    if (!connected) return;
    call<SyncStatusData>('sync.status').then(setStatus).catch(() => {});
  }, [connected, call]);

  useEffect(() => {
    refresh();

    // Listen for sync progress events (upload complete/error)
    const unsub1 = onEvent('event.syncProgress', (params: Record<string, unknown>) => {
      setRecentActivity(prev => [
        {
          name: params.name as string,
          stage: params.stage as string,
          time: new Date().toLocaleTimeString(),
        },
        ...prev,
      ].slice(0, 10));
      // Refresh file list after sync completes
      if (params.stage === 'synced' || params.stage === 'error' || params.stage === 'unchanged') {
        refresh();
      }
    });

    // Listen for file change events (add/change/delete detected)
    const unsub2 = onEvent('event.fileChanged', () => {
      refresh();
    });

    return () => { unsub1(); unsub2(); };
  }, [connected, call, onEvent, refresh]);

  async function handleSyncNow() {
    setSyncing(true);
    try {
      await call('sync.oneShot');
      refresh();
    } catch {
      // ignore
    } finally {
      setSyncing(false);
    }
  }

  async function toggleWatch() {
    try {
      if (status?.watching) {
        await call('sync.stop');
      } else {
        await call('sync.start');
      }
      refresh();
    } catch {
      // Sidecar not running — ignore
    }
  }

  const badgeStyle = (s: string): React.CSSProperties => ({
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: 10,
    fontSize: 11,
    fontWeight: 600,
    background: s === 'synced' ? '#14532d' : s === 'error' ? '#7f1d1d' : s === 'watching' ? '#1e3a5f' : '#44403c',
    color: s === 'synced' ? '#4ade80' : s === 'error' ? '#f87171' : s === 'watching' ? '#38bdf8' : '#a8a29e',
  });

  const stageBadge = (stage: string) => {
    const colors: Record<string, { bg: string; fg: string }> = {
      synced: { bg: '#14532d', fg: '#4ade80' },
      uploading: { bg: '#1e3a5f', fg: '#38bdf8' },
      error: { bg: '#7f1d1d', fg: '#f87171' },
      unchanged: { bg: '#44403c', fg: '#a8a29e' },
    };
    const c = colors[stage] || colors.unchanged;
    return { background: c.bg, color: c.fg, padding: '1px 6px', borderRadius: 8, fontSize: 10, fontWeight: 600 } as React.CSSProperties;
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <h2 style={{ fontSize: 20, fontWeight: 600 }}>Sync Status</h2>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            onClick={toggleWatch}
            style={{
              padding: '6px 16px',
              borderRadius: 6,
              border: 'none',
              fontSize: 13,
              cursor: 'pointer',
              background: status?.watching ? '#7f1d1d' : '#14532d',
              color: status?.watching ? '#f87171' : '#4ade80',
            }}
          >
            {status?.watching ? 'Stop Watching' : 'Start Watching'}
          </button>
          <button
            onClick={handleSyncNow}
            disabled={syncing}
            style={{
              padding: '6px 16px',
              borderRadius: 6,
              border: 'none',
              fontSize: 13,
              cursor: 'pointer',
              background: '#2563eb',
              color: 'white',
            }}
          >
            {syncing ? 'Syncing...' : 'Sync Now'}
          </button>
        </div>
      </div>

      {!connected && (
        <div style={{ padding: 12, background: '#7f1d1d', borderRadius: 6, marginBottom: 16, fontSize: 13 }}>
          Sidecar not connected.
        </div>
      )}

      {/* Summary */}
      {status && (
        <div style={{ display: 'flex', gap: 16, marginBottom: 24 }}>
          {[
            { label: 'Total Files', value: status.totalFiles, color: '#e2e8f0' },
            { label: 'Synced', value: status.syncedFiles, color: '#4ade80' },
            { label: 'Errors', value: status.errorFiles, color: '#f87171' },
          ].map(s => (
            <div key={s.label} style={{
              padding: '12px 20px',
              background: '#1e293b',
              borderRadius: 8,
              minWidth: 100,
            }}>
              <div style={{ fontSize: 24, fontWeight: 700, color: s.color }}>{s.value}</div>
              <div style={{ fontSize: 12, color: '#94a3b8' }}>{s.label}</div>
            </div>
          ))}
          <div style={{
            padding: '12px 20px',
            background: '#1e293b',
            borderRadius: 8,
            minWidth: 100,
          }}>
            <div style={{
              fontSize: 24,
              fontWeight: 700,
              color: status.watching ? '#38bdf8' : '#64748b',
            }}>
              {status.watching ? 'ON' : 'OFF'}
            </div>
            <div style={{ fontSize: 12, color: '#94a3b8' }}>Watcher</div>
          </div>
        </div>
      )}

      {/* Recent Activity */}
      {recentActivity.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <h3 style={{ fontSize: 14, color: '#64748b', fontWeight: 500, marginBottom: 8 }}>Recent Activity</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {recentActivity.map((a, i) => (
              <div key={`${a.name}-${i}`} style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                padding: '6px 12px',
                background: '#0f172a',
                borderRadius: 6,
                fontSize: 12,
              }}>
                <span style={{ color: '#e2e8f0' }}>{a.name}</span>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <span style={stageBadge(a.stage)}>{a.stage}</span>
                  <span style={{ color: '#64748b', fontSize: 11 }}>{a.time}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* File list */}
      <div style={{ background: '#1e293b', borderRadius: 8, overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #334155' }}>
              <th style={{ textAlign: 'left', padding: '10px 12px', color: '#94a3b8', fontWeight: 500 }}>File</th>
              <th style={{ textAlign: 'right', padding: '10px 12px', color: '#94a3b8', fontWeight: 500 }}>Size</th>
              <th style={{ textAlign: 'left', padding: '10px 12px', color: '#94a3b8', fontWeight: 500 }}>Last Synced</th>
              <th style={{ textAlign: 'center', padding: '10px 12px', color: '#94a3b8', fontWeight: 500 }}>Status</th>
            </tr>
          </thead>
          <tbody>
            {status?.files.length === 0 && (
              <tr>
                <td colSpan={4} style={{ padding: 24, textAlign: 'center', color: '#64748b' }}>
                  No files tracked yet. Add a watch folder to get started.
                </td>
              </tr>
            )}
            {status?.files.map(f => (
              <tr key={f.path} style={{ borderBottom: '1px solid #1e293b' }}>
                <td style={{ padding: '8px 12px', color: '#e2e8f0' }}>
                  {f.path.split(/[/\\]/).pop()}
                </td>
                <td style={{ padding: '8px 12px', textAlign: 'right', color: '#94a3b8' }}>
                  {(f.size / 1024).toFixed(1)} KB
                </td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>
                  {f.lastSynced ? new Date(f.lastSynced).toLocaleString() : '—'}
                </td>
                <td style={{ padding: '8px 12px', textAlign: 'center' }}>
                  <span style={badgeStyle(f.status)}>{f.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
