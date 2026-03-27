import { useState, useEffect } from 'react';
import { useSidecar } from '../hooks/useSidecar';

interface Config {
  platformUrl: string;
  apiKey: string;
  googleAiKey?: string;
}

export default function Settings() {
  const { call, connected } = useSidecar();
  const [config, setConfig] = useState<Config>({
    platformUrl: '',
    apiKey: '',
    googleAiKey: '',
  });
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<string | null>(null);

  useEffect(() => {
    if (!connected) return;
    call<Config>('config.get').then(setConfig).catch(() => {});
  }, [connected, call]);

  async function handleSave() {
    setSaving(true);
    setSaved(false);
    try {
      await call('config.set', config as unknown as Record<string, unknown>);
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (err) {
      setTestResult(`Save failed: ${err}`);
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setTestResult(null);
    try {
      const result = await call<{ ok: boolean; message: string }>('config.test');
      setTestResult(result.ok ? 'Connected successfully!' : `Failed: ${result.message}`);
    } catch (err) {
      setTestResult(`Test failed: ${err}`);
    } finally {
      setTesting(false);
    }
  }

  const inputStyle: React.CSSProperties = {
    width: '100%',
    padding: '8px 12px',
    background: '#1e293b',
    border: '1px solid #334155',
    borderRadius: 6,
    color: '#e2e8f0',
    fontSize: 14,
    outline: 'none',
  };

  const labelStyle: React.CSSProperties = {
    display: 'block',
    marginBottom: 6,
    fontSize: 13,
    color: '#94a3b8',
    fontWeight: 500,
  };

  const buttonStyle: React.CSSProperties = {
    padding: '8px 20px',
    borderRadius: 6,
    border: 'none',
    fontSize: 14,
    fontWeight: 500,
    cursor: 'pointer',
  };

  return (
    <div>
      <h2 style={{ fontSize: 20, fontWeight: 600, marginBottom: 24 }}>Settings</h2>

      {!connected && (
        <div style={{ padding: 12, background: '#7f1d1d', borderRadius: 6, marginBottom: 16, fontSize: 13 }}>
          Sidecar not connected. Start the sidecar process first.
        </div>
      )}

      <div style={{ maxWidth: 500, display: 'flex', flexDirection: 'column', gap: 20 }}>
        <div>
          <label style={labelStyle}>Platform URL</label>
          <input
            style={inputStyle}
            type="url"
            placeholder="https://app.univintel.com"
            value={config.platformUrl}
            onChange={e => setConfig({ ...config, platformUrl: e.target.value })}
          />
        </div>

        <div>
          <label style={labelStyle}>Agent API Key</label>
          <input
            style={inputStyle}
            type="password"
            placeholder="ubi_..."
            value={config.apiKey}
            onChange={e => setConfig({ ...config, apiKey: e.target.value })}
          />
          <span style={{ fontSize: 11, color: '#64748b', marginTop: 4, display: 'block' }}>
            Generate from Platform → Settings → Agent Keys
          </span>
        </div>

        <div>
          <label style={labelStyle}>Google AI API Key (optional)</label>
          <input
            style={inputStyle}
            type="password"
            placeholder="AIza..."
            value={config.googleAiKey ?? ''}
            onChange={e => setConfig({ ...config, googleAiKey: e.target.value })}
          />
          <span style={{ fontSize: 11, color: '#64748b', marginTop: 4, display: 'block' }}>
            For local AI column mapping. Get from ai.google.dev
          </span>
        </div>

        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
          <button
            style={{ ...buttonStyle, background: '#2563eb', color: 'white' }}
            onClick={handleSave}
            disabled={saving}
          >
            {saving ? 'Saving...' : saved ? 'Saved!' : 'Save'}
          </button>
          <button
            style={{ ...buttonStyle, background: '#334155', color: '#e2e8f0' }}
            onClick={handleTest}
            disabled={testing || !config.platformUrl || !config.apiKey}
          >
            {testing ? 'Testing...' : 'Test Connection'}
          </button>
        </div>

        {testResult && (
          <div style={{
            padding: 10,
            borderRadius: 6,
            fontSize: 13,
            background: testResult.startsWith('Connected') ? '#14532d' : '#7f1d1d',
          }}>
            {testResult}
          </div>
        )}
      </div>
    </div>
  );
}
