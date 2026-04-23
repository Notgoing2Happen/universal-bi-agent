import { useState, useEffect } from 'react';
import { useSidecar } from '../hooks/useSidecar';

interface Config {
  platformUrl: string;
  apiKey: string;
  googleAiKey?: string;
}

function decodeSetupToken(token: string): { platformUrl: string; apiKey: string } | null {
  try {
    const json = atob(token.trim());
    const data = JSON.parse(json);
    if (data.u && data.k) {
      return { platformUrl: data.u, apiKey: data.k };
    }
    return null;
  } catch {
    return null;
  }
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
  const [setupCode, setSetupCode] = useState('');
  const [setupError, setSetupError] = useState<string | null>(null);
  const [setupApplied, setSetupApplied] = useState(false);

  const needsSetup = connected && !config.platformUrl && !config.apiKey;

  useEffect(() => {
    if (!connected) return;
    call<Config>('config.get').then(c => { if (c) setConfig(c); }).catch(() => {});
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

  async function applySetupCode() {
    setSetupError(null);
    const decoded = decodeSetupToken(setupCode);
    if (!decoded) {
      setSetupError('Invalid setup code. Copy it from the platform download page.');
      return;
    }
    setSaving(true);
    try {
      await call('config.set', {
        platformUrl: decoded.platformUrl,
        apiKey: decoded.apiKey,
      });
      setConfig(prev => ({
        ...prev,
        platformUrl: decoded.platformUrl,
        apiKey: decoded.apiKey,
      }));
      setSetupApplied(true);
      setSetupCode('');
      setTimeout(() => setSetupApplied(false), 5000);
    } catch (err) {
      setSetupError(`Failed to apply: ${err}`);
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

      {connected === false && (
        <div style={{ padding: 12, background: '#7f1d1d', borderRadius: 6, marginBottom: 16, fontSize: 13 }}>
          Sidecar not connected. Start the sidecar process first.
        </div>
      )}

      {connected === null && (
        <div style={{ padding: 12, background: '#1e3a5f', borderRadius: 6, marginBottom: 16, fontSize: 13, color: '#60a5fa' }}>
          Connecting to sidecar...
        </div>
      )}

      {/* Quick Setup Code */}
      {connected && (
        <div style={{
          maxWidth: 500,
          marginBottom: 24,
          padding: 20,
          background: needsSetup ? '#172554' : '#1e293b',
          border: needsSetup ? '1px solid #2563eb' : '1px solid #334155',
          borderRadius: 8,
        }}>
          <h3 style={{ fontSize: 15, fontWeight: 600, marginBottom: 8, color: needsSetup ? '#93c5fd' : '#e2e8f0' }}>
            {needsSetup ? 'Quick Setup' : 'Setup Code'}
          </h3>
          <p style={{ fontSize: 12, color: '#94a3b8', marginBottom: 12 }}>
            Paste the setup code from the platform download page to connect automatically.
          </p>
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              style={{ ...inputStyle, flex: 1 }}
              type="text"
              placeholder="Paste setup code here..."
              value={setupCode}
              onChange={e => { setSetupCode(e.target.value); setSetupError(null); }}
            />
            <button
              style={{ ...buttonStyle, background: '#2563eb', color: 'white', flexShrink: 0 }}
              onClick={applySetupCode}
              disabled={saving || !setupCode.trim()}
            >
              {saving ? 'Applying...' : 'Apply'}
            </button>
          </div>
          {setupError && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#f87171' }}>{setupError}</div>
          )}
          {setupApplied && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#4ade80' }}>
              Connected! Platform URL and API key configured.
            </div>
          )}
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
