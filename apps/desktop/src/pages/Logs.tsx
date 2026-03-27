import { useState, useEffect, useRef } from 'react';
import { useSidecar } from '../hooks/useSidecar';

interface LogEntry {
  timestamp: string;
  level: 'info' | 'warn' | 'error';
  message: string;
}

export default function Logs() {
  const { connected, onEvent } = useSidecar();
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!connected) return;

    // Listen for log events from sidecar
    const unsub1 = onEvent('event.log', (params) => {
      setLogs(prev => [
        ...prev.slice(-499),
        params as unknown as LogEntry,
      ]);
    });

    // Also show file change detections
    const unsub2 = onEvent('event.fileChanged', (params) => {
      const p = params as Record<string, unknown>;
      setLogs(prev => [
        ...prev.slice(-499),
        {
          timestamp: (p.timestamp as string) || new Date().toISOString(),
          level: 'info' as const,
          message: `File ${p.type}: ${p.name}`,
        },
      ]);
    });

    return () => { unsub1(); unsub2(); };
  }, [connected, onEvent]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  function handleClear() {
    setLogs([]);
  }

  const levelColor = (level: string) =>
    level === 'error' ? '#f87171' : level === 'warn' ? '#fbbf24' : '#94a3b8';

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <h2 style={{ fontSize: 20, fontWeight: 600 }}>Activity Log</h2>
        <button
          onClick={handleClear}
          style={{
            padding: '4px 12px',
            borderRadius: 4,
            border: '1px solid #334155',
            background: 'transparent',
            color: '#94a3b8',
            fontSize: 12,
            cursor: 'pointer',
          }}
        >
          Clear
        </button>
      </div>

      <div style={{
        background: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 12,
        height: 'calc(100vh - 120px)',
        overflowY: 'auto',
        fontFamily: 'Consolas, Monaco, monospace',
        fontSize: 12,
        lineHeight: 1.6,
      }}>
        {logs.length === 0 && (
          <div style={{ color: '#475569', textAlign: 'center', paddingTop: 40 }}>
            No log entries yet. Activity will appear here.
          </div>
        )}
        {logs.map((entry, i) => (
          <div key={i} style={{ display: 'flex', gap: 8 }}>
            <span style={{ color: '#475569', flexShrink: 0 }}>
              {new Date(entry.timestamp).toLocaleTimeString()}
            </span>
            <span style={{ color: levelColor(entry.level), flexShrink: 0, width: 40 }}>
              [{entry.level.toUpperCase()}]
            </span>
            <span style={{ color: '#e2e8f0' }}>{entry.message}</span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
