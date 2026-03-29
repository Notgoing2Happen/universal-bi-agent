import { useState, useEffect } from 'react';
import { useSidecar } from '../hooks/useSidecar';
import DropZone, { type FileEntry } from '../components/DropZone';

interface WatchFolder {
  path: string;
  extensions: string[];
  recursive: boolean;
  addedAt: string;
}

export default function FolderManager() {
  const { call, connected } = useSidecar();
  const [folders, setFolders] = useState<WatchFolder[]>([]);
  const [, setImportedFiles] = useState<FileEntry[]>([]);

  useEffect(() => {
    if (!connected) return;
    call<WatchFolder[]>('folders.list').then(f => setFolders(f || [])).catch(() => {});
  }, [connected, call]);

  async function handleAddFolder() {
    try {
      let folderPath: string | null = null;

      try {
        const { open } = await import('@tauri-apps/plugin-dialog');
        const selected = await open({
          directory: true,
          multiple: false,
          title: 'Select folder to watch for data files',
        });
        folderPath = selected as string | null;
      } catch {
        // Fallback for browser dev
        folderPath = prompt('Enter folder path to watch:');
      }

      if (!folderPath) return;

      await call('folders.add', {
        path: folderPath,
        extensions: ['.csv', '.xlsx', '.xls', '.json', '.tsv'],
        recursive: true,
      });
      const updated = await call<WatchFolder[]>('folders.list');
      setFolders(updated || []);
    } catch (err) {
      console.error('Failed to add folder:', err);
    }
  }

  async function handleSelectSaveFolder() {
    try {
      let folderPath: string | null = null;

      try {
        const { open } = await import('@tauri-apps/plugin-dialog');
        const selected = await open({
          directory: true,
          multiple: false,
          title: 'Select where to save synced documents',
        });
        folderPath = selected as string | null;
      } catch {
        folderPath = prompt('Enter save folder path:');
      }

      if (!folderPath) return;

      await call('config.set', { saveFolder: folderPath });
    } catch (err) {
      console.error('Failed to set save folder:', err);
    }
  }

  async function handleRemove(path: string) {
    await call('folders.remove', { path });
    const updated = await call<WatchFolder[]>('folders.list');
    setFolders(updated || []);
  }

  async function handleFilesDropped(files: FileEntry[]) {
    setImportedFiles(prev => [...files, ...prev].slice(0, 20));

    // Send each dropped file to the sidecar for processing
    for (const file of files) {
      try {
        await call('sync.importFile', { path: file.path, name: file.name });
      } catch (err) {
        console.error(`Failed to import ${file.name}:`, err);
      }
    }
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <h2 style={{ fontSize: 20, fontWeight: 600 }}>Folders & Import</h2>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            onClick={handleSelectSaveFolder}
            disabled={!connected}
            style={{
              padding: '6px 16px',
              borderRadius: 6,
              border: '1px solid #334155',
              fontSize: 13,
              cursor: 'pointer',
              background: 'transparent',
              color: '#94a3b8',
            }}
          >
            Set Save Folder
          </button>
          <button
            onClick={handleAddFolder}
            disabled={!connected}
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
            + Watch Folder
          </button>
        </div>
      </div>

      {/* Drag and drop zone */}
      <DropZone
        onFilesDropped={handleFilesDropped}
        style={{ marginBottom: 24 }}
      />

      {/* Watch folders list */}
      <div style={{ marginBottom: 8 }}>
        <h3 style={{ fontSize: 14, color: '#64748b', fontWeight: 500, marginBottom: 12 }}>
          Watched Folders ({folders.length})
        </h3>
      </div>

      {folders.length === 0 ? (
        <div style={{
          padding: 24,
          textAlign: 'center',
          background: '#1e293b',
          borderRadius: 8,
          color: '#64748b',
          fontSize: 13,
        }}>
          No folders being watched. Click "Watch Folder" to select a folder,
          or drag & drop files above.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {folders.map(f => (
            <div key={f.path} style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '12px 16px',
              background: '#1e293b',
              borderRadius: 8,
            }}>
              <div>
                <div style={{ fontSize: 14, color: '#e2e8f0', fontFamily: 'monospace' }}>
                  {f.path}
                </div>
                <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>
                  {(f.extensions || []).join(', ')} {f.recursive ? '· Recursive' : ''}
                </div>
              </div>
              <button
                onClick={() => handleRemove(f.path)}
                style={{
                  padding: '4px 12px',
                  borderRadius: 4,
                  border: '1px solid #7f1d1d',
                  background: 'transparent',
                  color: '#f87171',
                  fontSize: 12,
                  cursor: 'pointer',
                }}
              >
                Remove
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
