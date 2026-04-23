import { useState, useCallback, useRef, useEffect } from 'react';

const isTauri = typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window;

interface DropZoneProps {
  onFilesDropped: (files: FileEntry[]) => void;
  acceptedExtensions?: string[];
  style?: React.CSSProperties;
}

export interface FileEntry {
  name: string;
  path: string;
  size: number;
  type: string;
}

/**
 * Drag-and-drop zone for importing files.
 *
 * In Tauri: uses @tauri-apps/api/event to listen for file drops (browser
 * drag events don't provide file paths in Tauri's WebView).
 * In browser: uses standard HTML5 drag-and-drop API.
 */
export default function DropZone({ onFilesDropped, acceptedExtensions, style }: DropZoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [recentFiles, setRecentFiles] = useState<FileEntry[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const exts = acceptedExtensions ?? ['.csv', '.xlsx', '.xls', '.json', '.tsv', '.parquet'];

  // Tauri drag-and-drop: listen for native file drop events
  useEffect(() => {
    if (!isTauri) return;

    let unlisten: (() => void) | undefined;

    async function setupTauriDrop() {
      try {
        const { listen } = await import('@tauri-apps/api/event');

        // Tauri v2 drag-drop events
        const unlistenDrop = await listen<{ paths: string[] }>('tauri://drag-drop', (event) => {
          setIsDragging(false);
          const paths = event.payload.paths || [];
          const entries: FileEntry[] = [];
          for (const p of paths) {
            const name = p.split(/[/\\]/).pop() || p;
            const ext = '.' + name.split('.').pop()?.toLowerCase();
            if (exts.includes(ext)) {
              entries.push({ name, path: p, size: 0, type: ext });
            }
          }
          if (entries.length > 0) {
            setRecentFiles(prev => [...entries, ...prev].slice(0, 10));
            onFilesDropped(entries);
          }
        });

        const unlistenEnter = await listen('tauri://drag-enter', () => setIsDragging(true));
        const unlistenLeave = await listen('tauri://drag-leave', () => setIsDragging(false));

        unlisten = () => {
          unlistenDrop();
          unlistenEnter();
          unlistenLeave();
        };
      } catch (err) {
        console.error('[DropZone] Failed to set up Tauri drag-drop:', err);
      }
    }

    setupTauriDrop();
    return () => { unlisten?.(); };
  }, [exts, onFilesDropped]);

  // Browser drag-and-drop handlers (non-Tauri only)
  const handleDragOver = useCallback((e: React.DragEvent) => {
    if (isTauri) return; // Tauri handles this natively
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    if (isTauri) return;
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  }, []);

  const processFiles = useCallback((fileList: FileList | null) => {
    if (!fileList || fileList.length === 0) return;

    const entries: FileEntry[] = [];
    for (let i = 0; i < fileList.length; i++) {
      const file = fileList[i];
      const ext = '.' + file.name.split('.').pop()?.toLowerCase();
      if (exts.includes(ext)) {
        entries.push({
          name: file.name,
          path: (file as unknown as { path?: string }).path ?? file.name,
          size: file.size,
          type: ext,
        });
      }
    }

    if (entries.length > 0) {
      setRecentFiles(prev => [...entries, ...prev].slice(0, 10));
      onFilesDropped(entries);
    }
  }, [exts, onFilesDropped]);

  const handleDrop = useCallback((e: React.DragEvent) => {
    if (isTauri) return; // Tauri handles this natively
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    processFiles(e.dataTransfer.files);
  }, [processFiles]);

  const handleClick = async () => {
    if (isTauri) {
      // Use Tauri's native file dialog — provides full filesystem paths
      try {
        const { open } = await import('@tauri-apps/plugin-dialog');
        const selected = await open({
          multiple: true,
          title: 'Select data files to import',
          filters: [{
            name: 'Data Files',
            extensions: exts.map(e => e.replace('.', '')),
          }],
        });

        if (!selected) return;
        const paths = Array.isArray(selected) ? selected : [selected];
        const entries: FileEntry[] = paths.map(p => {
          const name = p.split(/[/\\]/).pop() || p;
          const ext = '.' + name.split('.').pop()?.toLowerCase();
          return { name, path: p, size: 0, type: ext };
        });

        if (entries.length > 0) {
          setRecentFiles(prev => [...entries, ...prev].slice(0, 10));
          onFilesDropped(entries);
        }
      } catch (err) {
        console.error('[DropZone] Tauri file dialog failed:', err);
      }
    } else {
      // Browser fallback
      fileInputRef.current?.click();
    }
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    processFiles(e.target.files);
    // Reset so same file can be selected again
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <div style={style}>
      {/* Drop zone */}
      <div
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onClick={handleClick}
        style={{
          border: isDragging ? '2px solid #3b82f6' : '2px dashed #334155',
          borderRadius: 12,
          padding: '40px 24px',
          textAlign: 'center',
          cursor: 'pointer',
          background: isDragging ? 'rgba(59, 130, 246, 0.08)' : '#1e293b',
          transition: 'all 0.2s',
        }}
      >
        <div style={{
          fontSize: 40,
          marginBottom: 12,
          opacity: isDragging ? 1 : 0.6,
          transition: 'opacity 0.2s',
        }}>
          {isDragging ? '📥' : '📁'}
        </div>
        <div style={{ fontSize: 15, fontWeight: 500, color: '#e2e8f0', marginBottom: 6 }}>
          {isDragging ? 'Drop files here' : 'Drag & drop files here'}
        </div>
        <div style={{ fontSize: 13, color: '#64748b', marginBottom: 12 }}>
          or click to browse
        </div>
        <div style={{ fontSize: 11, color: '#475569' }}>
          Accepts: {exts.join(', ')}
        </div>
      </div>

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept={exts.join(',')}
        onChange={handleFileInput}
        style={{ display: 'none' }}
      />

      {/* Recent dropped files */}
      {recentFiles.length > 0 && (
        <div style={{ marginTop: 16 }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>Recently added</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {recentFiles.map((f, i) => (
              <div
                key={`${f.name}-${i}`}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '6px 12px',
                  background: '#0f172a',
                  borderRadius: 6,
                  fontSize: 12,
                }}
              >
                <span style={{ color: '#e2e8f0' }}>
                  <span style={{ color: '#3b82f6', marginRight: 6 }}>
                    {f.type === '.csv' ? '📊' : f.type === '.json' ? '📋' : '📑'}
                  </span>
                  {f.name}
                </span>
                <span style={{ color: '#64748b' }}>{formatSize(f.size)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
