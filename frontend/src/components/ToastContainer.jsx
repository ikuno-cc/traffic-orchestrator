import React from 'react'

const ICONS = {
  success: '✓',
  error:   '✕',
  info:    'ℹ',
  warn:    '⚠',
}

const COLORS = {
  success: '#34d399',
  error:   '#fb7185',
  info:    '#00d4ff',
  warn:    '#fbbf24',
}

export default function ToastContainer({ toasts, onRemove }) {
  return (
    <div style={{
      position: 'fixed', bottom: 24, right: 24,
      display: 'flex', flexDirection: 'column', gap: 8,
      zIndex: 9999, pointerEvents: 'none',
    }}>
      {toasts.map(t => (
        <div
          key={t.id}
          onClick={() => onRemove(t.id)}
          style={{
            display: 'flex', alignItems: 'center', gap: 10,
            padding: '10px 14px', borderRadius: 8,
            background: 'rgba(14,18,28,.97)',
            border: `1px solid ${COLORS[t.type] || COLORS.info}33`,
            borderLeft: `3px solid ${COLORS[t.type] || COLORS.info}`,
            fontSize: 11, color: '#e2e8f4',
            fontFamily: 'DM Mono, monospace',
            minWidth: 260, maxWidth: 380,
            boxShadow: '0 8px 32px rgba(0,0,0,.5)',
            pointerEvents: 'all', cursor: 'pointer',
            animation: 'toast-in .25s ease both',
          }}
        >
          <span style={{ color: COLORS[t.type], fontSize: 14, flexShrink: 0 }}>
            {ICONS[t.type] || 'ℹ'}
          </span>
          <span style={{ lineHeight: 1.4 }}>{t.message}</span>
        </div>
      ))}
    </div>
  )
}
