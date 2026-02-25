import React from 'react'

const CONFIG = {
  running:   { label: 'Running',   color: '#00d4ff', bg: 'rgba(0,212,255,.1)',   dot: true  },
  queued:    { label: 'Queued',    color: '#a78bfa', bg: 'rgba(167,139,250,.1)', dot: false },
  success:   { label: 'Success',   color: '#34d399', bg: 'rgba(52,211,153,.1)',  dot: false },
  failed:    { label: 'Failed',    color: '#fb7185', bg: 'rgba(251,113,133,.1)', dot: false },
  retrying:  { label: 'Retrying',  color: '#fbbf24', bg: 'rgba(251,191,36,.1)',  dot: true  },
  paused:    { label: 'Paused',    color: '#fde68a', bg: 'rgba(253,230,138,.1)', dot: false },
  cancelled: { label: 'Cancelled', color: '#4a5568', bg: 'rgba(74,85,104,.15)',  dot: false },
}

export default function StatusBadge({ status, size = 'md' }) {
  const cfg = CONFIG[status] || { label: status, color: '#4a5568', bg: 'rgba(74,85,104,.15)', dot: false }
  const fs = size === 'sm' ? '9px' : size === 'lg' ? '12px' : '10px'
  const px = size === 'sm' ? '5px' : '8px'
  const py = size === 'sm' ? '1px' : '3px'

  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: '5px',
      padding: `${py} ${px}`, borderRadius: '20px',
      background: cfg.bg, color: cfg.color,
      fontSize: fs, fontWeight: 700, letterSpacing: '.03em',
      fontFamily: 'DM Mono, monospace', whiteSpace: 'nowrap',
    }}>
      {cfg.dot && (
        <span style={{
          width: 5, height: 5, borderRadius: '50%',
          background: cfg.color, flexShrink: 0,
          animation: 'pulse-dot 1.5s ease infinite',
          boxShadow: `0 0 6px ${cfg.color}`,
        }} />
      )}
      {cfg.label}
    </span>
  )
}
