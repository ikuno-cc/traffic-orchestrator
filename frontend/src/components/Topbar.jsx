import React, { useState, useEffect } from 'react'

export default function Topbar({ lastPoll, onRefresh, loading, error }) {
  const [clock, setClock] = useState('')
  const [countdown, setCountdown] = useState(0)

  useEffect(() => {
    const t = setInterval(() => {
      setClock(new Date().toLocaleTimeString())
      if (lastPoll) {
        const elapsed = (Date.now() - lastPoll) / 1000
        setCountdown(Math.max(0, Math.ceil(4 - elapsed)))
      }
    }, 500)
    return () => clearInterval(t)
  }, [lastPoll])

  return (
    <header style={{
      height: 50, background: 'rgba(8,11,18,.96)',
      backdropFilter: 'blur(16px)',
      borderBottom: '1px solid var(--border-1)',
      display: 'flex', alignItems: 'center',
      padding: '0 20px', gap: 16, position: 'sticky',
      top: 0, zIndex: 100, flexShrink: 0,
    }}>
      <div style={{ fontSize: 10, color: 'var(--text-3)', letterSpacing: '.08em' }}>
        n8n ↔ ComfyUI ↔ Azure
      </div>

      {error && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 6,
          padding: '3px 10px', borderRadius: 6,
          background: 'rgba(251,113,133,.1)', border: '1px solid rgba(251,113,133,.3)',
          fontSize: 10, color: 'var(--rose)',
        }}>
          <span>⚠</span> API Error — {error}
        </div>
      )}

      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 16, fontSize: 11, color: 'var(--text-2)' }}>
        {loading && (
          <span style={{ color: 'var(--cyan)', fontSize: 10, animation: 'blink 1s step-end infinite' }}>
            polling…
          </span>
        )}
        {!loading && lastPoll && (
          <span style={{ color: 'var(--text-3)', fontSize: 10 }}>
            next in {countdown}s
          </span>
        )}
        <button
          onClick={onRefresh}
          style={{
            background: 'transparent', border: '1px solid var(--border-2)',
            color: 'var(--text-2)', padding: '4px 10px', borderRadius: 6,
            fontSize: 10, cursor: 'pointer', fontFamily: 'var(--font-mono)',
            transition: 'all .15s',
          }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = 'var(--cyan)'; e.currentTarget.style.color = 'var(--cyan)' }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border-2)'; e.currentTarget.style.color = 'var(--text-2)' }}
        >
          ↻ Refresh
        </button>
        <span style={{ color: 'var(--text-3)', fontVariantNumeric: 'tabular-nums' }}>{clock}</span>
      </div>
    </header>
  )
}
