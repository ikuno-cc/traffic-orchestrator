import React from 'react'

const NAV = [
  { id: 'dashboard', label: 'Dashboard',  icon: '[]', section: 'Monitor' },
  { id: 'requests',  label: 'Requests',   icon: '<>', badge: 'active'    },
  { id: 'services',  label: 'Services',   icon: '*', section: 'Config'   },
  { id: 'dispatch',  label: 'Dispatch',   icon: '->'                      },
]

export default function Sidebar({ active, onChange, stats, requests }) {
  const by = stats?.by_status || {}
  const activeCount = (by.running || 0) + (by.queued || 0) + (by.retrying || 0)
  const failedCount = by.failed || 0

  return (
    <aside style={{
      width: 200, background: 'var(--bg-1)', borderRight: '1px solid var(--border-1)',
      display: 'flex', flexDirection: 'column', flexShrink: 0,
      position: 'relative', zIndex: 10,
    }}>
      {/* Logo */}
      <div style={{
        padding: '18px 16px 14px', borderBottom: '1px solid var(--border-1)',
        display: 'flex', alignItems: 'center', gap: 10,
      }}>
        <div style={{
          width: 26, height: 26, borderRadius: 8, flexShrink: 0,
          background: 'conic-gradient(var(--cyan) 0deg, var(--violet) 120deg, var(--emerald) 240deg, var(--cyan) 360deg)',
          animation: 'spin 8s linear infinite',
        }} />
        <div>
          <div style={{ fontFamily: 'var(--font-display)', fontSize: 11, fontWeight: 700, letterSpacing: '.08em', color: 'var(--text-1)' }}>
            ORCHESTRATOR
          </div>
          <div style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.06em' }}>
            v2.0
          </div>
        </div>
      </div>

      {/* Nav items */}
      <nav style={{ padding: '10px 0', flex: 1 }}>
        {NAV.map((item) => {
          const isActive = active === item.id
          const badgeVal = item.badge === 'active' ? activeCount : null
          const hasFailed = item.badge === 'active' && failedCount > 0

          return (
            <React.Fragment key={item.id}>
              {item.section && (
                <div style={{
                  fontSize: 9, letterSpacing: '.1em', color: 'var(--text-3)',
                  padding: '12px 16px 4px', textTransform: 'uppercase',
                }}>
                  {item.section}
                </div>
              )}
              <button
                onClick={() => onChange(item.id)}
                style={{
                  display: 'flex', alignItems: 'center', gap: 9,
                  width: '100%', padding: '9px 16px',
                  background: isActive ? 'rgba(0,212,255,.06)' : 'transparent',
                  border: 'none', borderLeft: `2px solid ${isActive ? 'var(--cyan)' : 'transparent'}`,
                  color: isActive ? 'var(--cyan)' : 'var(--text-2)',
                  fontSize: 12, fontFamily: 'var(--font-mono)',
                  cursor: 'pointer', transition: 'all .15s',
                  textAlign: 'left',
                }}
                onMouseEnter={e => { if (!isActive) { e.currentTarget.style.background = 'var(--bg-2)'; e.currentTarget.style.color = 'var(--text-1)' } }}
                onMouseLeave={e => { if (!isActive) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-2)' } }}
              >
                <span style={{ width: 14, textAlign: 'center' }}>{item.icon}</span>
                <span>{item.label}</span>
                {badgeVal !== null && badgeVal > 0 && (
                  <span style={{
                    marginLeft: 'auto', fontSize: 9, fontWeight: 700,
                    padding: '1px 6px', borderRadius: 10,
                    background: hasFailed ? 'rgba(251,113,133,.2)' : 'rgba(167,139,250,.2)',
                    color: hasFailed ? 'var(--rose)' : 'var(--violet)',
                    minWidth: 18, textAlign: 'center',
                  }}>
                    {badgeVal}
                  </span>
                )}
              </button>
            </React.Fragment>
          )
        })}
      </nav>

      {/* Bottom: health indicator */}
      <div style={{
        padding: '10px 16px', borderTop: '1px solid var(--border-1)',
        fontSize: 10, color: 'var(--text-3)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{
            width: 6, height: 6, borderRadius: '50%',
            background: 'var(--emerald)',
            animation: 'pulse-dot 2s ease infinite',
            boxShadow: '0 0 6px var(--emerald)',
          }} />
          API connected
        </div>
      </div>
    </aside>
  )
}

