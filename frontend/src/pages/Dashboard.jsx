import React, { useState, useEffect } from 'react'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, BarChart, Bar, Cell } from 'recharts'
import StatusBadge from '../components/StatusBadge'
import { parseApiDate } from '../utils/datetime'

function StatCard({ label, value, sub, color, icon }) {
  return (
    <div style={{
      background: 'var(--bg-2)', border: '1px solid var(--border-1)',
      borderRadius: 'var(--radius-md)', padding: '16px 18px',
      position: 'relative', overflow: 'hidden',
      animation: 'slide-up .3s ease both',
    }}>
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0, height: 2,
        background: color,
        boxShadow: `0 0 12px ${color}66`,
      }} />
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <div style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.1em', textTransform: 'uppercase', marginBottom: 6 }}>
            {label}
          </div>
          <div style={{
            fontFamily: 'var(--font-display)', fontSize: 34, fontWeight: 700,
            color, lineHeight: 1, letterSpacing: '-.02em',
          }}>
            {value}
          </div>
          {sub && (
            <div style={{ fontSize: 9, color: 'var(--text-3)', marginTop: 5 }}>
              {sub}
            </div>
          )}
        </div>
        <div style={{ fontSize: 20, opacity: .15 }}>{icon}</div>
      </div>
    </div>
  )
}

function elapsed(startIso, endIso = null) {
  const start = parseApiDate(startIso)
  const end = endIso ? parseApiDate(endIso) : new Date()
  if (!start || Number.isNaN(start.getTime())) return '0s'
  if (!end || Number.isNaN(end.getTime())) return '0s'
  const diff = Math.max(0, end - start) / 1000
  if (diff < 60) return `${Math.floor(diff)}s`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ${Math.floor(diff % 60)}s`
  return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m`
}

function timeAgo(iso) {
  if (!iso) return '—'
  const parsed = parseApiDate(iso)
  if (!parsed || Number.isNaN(parsed.getTime())) return '-'
  const diff = (Date.now() - parsed) / 1000
  if (diff < 5) return 'just now'
  if (diff < 60) return `${Math.floor(diff)}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

function shortenError(err) {
  if (!err) return 'Unknown error'
  return err.replace(/Traceback[\s\S]*$/, '').trim().slice(0, 90) + (err.length > 90 ? '…' : '')
}

const STATUS_COLORS = {
  running: '#00d4ff', queued: '#a78bfa', success: '#34d399',
  failed: '#fb7185', retrying: '#fbbf24', paused: '#fde68a', cancelled: '#4a5568',
}

function WorkerControl({ workers }) {
  return (
    <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', letterSpacing: '.04em' }}>WORKER CONTROL</div>
        <span style={{ fontSize: 10, color: 'var(--text-3)' }}>
          {workers?.online_workers || 0} online
        </span>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, fontSize: 10, color: 'var(--text-3)' }}>
        <span>Total configured workers: {workers?.total_concurrency || 0}</span>
        <span>Services: {workers?.workers?.length || 0}</span>
      </div>

      <div style={{ fontSize: 10, color: 'var(--text-3)' }}>
        Worker count is edited per service from the Services page.
      </div>
    </div>
  )
}


export default function Dashboard({ requests, services, stats, serviceStats, workers, refresh, toast }) {
  const [tick, setTick] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setTick(v => v + 1), 1000)
    return () => clearInterval(t)
  }, [])

  const by = stats?.by_status || {}
  const running  = (by.running || 0) + (by.retrying || 0)
  const queued   = by.queued || 0
  const success  = by.success || 0
  const failed   = by.failed || 0
  const total    = stats?.total_requests || 0
  const successRate = total > 0 ? Math.round(success / total * 100) : 0

  // Stuck detection: running > 5 min
  const stuck = requests.filter(r => r.status === 'running' && r.durationMs > 5 * 60 * 1000)

  // Throughput chart data (requests per minute buckets over last 20 mins)
  const buckets = Array.from({ length: 20 }, (_, i) => {
    const ago = (19 - i) * 60 * 1000
    const label = `${19 - i}m`
    const count = requests.filter(r => {
      const created = parseApiDate(r.created_at)
      const diff = Date.now() - created
      return diff >= ago && diff < ago + 60 * 1000
    }).length
    return { label, count }
  })

  // Status distribution for bar chart
  const statusDist = Object.entries(by)
    .filter(([, v]) => v > 0)
    .map(([k, v]) => ({ name: k, value: v, color: STATUS_COLORS[k] || '#4a5568' }))

  // Recent 15 requests for feed
  const feed = [...requests]
    .sort((a, b) => parseApiDate(b.updated_at) - parseApiDate(a.updated_at))
    .slice(0, 15)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20, animation: 'fade-in .2s ease' }}>
      <div>
        <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 700, letterSpacing: '-.01em' }}>
          System <span style={{ color: 'var(--cyan)' }}>Overview</span>
        </h1>
        <p style={{ fontSize: 11, color: 'var(--text-3)', marginTop: 2 }}>
          Real-time traffic monitoring � polling every 4s
        </p>
      </div>

      {/* Stuck banner */}
      {stuck.length > 0 && (
        <div style={{
          background: 'rgba(251,191,36,.07)', border: '1px solid rgba(251,191,36,.3)',
          borderRadius: 'var(--radius-md)', padding: '10px 16px',
          display: 'flex', alignItems: 'center', gap: 10, fontSize: 11, color: '#fbbf24',
          animation: 'fade-in .2s ease',
        }}>
          <span style={{ fontSize: 16 }}>⚠</span>
          <span>
            <strong>{stuck.length} request{stuck.length > 1 ? 's' : ''}</strong> {stuck.length > 1 ? 'have' : 'has'} been running for over 5 minutes
            — may be frozen. Check the <strong>Requests</strong> tab.
          </span>
        </div>
      )}

      {/* Stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>
        <StatCard label="Total"     value={total}        color="var(--cyan)"    icon="◈" sub={`${stats?.active_services || 0} services`} />
        <StatCard label="Running"   value={running}      color="var(--cyan)"    icon="▶" sub={by.retrying > 0 ? `${by.retrying} retrying` : 'all healthy'} />
        <StatCard label="Queued"    value={queued}       color="var(--violet)"  icon="◷" sub={by.paused > 0 ? `${by.paused} paused` : ''} />
        <StatCard label="Success"   value={success}      color="var(--emerald)" icon="✓" sub={`${successRate}% success rate`} />
        <StatCard label="Failed"    value={failed}       color="var(--rose)"    icon="✕" sub={failed > 0 ? 'click Requests' : 'all clear'} />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {/* Throughput chart */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', padding: '16px', overflow: 'hidden' }}>
          <div style={{ fontSize: 11, fontWeight: 700, marginBottom: 14, color: 'var(--text-2)', letterSpacing: '.04em' }}>
            REQUEST THROUGHPUT <span style={{ fontWeight: 400, color: 'var(--text-3)' }}>last 20 min</span>
          </div>
          <ResponsiveContainer width="100%" height={120}>
            <AreaChart data={buckets} margin={{ top: 0, right: 0, left: -30, bottom: 0 }}>
              <defs>
                <linearGradient id="cyanGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor="#00d4ff" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#00d4ff" stopOpacity={0}    />
                </linearGradient>
              </defs>
              <XAxis dataKey="label" tick={{ fontSize: 8, fill: '#3d4e65' }} tickLine={false} axisLine={false} interval={4} />
              <YAxis tick={{ fontSize: 8, fill: '#3d4e65' }} tickLine={false} axisLine={false} allowDecimals={false} />
              <Tooltip
                contentStyle={{ background: '#0c1018', border: '1px solid #1e2a40', borderRadius: 6, fontSize: 10 }}
                labelStyle={{ color: '#7c8ea8' }} itemStyle={{ color: '#00d4ff' }}
              />
              <Area type="monotone" dataKey="count" stroke="#00d4ff" strokeWidth={1.5} fill="url(#cyanGrad)" dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Status distribution */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', padding: '16px' }}>
          <div style={{ fontSize: 11, fontWeight: 700, marginBottom: 14, color: 'var(--text-2)', letterSpacing: '.04em' }}>
            STATUS DISTRIBUTION
          </div>
          {statusDist.length === 0 ? (
            <div style={{ height: 120, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-3)', fontSize: 11 }}>
              No data yet
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={120}>
              <BarChart data={statusDist} margin={{ top: 0, right: 0, left: -30, bottom: 0 }}>
                <XAxis dataKey="name" tick={{ fontSize: 8, fill: '#3d4e65' }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fontSize: 8, fill: '#3d4e65' }} tickLine={false} axisLine={false} allowDecimals={false} />
                <Tooltip
                  contentStyle={{ background: '#0c1018', border: '1px solid #1e2a40', borderRadius: 6, fontSize: 10 }}
                  labelStyle={{ color: '#7c8ea8' }}
                />
                <Bar dataKey="value" radius={[3, 3, 0, 0]}>
                  {statusDist.map((entry, i) => <Cell key={i} fill={entry.color} opacity={0.85} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      <WorkerControl workers={workers} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {/* Services health */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', overflow: 'hidden' }}>
          <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)', fontSize: 11, fontWeight: 700, color: 'var(--text-2)', letterSpacing: '.04em' }}>
            SERVICES HEALTH
          </div>
          {services.length === 0 ? (
            <div style={{ padding: 28, textAlign: 'center', color: 'var(--text-3)', fontSize: 11 }}>No services registered</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
              <thead>
                <tr>
                  {['Service', 'Type', 'State', 'Running', 'Failed', 'Total'].map(h => (
                    <th key={h} style={{ textAlign: 'left', padding: '6px 14px', fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase', borderBottom: '1px solid var(--border-1)', background: 'var(--bg-3)' }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {services.map(s => {
                  const ss = serviceStats[s.id] || {}
                  const state = s.paused ? 'paused' : ss.running > 0 ? 'running' : 'success'
                  return (
                    <tr key={s.id} style={{ borderBottom: '1px solid rgba(255,255,255,.025)' }}>
                      <td style={{ padding: '9px 14px', fontWeight: 700 }}>{s.name}</td>
                      <td style={{ padding: '9px 14px' }}>
                        <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 4, background: 'var(--cyan-dim)', color: 'var(--cyan)' }}>
                          {s.type}
                        </span>
                      </td>
                      <td style={{ padding: '9px 14px' }}><StatusBadge status={state} size="sm" /></td>
                      <td style={{ padding: '9px 14px', color: ss.running > 0 ? 'var(--cyan)' : 'var(--text-3)' }}>{ss.running || 0}</td>
                      <td style={{ padding: '9px 14px', color: ss.failed > 0 ? 'var(--rose)' : 'var(--text-3)', fontWeight: ss.failed > 0 ? 700 : 400 }}>{ss.failed || 0}</td>
                      <td style={{ padding: '9px 14px', color: 'var(--text-2)' }}>{ss.total || 0}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </div>

        {/* Live feed */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', overflow: 'hidden' }}>
          <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)', display: 'flex', alignItems: 'center' }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-2)', letterSpacing: '.04em' }}>LIVE ACTIVITY</span>
            <span style={{ marginLeft: 'auto', fontSize: 9, color: 'var(--text-3)' }}>{requests.length} total</span>
          </div>
          <div style={{ maxHeight: 280, overflowY: 'auto' }}>
            {feed.length === 0 ? (
              <div style={{ padding: 28, textAlign: 'center', color: 'var(--text-3)', fontSize: 11 }}>No activity yet</div>
            ) : feed.map(r => {
              const color = STATUS_COLORS[r.status] || '#4a5568'
              let detail = ''
              if (r.status === 'running')   detail = `Running for ${elapsed(r.created_at)}`
              else if (r.status === 'failed')   detail = shortenError(r.error)
              else if (r.status === 'retrying') detail = `Retry ${r.retry_count || '?'}/3 — ${shortenError(r.error)}`
              else if (r.status === 'success')  detail = `Done in ${elapsed(r.created_at, r.updated_at)}`
              else if (r.status === 'paused')   detail = 'Held — service paused'
              else if (r.status === 'cancelled') detail = 'Manually cancelled'
              else detail = r.status

              return (
                <div key={r.id} style={{
                  display: 'flex', alignItems: 'flex-start', gap: 10,
                  padding: '9px 14px', borderBottom: '1px solid rgba(255,255,255,.025)',
                }}>
                  <div style={{
                    width: 7, height: 7, borderRadius: '50%', background: color,
                    flexShrink: 0, marginTop: 4,
                    ...(r.status === 'running' || r.status === 'retrying' ? { boxShadow: `0 0 6px ${color}`, animation: 'pulse-dot 1.5s ease infinite' } : {}),
                  }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 11, fontWeight: 700, display: 'flex', gap: 6, alignItems: 'center' }}>
                      {r.service_name}
                      <span style={{ fontSize: 9, color: 'var(--text-3)', fontWeight: 400 }}>#{r.id.slice(0, 6)}</span>
                    </div>
                    <div style={{ fontSize: 10, color: r.status === 'failed' ? 'var(--rose)' : r.status === 'success' ? 'var(--emerald)' : r.status === 'retrying' ? 'var(--amber)' : 'var(--text-2)', marginTop: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {detail}
                    </div>
                  </div>
                  <div style={{ fontSize: 10, color: 'var(--text-3)', flexShrink: 0 }}>{timeAgo(r.updated_at)}</div>
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  )
}




