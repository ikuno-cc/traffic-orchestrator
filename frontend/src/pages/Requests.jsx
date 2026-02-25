import React, { useState, useEffect, useCallback } from 'react'
import StatusBadge from '../components/StatusBadge'
import { api } from '../api/client'
import { parseApiDate } from '../utils/datetime'

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

function RetryPips({ count = 0, status }) {
  const max = 3
  return (
    <div style={{ display: 'flex', gap: 3, alignItems: 'center' }} title={`${count}/${max} attempts`}>
      {Array.from({ length: max }, (_, i) => (
        <div key={i} style={{
          width: 7, height: 7, borderRadius: '50%',
          background: i < count
            ? (status === 'failed' ? 'var(--rose)' : 'var(--amber)')
            : 'var(--border-2)',
        }} />
      ))}
    </div>
  )
}

function PriorityBar({ value }) {
  const color = value >= 8 ? 'var(--rose)' : value >= 6 ? 'var(--amber)' : 'var(--cyan)'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 36, height: 3, background: 'var(--border-2)', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${value * 10}%`, height: '100%', background: color, borderRadius: 2 }} />
      </div>
      <span style={{ fontSize: 10, color, fontWeight: 700 }}>{value}</span>
    </div>
  )
}

function ErrorModal({ request, onClose }) {
  if (!request) return null
  return (
    <div
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
      style={{
        position: 'fixed', inset: 0, background: 'rgba(0,0,0,.75)',
        backdropFilter: 'blur(8px)', zIndex: 1000,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        animation: 'fade-in .15s ease',
      }}
    >
      <div style={{
        background: 'var(--bg-2)', border: '1px solid var(--border-2)',
        borderRadius: 'var(--radius-lg)', padding: 24, width: 620,
        maxWidth: '95vw', maxHeight: '80vh',
        display: 'flex', flexDirection: 'column', gap: 14,
        animation: 'slide-up .2s ease',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <div style={{ fontFamily: 'var(--font-display)', fontSize: 16, fontWeight: 700, color: 'var(--rose)' }}>
              Error Detail
            </div>
            <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 3 }}>
              {request.id} · {request.service_name} · {request.retry_count || 0}/3 retries
            </div>
          </div>
          <button onClick={onClose} style={{ background: 'transparent', border: 'none', color: 'var(--text-2)', fontSize: 16, cursor: 'pointer' }}>✕</button>
        </div>

        <div style={{
          background: 'rgba(251,113,133,.05)', border: '1px solid rgba(251,113,133,.2)',
          borderRadius: 8, padding: 14,
          fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-1)',
          whiteSpace: 'pre-wrap', wordBreak: 'break-all',
          overflowY: 'auto', maxHeight: 320, lineHeight: 1.6,
        }}>
          {request.error || 'No error message recorded.'}
        </div>

        {/* Metadata */}
        {request.metadata && Object.keys(request.metadata).length > 0 && (
          <>
            <div style={{ fontSize: 10, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase' }}>
              Metadata
            </div>
            <div style={{
              background: 'var(--bg-3)', border: '1px solid var(--border-1)',
              borderRadius: 8, padding: 12, fontSize: 11,
              fontFamily: 'var(--font-mono)', color: 'var(--text-2)',
              whiteSpace: 'pre-wrap', maxHeight: 140, overflowY: 'auto',
            }}>
              {JSON.stringify(request.metadata, null, 2)}
            </div>
          </>
        )}

        <button
          onClick={onClose}
          style={{
            alignSelf: 'flex-start', background: 'transparent',
            border: '1px solid var(--border-2)', color: 'var(--text-2)',
            padding: '6px 14px', borderRadius: 6, fontSize: 11,
            cursor: 'pointer', fontFamily: 'var(--font-mono)',
          }}
        >
          Close
        </button>
      </div>
    </div>
  )
}

export default function Requests({ requests, services, refresh, toast }) {
  const [filterService, setFilterService] = useState('')
  const [filterStatus,  setFilterStatus]  = useState('')
  const [sortField, setSortField]  = useState('created_at')
  const [sortDir,   setSortDir]    = useState('desc')
  const [errorReq,  setErrorReq]   = useState(null)
  const [tick, setTick] = useState(0)

  // Live tick for running timers
  useEffect(() => {
    const t = setInterval(() => setTick(v => v + 1), 1000)
    return () => clearInterval(t)
  }, [])

  const handleSort = (field) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortField(field); setSortDir('desc') }
  }

  const handleCancel = async (id) => {
    try { await api.requests.cancel(id); toast('Request cancelled', 'info'); refresh() }
    catch (e) { toast(e.message, 'error') }
  }

  const handleDelete = async (id) => {
    try { await api.requests.delete(id); toast('Request removed', 'info'); refresh() }
    catch (e) { toast(e.message, 'error') }
  }

  const handleClearDone = async () => {
    const done = requests.filter(r => ['success', 'failed', 'cancelled'].includes(r.status))
    await Promise.all(done.map(r => api.requests.delete(r.id).catch(() => {})))
    toast(`Cleared ${done.length} completed requests`, 'success')
    refresh()
  }

  let filtered = [...requests]
  if (filterService) filtered = filtered.filter(r => r.service_id === filterService)
  if (filterStatus)  filtered = filtered.filter(r => r.status === filterStatus)
  filtered.sort((a, b) => {
    let va = a[sortField], vb = b[sortField]
    if (sortField === 'created_at' || sortField === 'updated_at') { va = parseApiDate(va); vb = parseApiDate(vb) }
    if (va < vb) return sortDir === 'asc' ? -1 : 1
    if (va > vb) return sortDir === 'asc' ? 1 : -1
    return 0
  })

  const Th = ({ label, field, style = {} }) => (
    <th
      onClick={() => field && handleSort(field)}
      style={{
        textAlign: 'left', padding: '7px 12px',
        fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase',
        borderBottom: '1px solid var(--border-1)', background: 'var(--bg-3)',
        cursor: field ? 'pointer' : 'default', whiteSpace: 'nowrap',
        userSelect: 'none', ...style,
      }}
    >
      {label}
      {field && sortField === field && (
        <span style={{ marginLeft: 4, color: 'var(--cyan)' }}>{sortDir === 'asc' ? '↑' : '↓'}</span>
      )}
    </th>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, animation: 'fade-in .2s ease' }}>
      <ErrorModal request={errorReq} onClose={() => setErrorReq(null)} />

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 700 }}>
          Request <span style={{ color: 'var(--cyan)' }}>Monitor</span>
        </h1>
        <span style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 4 }}>{filtered.length} of {requests.length}</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
          {/* Filters */}
          <select
            value={filterService} onChange={e => setFilterService(e.target.value)}
            style={selectStyle}
          >
            <option value="">All Services</option>
            {services.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
          </select>
          <select
            value={filterStatus} onChange={e => setFilterStatus(e.target.value)}
            style={selectStyle}
          >
            <option value="">All Status</option>
            {['running','queued','retrying','success','failed','paused','cancelled'].map(s =>
              <option key={s} value={s}>{s}</option>
            )}
          </select>
          <button onClick={refresh} style={btnGhost}>↻ Refresh</button>
          <button onClick={handleClearDone} style={btnDanger}>Clear Done</button>
        </div>
      </div>

      {/* Table */}
      <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', overflow: 'hidden' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr>
                <Th label="ID"       field="id"         />
                <Th label="Service"  field="service_name" />
                <Th label="Status"   field="status"     />
                <Th label="Error / Info"                 />
                <Th label="Retries"                      />
                <Th label="Priority" field="priority"   />
                <Th label="Duration"                     />
                <Th label="Created"  field="created_at" />
                <Th label="Actions"                      />
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr>
                  <td colSpan={9} style={{ textAlign: 'center', padding: 36, color: 'var(--text-3)', fontSize: 11 }}>
                    No requests match the current filters
                  </td>
                </tr>
              ) : filtered.map(r => {
                const isRunning  = r.status === 'running'
                const isActive   = r.isActive
                const hasFailed  = r.status === 'failed' || r.status === 'retrying'
                const dur = isRunning
                  ? elapsed(r.created_at)
                  : elapsed(r.created_at, r.updated_at)
                const createdAt = parseApiDate(r.created_at)
                const updatedAt = parseApiDate(r.updated_at)
                const durSeconds = createdAt && updatedAt ? (updatedAt - createdAt) / 1000 : 0

                // Info cell content
                let infoContent = null
                if (hasFailed && r.error) {
                  infoContent = (
                    <span
                      onClick={() => setErrorReq(r)}
                      style={{
                        color: 'var(--rose)', fontSize: 10, cursor: 'pointer',
                        maxWidth: 200, display: 'block', whiteSpace: 'nowrap',
                        overflow: 'hidden', textOverflow: 'ellipsis',
                        textDecoration: 'underline dotted',
                      }}
                      title="Click to see full error"
                    >
                      {r.error.replace(/Traceback[\s\S]*$/, '').trim().slice(0, 60)}…
                    </span>
                  )
                } else if (isRunning) {
                  infoContent = (
                    <span style={{ color: 'var(--cyan)', fontSize: 10, animation: 'blink 1s step-end infinite' }}>
                      ▶ In progress…
                    </span>
                  )
                } else if (r.status === 'success') {
                  infoContent = <span style={{ color: 'var(--emerald)', fontSize: 10 }}>✓ Completed</span>
                } else if (r.status === 'paused') {
                  infoContent = <span style={{ color: 'var(--paused)', fontSize: 10 }}>⏸ Held — service paused</span>
                } else if (r.status === 'cancelled') {
                  infoContent = <span style={{ color: 'var(--text-3)', fontSize: 10 }}>Manually cancelled</span>
                }

                return (
                  <tr key={r.id} style={{ borderBottom: '1px solid rgba(255,255,255,.025)' }}>
                    <td style={{ padding: '9px 12px', color: 'var(--text-3)', fontSize: 10, fontFamily: 'var(--font-mono)' }}
                        title={r.id}>
                      {r.id.slice(0, 8)}…
                    </td>
                    <td style={{ padding: '9px 12px', fontWeight: 600 }}>{r.service_name}</td>
                    <td style={{ padding: '9px 12px' }}><StatusBadge status={r.status} /></td>
                    <td style={{ padding: '9px 12px', maxWidth: 220 }}>{infoContent || <span style={{ color: 'var(--text-3)' }}>—</span>}</td>
                    <td style={{ padding: '9px 12px' }}><RetryPips count={r.retry_count || 0} status={r.status} /></td>
                    <td style={{ padding: '9px 12px' }}><PriorityBar value={r.priority || 5} /></td>
                    <td style={{ padding: '9px 12px' }}>
                      <span style={{
                        fontSize: 10, fontVariantNumeric: 'tabular-nums',
                        color: isRunning ? 'var(--cyan)' : durSeconds > 30 ? 'var(--amber)' : 'var(--text-2)',
                        animation: isRunning ? 'blink 1s step-end infinite' : 'none',
                      }}>
                        {dur}
                      </span>
                    </td>
                    <td style={{ padding: '9px 12px', color: 'var(--text-3)', fontSize: 10, whiteSpace: 'nowrap' }}>
                      {timeAgo(r.created_at)}
                    </td>
                    <td style={{ padding: '9px 12px' }}>
                      <div style={{ display: 'flex', gap: 5 }}>
                        {hasFailed && (
                          <button onClick={() => setErrorReq(r)} style={{ ...btnSmIcon, color: 'var(--rose)', borderColor: 'rgba(251,113,133,.3)' }} title="View error">
                            !
                          </button>
                        )}
                        {isActive && (
                          <button onClick={() => handleCancel(r.id)} style={{ ...btnSmIcon, color: 'var(--rose)', borderColor: 'rgba(251,113,133,.3)' }} title="Cancel">
                            ✕
                          </button>
                        )}
                        <button onClick={() => handleDelete(r.id)} style={{ ...btnSmIcon }} title="Delete">
                          🗑
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

const selectStyle = {
  background: 'var(--bg-3)', border: '1px solid var(--border-2)',
  borderRadius: 6, color: 'var(--text-1)', fontFamily: 'var(--font-mono)',
  fontSize: 10, padding: '5px 8px', outline: 'none',
}
const btnBase = {
  fontFamily: 'var(--font-mono)', fontSize: 10, fontWeight: 700,
  cursor: 'pointer', borderRadius: 6, padding: '5px 10px',
  border: '1px solid var(--border-2)', transition: 'all .15s',
}
const btnGhost  = { ...btnBase, background: 'transparent', color: 'var(--text-2)' }
const btnDanger = { ...btnBase, background: 'rgba(251,113,133,.1)', color: 'var(--rose)', borderColor: 'rgba(251,113,133,.3)' }
const btnSmIcon = { ...btnBase, padding: '3px 7px', width: 24, height: 24, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--bg-3)', color: 'var(--text-2)' }



