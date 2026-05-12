import React, { useState } from 'react'
import StatusBadge from '../components/StatusBadge'
import { api } from '../api/client'

const TYPE_COLORS = {
  comfyui: { bg: 'rgba(0,212,255,.1)',    color: 'var(--cyan)'    },
  n8n:     { bg: 'rgba(52,211,153,.1)',   color: 'var(--emerald)' },
  custom:  { bg: 'rgba(167,139,250,.1)',  color: 'var(--violet)'  },
  omnivoice: { bg: 'rgba(251,146,60,.1)', color: 'var(--amber)'   },
}

function ServiceModal({ service, onClose, onSave, toast }) {
  const editing = !!service
  const [form, setForm] = useState({
    name:        service?.name        || '',
    type:        service?.type        || 'comfyui',
    url:         service?.url         || '',
    description: service?.description || '',
    timeout:     service?.timeout     || 120,
    delay_seconds: service?.delay_seconds ?? 3,
    worker_count: service?.worker_count ?? 1,
    enabled:     service?.enabled     ?? true,
    headers:     JSON.stringify(service?.headers || {}, null, 2),
  })
  const [saving, setSaving] = useState(false)

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const handleSave = async () => {
    if (!form.name || !form.url) { toast('Name and URL are required', 'error'); return }
    let headers = {}
    try { headers = JSON.parse(form.headers || '{}') }
    catch { toast('Invalid JSON in headers field', 'error'); return }

    setSaving(true)
    try {
      const data = {
        ...form,
        headers,
        timeout: parseInt(form.timeout),
        delay_seconds: Math.max(0, Number(form.delay_seconds) || 3),
        worker_count: Math.max(1, parseInt(form.worker_count || 1)),
      }
      if (editing) await api.services.update(service.id, data)
      else         await api.services.create(data)
      toast(editing ? 'Service updated' : 'Service created', 'success')
      onSave()
      onClose()
    } catch (e) { toast(e.message, 'error') }
    finally { setSaving(false) }
  }

  return (
    <div
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
      style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,.75)', backdropFilter: 'blur(8px)', zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center', animation: 'fade-in .15s ease' }}
    >
      <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-2)', borderRadius: 'var(--radius-lg)', padding: 24, width: 520, maxWidth: '95vw', display: 'flex', flexDirection: 'column', gap: 14, animation: 'slide-up .2s ease' }}>
        <div style={{ fontFamily: 'var(--font-display)', fontSize: 17, fontWeight: 700 }}>
          {editing ? 'Edit Service' : 'Add New Service'}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
          <Field label="Name">
            <input style={inputStyle} value={form.name} onChange={e => set('name', e.target.value)} placeholder="My ComfyUI" />
          </Field>
          <Field label="Type">
            <select style={inputStyle} value={form.type} onChange={e => set('type', e.target.value)}>
              <option value="comfyui">ComfyUI</option>
              <option value="n8n">n8n</option>
              <option value="omnivoice">OmniVoice</option>
              <option value="custom">Custom HTTP</option>
            </select>
          </Field>
        </div>

        <Field label="Endpoint URL">
          <input style={inputStyle} value={form.url} onChange={e => set('url', e.target.value)} placeholder="http://localhost:8188" />
        </Field>

        <Field label="Description (optional)">
          <input style={inputStyle} value={form.description} onChange={e => set('description', e.target.value)} placeholder="Short description…" />
        </Field>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 12 }}>
          <Field label="Timeout (seconds)">
            <input style={inputStyle} type="number" value={form.timeout} onChange={e => set('timeout', e.target.value)} min={5} max={3600} />
          </Field>
          <Field label="Delay (seconds)">
            <input style={inputStyle} type="number" value={form.delay_seconds} onChange={e => set('delay_seconds', e.target.value)} min={0} step="0.1" max={3600} />
          </Field>
          <Field label="Workers">
            <input style={inputStyle} type="number" value={form.worker_count} onChange={e => set('worker_count', e.target.value)} min={1} max={64} />
          </Field>
          <Field label="Enabled">
            <select style={inputStyle} value={String(form.enabled)} onChange={e => set('enabled', e.target.value === 'true')}>
              <option value="true">Yes</option>
              <option value="false">No</option>
            </select>
          </Field>
        </div>

        <Field label="Custom Headers (JSON)">
          <textarea style={{ ...inputStyle, minHeight: 64, resize: 'vertical' }} value={form.headers} onChange={e => set('headers', e.target.value)} />
        </Field>

        <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
          <button onClick={handleSave} disabled={saving} style={{ ...btnPrimary, opacity: saving ? .6 : 1 }}>
            {saving ? 'Saving…' : editing ? 'Update Service' : 'Create Service'}
          </button>
          <button onClick={onClose} style={btnGhost}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

function Field({ label, children }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      <label style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase' }}>{label}</label>
      {children}
    </div>
  )
}

export default function Services({ services, serviceStats, refresh, toast }) {
  const [modal, setModal] = useState(null) // null | 'new' | service object

  const handleDelete = async (id) => {
    if (!confirm('Delete this service? All related request history stays.')) return
    try { await api.services.delete(id); toast('Service deleted', 'info'); refresh() }
    catch (e) { toast(e.message, 'error') }
  }

  const handlePause = async (id) => {
    try { await api.services.pause(id); toast('Service paused — requests will be held', 'warn'); refresh() }
    catch (e) { toast(e.message, 'error') }
  }

  const handleResume = async (id) => {
    try {
      const r = await api.services.resume(id)
      toast(`Resumed — ${r.requeued} held requests re-queued`, 'success')
      refresh()
    }
    catch (e) { toast(e.message, 'error') }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, animation: 'fade-in .2s ease' }}>
      {modal && (
        <ServiceModal
          service={modal === 'new' ? null : modal}
          onClose={() => setModal(null)}
          onSave={refresh}
          toast={toast}
        />
      )}

      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 700 }}>
          Service <span style={{ color: 'var(--cyan)' }}>Registry</span>
        </h1>
        <button onClick={() => setModal('new')} style={{ ...btnPrimary, marginLeft: 'auto' }}>
          + Add Service
        </button>
      </div>

      {services.length === 0 ? (
        <div style={{
          background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)',
          padding: 60, textAlign: 'center', color: 'var(--text-3)', fontSize: 12,
        }}>
          No services registered yet — click "Add Service" to get started
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 14 }}>
          {services.map(s => {
            const ss = serviceStats[s.id] || {}
            const tc = TYPE_COLORS[s.type] || TYPE_COLORS.custom
            const isPaused = s.paused
            const state = isPaused ? 'paused' : ss.running > 0 ? 'running' : 'success'

            return (
              <div key={s.id} style={{
                background: 'var(--bg-2)', border: `1px solid ${isPaused ? 'rgba(253,230,138,.2)' : 'var(--border-1)'}`,
                borderRadius: 'var(--radius-md)', padding: 18,
                display: 'flex', flexDirection: 'column', gap: 12,
                transition: 'border-color .2s, transform .15s',
              }}
                onMouseEnter={e => e.currentTarget.style.transform = 'translateY(-2px)'}
                onMouseLeave={e => e.currentTarget.style.transform = 'translateY(0)'}
              >
                {/* Header row */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div>
                    <div style={{ fontFamily: 'var(--font-display)', fontSize: 15, fontWeight: 700 }}>{s.name}</div>
                    <div style={{ fontSize: 10, color: 'var(--text-3)', marginTop: 2, wordBreak: 'break-all' }}>{s.url}</div>
                  </div>
                  <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: tc.bg, color: tc.color, flexShrink: 0, marginLeft: 8 }}>
                    {s.type.toUpperCase()}
                  </span>
                </div>

                {/* Status + desc */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <StatusBadge status={state} size="sm" />
                  <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{s.description || 'No description'}</span>
                </div>

                {/* Stats row */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, background: 'var(--bg-3)', borderRadius: 8, padding: '10px 12px' }}>
                  {[
                    { label: 'Running', val: ss.running || 0, color: 'var(--cyan)'    },
                    { label: 'Success', val: ss.success || 0, color: 'var(--emerald)' },
                    { label: 'Failed',  val: ss.failed  || 0, color: ss.failed > 0 ? 'var(--rose)' : 'var(--text-3)' },
                    { label: 'Total',   val: ss.total   || 0, color: 'var(--text-2)'  },
                  ].map(st => (
                    <div key={st.label} style={{ display: 'flex', flexDirection: 'column', gap: 1, minWidth: 52 }}>
                      <div style={{ fontFamily: 'var(--font-display)', fontSize: 18, fontWeight: 700, color: st.color, lineHeight: 1 }}>
                        {st.val}
                      </div>
                      <div style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.06em' }}>{st.label}</div>
                    </div>
                  ))}
                  <div style={{ marginLeft: 'auto', display: 'flex', flexDirection: 'column', gap: 1, minWidth: 52 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-3)' }}>timeout</div>
                    <div style={{ fontSize: 11, color: 'var(--text-2)' }}>{s.timeout}s</div>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 1, minWidth: 52 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-3)' }}>delay</div>
                    <div style={{ fontSize: 11, color: 'var(--text-2)' }}>{Number(s.delay_seconds ?? 3)}s</div>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 1, minWidth: 52 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-3)' }}>workers</div>
                    <div style={{ fontSize: 11, color: 'var(--text-2)' }}>{Math.max(1, Number(s.worker_count ?? 1))}</div>
                  </div>
                </div>

                {/* Actions */}
                <div style={{ display: 'flex', gap: 6, marginTop: 2 }}>
                  {isPaused
                    ? <button onClick={() => handleResume(s.id)} style={btnSuccess}>▶ Resume</button>
                    : <button onClick={() => handlePause(s.id)} style={btnWarn}>⏸ Pause</button>
                  }
                  <button onClick={() => setModal(s)} style={btnGhost}>Edit</button>
                  <button onClick={() => handleDelete(s.id)} style={{ ...btnGhost, marginLeft: 'auto', color: 'var(--rose)' }}>Delete</button>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

const inputStyle = {
  background: 'var(--bg-3)', border: '1px solid var(--border-2)',
  borderRadius: 6, color: 'var(--text-1)', fontFamily: 'var(--font-mono)',
  fontSize: 11, padding: '8px 10px', outline: 'none', width: '100%',
}
const btnBase = {
  fontFamily: 'var(--font-mono)', fontSize: 11, fontWeight: 700,
  cursor: 'pointer', borderRadius: 6, padding: '7px 14px',
  border: '1px solid transparent', transition: 'all .15s',
}
const btnPrimary = { ...btnBase, background: 'var(--cyan)', color: '#000' }
const btnGhost   = { ...btnBase, background: 'transparent', border: '1px solid var(--border-2)', color: 'var(--text-2)' }
const btnWarn    = { ...btnBase, background: 'rgba(251,191,36,.1)', border: '1px solid rgba(251,191,36,.3)', color: 'var(--amber)' }
const btnSuccess = { ...btnBase, background: 'rgba(52,211,153,.1)', border: '1px solid rgba(52,211,153,.3)', color: 'var(--emerald)' }
