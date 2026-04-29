import React, { useState } from 'react'
import StatusBadge from '../components/StatusBadge'
import { api } from '../api/client'

const COMFYUI_EXAMPLE = {
  "120": { "inputs": { "model": "InfiniteTalk/Wan2_1-InfiniteTalk-Single_fp8_e4m3fn_scaled_KJ.safetensors" }, "class_type": "MultiTalkModelLoader" },
  "125": { "inputs": { "audio": "chatterbox-speech (5).wav" }, "class_type": "LoadAudio" },
  "284": { "inputs": { "image": "Untitled design (12).png" }, "class_type": "LoadImage" },
  "241": { "inputs": { "positive_prompt": "Serious calm expert speaking", "negative_prompt": "blurred, static" }, "class_type": "WanVideoTextEncode" },
}

function JsonEditor({ value, onChange, label, minHeight = 120 }) {
  const [err, setErr] = useState(null)
  const handle = (v) => {
    onChange(v)
    try { JSON.parse(v); setErr(null) }
    catch (e) { setErr('Invalid JSON') }
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <label style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase' }}>{label}</label>
        {err && <span style={{ fontSize: 9, color: 'var(--rose)' }}>{err}</span>}
      </div>
      <textarea
        value={value}
        onChange={e => handle(e.target.value)}
        style={{
          background: 'var(--bg-3)', border: `1px solid ${err ? 'var(--rose)' : 'var(--border-2)'}`,
          borderRadius: 6, color: 'var(--text-1)', fontFamily: 'var(--font-mono)',
          fontSize: 11, padding: '10px 12px', outline: 'none',
          minHeight, resize: 'vertical', lineHeight: 1.6,
        }}
      />
    </div>
  )
}

function ResultCard({ result }) {
  if (!result) return null
  const hasResponse = result.response !== undefined && result.response !== null
  const hasError = result.error !== undefined && result.error !== null

  return (
    <div style={{
      background: 'var(--bg-3)', border: '1px solid var(--border-2)',
      borderRadius: 8, padding: 14, animation: 'slide-up .2s ease',
      display: 'flex', flexDirection: 'column', gap: 8,
    }}>
      <div style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.1em', textTransform: 'uppercase' }}>
        Dispatch Result
      </div>
      <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
        <div>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>Request ID </span>
          <code style={{ fontSize: 11, color: 'var(--cyan)' }}>{result.request_id}</code>
        </div>
        <StatusBadge status={result.status} />
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-2)' }}>
        {result.status === 'queued'
          ? 'Worker will pick this up shortly.'
          : 'Service is paused or completed.'}
      </div>
      {hasResponse && (
        <div>
          <div style={{ fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase', marginBottom: 4 }}>
            Response
          </div>
          <pre style={{
            margin: 0, fontSize: 10, lineHeight: 1.5, color: 'var(--text-1)',
            background: 'var(--bg-1)', border: '1px solid var(--border-2)',
            borderRadius: 6, padding: 10, maxHeight: 220, overflow: 'auto',
          }}>
            {JSON.stringify(result.response, null, 2)}
          </pre>
        </div>
      )}
      {hasError && <div style={{ fontSize: 10, color: 'var(--rose)' }}>{String(result.error)}</div>}
    </div>
  )
}
export default function Dispatch({ services, refresh, toast }) {
  const [serviceId, setServiceId] = useState('')
  const [priority,  setPriority]  = useState('5')
  const [delaySeconds, setDelaySeconds] = useState('')
  const [payload,   setPayload]   = useState('')
  const [metadata,  setMetadata]  = useState('{}')
  const [webhook,   setWebhook]   = useState('')
  const [result,    setResult]    = useState(null)
  const [sending,   setSending]   = useState(false)
  const [history,   setHistory]   = useState([])

  const handleDispatch = async () => {
    if (!serviceId) { toast('Select a service first', 'error'); return }
    let parsedPayload, parsedMeta
    try { parsedPayload = JSON.parse(payload || '{}') }
    catch { toast('Payload must be valid JSON', 'error'); return }
    try { parsedMeta = JSON.parse(metadata || '{}') }
    catch { toast('Metadata must be valid JSON', 'error'); return }

    setSending(true)
    try {
      const r = await api.dispatch({
        service_id:  serviceId,
        payload:     parsedPayload,
        metadata:    parsedMeta,
        priority:    parseInt(priority) || 5,
        delay_seconds: delaySeconds === '' ? null : Math.max(0, Number(delaySeconds) || 0),
        webhook_url: webhook || null,
      })
      setResult(r)
      setHistory(h => [{ ...r, service_name: services.find(s => s.id === serviceId)?.name, ts: new Date().toISOString() }, ...h.slice(0, 9)])
      toast(`Dispatched ${r.request_id.slice(0, 8)}`, 'success')
      refresh()
    } catch (e) {
      toast(e.message, 'error')
    } finally {
      setSending(false)
    }
  }

  const loadExample = () => {
    setPayload(JSON.stringify(COMFYUI_EXAMPLE, null, 2))
    setMetadata(JSON.stringify({ source: 'manual', workflow: 'infinite-talk', triggered_by: 'dashboard' }, null, 2))
    toast('ComfyUI example loaded', 'info')
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, animation: 'fade-in .2s ease' }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
        <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 700 }}>
          Manual <span style={{ color: 'var(--cyan)' }}>Dispatch</span>
        </h1>
        <span style={{ fontSize: 11, color: 'var(--text-3)' }}>Send a request directly from the dashboard</span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 360px', gap: 16 }}>
        {/* Main form */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', padding: 20, display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
              <label style={labelStyle}>Target Service</label>
              <select value={serviceId} onChange={e => setServiceId(e.target.value)} style={inputStyle}>
                <option value="">-- select a service --</option>
                {services.map(s => (
                  <option key={s.id} value={s.id}>{s.name} ({s.type}){s.paused ? ' [paused]' : ''}</option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
              <label style={labelStyle}>Priority (1 = low, 10 = urgent)</label>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <input
                  type="range" min="1" max="10" value={priority}
                  onChange={e => setPriority(e.target.value)}
                  style={{ flex: 1, accentColor: 'var(--cyan)' }}
                />
                <span style={{ fontSize: 14, fontFamily: 'var(--font-display)', fontWeight: 700, color: parseInt(priority) >= 8 ? 'var(--rose)' : parseInt(priority) >= 6 ? 'var(--amber)' : 'var(--cyan)', minWidth: 16, textAlign: 'center' }}>
                  {priority}
                </span>
              </div>
            </div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            <label style={labelStyle}>Delay Between Requests (seconds)</label>
            <input
              type="number"
              min="0"
              step="0.1"
              value={delaySeconds}
              onChange={e => setDelaySeconds(e.target.value)}
              placeholder="Service default (3s)"
              style={inputStyle}
            />
          </div>

          <JsonEditor label="Payload (JSON)" value={payload} onChange={setPayload} minHeight={200} />
          <JsonEditor label="Routing Metadata (JSON)" value={metadata} onChange={setMetadata} minHeight={72} />

          <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            <label style={labelStyle}>Webhook URL (optional - called on completion)</label>
            <input
              type="url" value={webhook} onChange={e => setWebhook(e.target.value)}
              placeholder="http://localhost:5678/webhook/done"
              style={inputStyle}
            />
          </div>

          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <button onClick={handleDispatch} disabled={sending} style={{ ...btnPrimary, opacity: sending ? .6 : 1, minWidth: 120 }}>
              {sending ? 'Sending...' : '-> Dispatch'}
            </button>
            <button onClick={loadExample} style={btnGhost}>Load ComfyUI Example</button>
            <button onClick={() => { setPayload(''); setMetadata('{}'); setWebhook(''); setDelaySeconds(''); setResult(null) }} style={{ ...btnGhost, marginLeft: 'auto', color: 'var(--text-3)' }}>
              Clear
            </button>
          </div>

          <ResultCard result={result} />
        </div>

        {/* Sidebar: dispatch history */}
        <div style={{ background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 'var(--radius-md)', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
          <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-1)', fontSize: 11, fontWeight: 700, color: 'var(--text-2)', letterSpacing: '.04em' }}>
            SESSION HISTORY
          </div>
          {history.length === 0 ? (
            <div style={{ padding: 28, textAlign: 'center', color: 'var(--text-3)', fontSize: 11, flex: 1 }}>
              Dispatched requests appear here
            </div>
          ) : (
            <div style={{ flex: 1, overflowY: 'auto' }}>
              {history.map((h, i) => (
                <div key={i} style={{ padding: '10px 16px', borderBottom: '1px solid rgba(255,255,255,.025)', display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <StatusBadge status={h.status} size="sm" />
                    <span style={{ fontSize: 9, color: 'var(--text-3)', marginLeft: 'auto' }}>
                      {new Date(h.ts).toLocaleTimeString()}
                    </span>
                  </div>
                  <div style={{ fontSize: 10, color: 'var(--text-2)', fontFamily: 'var(--font-mono)' }}>
                    {h.service_name}
                  </div>
                  <code style={{ fontSize: 9, color: 'var(--text-3)' }}>{h.request_id.slice(0, 16)}…</code>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

const labelStyle = { fontSize: 9, color: 'var(--text-3)', letterSpacing: '.08em', textTransform: 'uppercase' }
const inputStyle = {
  background: 'var(--bg-3)', border: '1px solid var(--border-2)',
  borderRadius: 6, color: 'var(--text-1)', fontFamily: 'var(--font-mono)',
  fontSize: 11, padding: '8px 10px', outline: 'none', width: '100%',
}
const btnBase    = { fontFamily: 'var(--font-mono)', fontSize: 11, fontWeight: 700, cursor: 'pointer', borderRadius: 6, padding: '8px 16px', border: '1px solid transparent', transition: 'all .15s' }
const btnPrimary = { ...btnBase, background: 'var(--cyan)', color: '#000' }
const btnGhost   = { ...btnBase, background: 'transparent', border: '1px solid var(--border-2)', color: 'var(--text-2)' }


