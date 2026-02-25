const BASE = import.meta.env.DEV ? '/api' : '/api'

async function req(path, options = {}) {
  const res = await fetch(BASE + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`${res.status}: ${body}`)
  }
  return res.json()
}

// ── Services ─────────────────────────────────────────────────
export const api = {
  services: {
    list:   ()         => req('/services'),
    get:    (id)       => req(`/services/${id}`),
    create: (data)     => req('/services',          { method: 'POST', body: JSON.stringify(data) }),
    update: (id, data) => req(`/services/${id}`,    { method: 'PUT',  body: JSON.stringify({ id, ...data }) }),
    delete: (id)       => req(`/services/${id}`,    { method: 'DELETE' }),
    pause:  (id)       => req(`/services/${id}/pause`,  { method: 'POST' }),
    resume: (id)       => req(`/services/${id}/resume`, { method: 'POST' }),
  },

  requests: {
    list:   (params = {}) => {
      const q = new URLSearchParams({ limit: 300, ...params }).toString()
      return req(`/requests?${q}`)
    },
    get:    (id)   => req(`/requests/${id}`),
    cancel: (id)   => req(`/requests/${id}/cancel`, { method: 'POST' }),
    delete: (id)   => req(`/requests/${id}`,        { method: 'DELETE' }),
  },

  dispatch: (data) => req('/dispatch', { method: 'POST', body: JSON.stringify(data) }),

  stats: () => req('/stats'),

  health: () => req('/health'),

  workers: {
    status: () => req('/workers'),
    setConcurrency: (concurrency) =>
      req('/workers/concurrency', {
        method: 'POST',
        body: JSON.stringify({ concurrency }),
      }),
  },
}
