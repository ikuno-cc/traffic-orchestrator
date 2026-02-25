import React, { useState } from 'react'
import Sidebar from './components/Sidebar'
import Topbar from './components/Topbar'
import ToastContainer from './components/ToastContainer'
import Dashboard from './pages/Dashboard'
import Requests from './pages/Requests'
import Services from './pages/Services'
import Dispatch from './pages/Dispatch'
import useStore from './store/useStore'
import { useToast } from './hooks/useToast'

export default function App() {
  const [page, setPage] = useState('dashboard')
  const { toasts, toast, removeToast } = useToast()
  const store = useStore()

  const {
    services, requests, stats, health, workers,
    loading, error, lastPoll, refresh, serviceStats,
  } = store

  const pages = {
    dashboard: <Dashboard requests={requests} services={services} stats={stats} serviceStats={serviceStats} workers={workers} refresh={refresh} toast={toast} />,
    requests:  <Requests  requests={requests} services={services} refresh={refresh} toast={toast} />,
    services:  <Services  services={services} serviceStats={serviceStats} refresh={refresh} toast={toast} />,
    dispatch:  <Dispatch  services={services} refresh={refresh} toast={toast} />,
  }

  return (
    <div className="grid-bg" style={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
      <Topbar lastPoll={lastPoll} onRefresh={refresh} loading={loading} error={error} />

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <Sidebar active={page} onChange={setPage} stats={stats} requests={requests} />

        <main style={{
          flex: 1, overflowY: 'auto', padding: 24,
          background: 'var(--bg-0)',
        }}>
          {loading && !requests.length ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {[...Array(4)].map((_, i) => (
                <div key={i} className="skeleton" style={{ height: i === 0 ? 40 : 80, animationDelay: `${i * .1}s` }} />
              ))}
            </div>
          ) : (
            pages[page] || pages.dashboard
          )}
        </main>
      </div>

      <ToastContainer toasts={toasts} onRemove={removeToast} />
    </div>
  )
}
