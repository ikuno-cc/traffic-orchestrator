import { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api/client'
import { parseApiDate } from '../utils/datetime'

const POLL_MS = 1500
const FULL_POLL_MS = 10000

function useStore() {
  const [services,  setServices]  = useState([])
  const [requests,  setRequests]  = useState([])
  const [stats,     setStats]     = useState({})
  const [health,    setHealth]    = useState(null)
  const [workers,   setWorkers]   = useState(null)
  const [loading,   setLoading]   = useState(true)
  const [lastPoll,  setLastPoll]  = useState(null)
  const [error,     setError]     = useState(null)
  const reqPollRef = useRef(null)
  const fullPollRef = useRef(null)

  const fetchAll = useCallback(async (silent = false) => {
    try {
      if (!silent) setLoading(true)
      const [svc, reqs, st, h, w] = await Promise.all([
        api.services.list(),
        api.requests.list(),
        api.stats(),
        api.health().catch(() => null),
        api.workers.status().catch(() => null),
      ])
      setServices(svc)
      setRequests(reqs)
      setStats(st)
      setHealth(h)
      setWorkers(w)
      setLastPoll(new Date())
      setError(null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchRequestsOnly = useCallback(async () => {
    try {
      const reqs = await api.requests.list()
      setRequests(reqs)
      setLastPoll(new Date())
    } catch (e) {
      setError(e.message)
    }
  }, [])

  // Initial load + polling
  useEffect(() => {
    fetchAll()
    reqPollRef.current = setInterval(() => fetchRequestsOnly(), POLL_MS)
    fullPollRef.current = setInterval(() => fetchAll(true), FULL_POLL_MS)
    return () => {
      clearInterval(reqPollRef.current)
      clearInterval(fullPollRef.current)
    }
  }, [fetchAll, fetchRequestsOnly])

  const refresh = useCallback(() => fetchAll(false), [fetchAll])

  // Derived: enriched requests with computed fields
  const safeDiffMs = (start, end = null) => {
    const s = parseApiDate(start)
    const e = end ? parseApiDate(end) : new Date()
    if (!s || Number.isNaN(s.getTime())) return 0
    if (!e || Number.isNaN(e.getTime())) return 0
    return Math.max(0, e - s)
  }

  const enrichedRequests = requests.map(r => ({
    ...r,
    durationMs: r.status === 'running'
      ? safeDiffMs(r.created_at)
      : safeDiffMs(r.created_at, r.updated_at),
    isTerminal: ['success', 'failed', 'cancelled'].includes(r.status),
    isActive:   ['running', 'queued', 'retrying'].includes(r.status),
  }))

  // Derived: per-service request counts
  const serviceStats = services.reduce((acc, s) => {
    const sReqs = enrichedRequests.filter(r => r.service_id === s.id)
    acc[s.id] = {
      total:    sReqs.length,
      running:  sReqs.filter(r => r.status === 'running' || r.status === 'retrying').length,
      success:  sReqs.filter(r => r.status === 'success').length,
      failed:   sReqs.filter(r => r.status === 'failed').length,
      queued:   sReqs.filter(r => r.status === 'queued').length,
    }
    return acc
  }, {})

  return {
    services, setServices,
    requests: enrichedRequests, rawRequests: requests, setRequests,
    stats, health, workers,
    loading, error, lastPoll,
    refresh, fetchAll,
    serviceStats,
    POLL_MS,
  }
}

export default useStore


