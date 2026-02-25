export function parseApiDate(value) {
  if (!value) return null
  if (value instanceof Date) return value
  if (typeof value !== 'string') return new Date(value)

  // Treat timezone-less API timestamps as UTC to avoid local offset drift.
  const hasTimezone = /(?:Z|[+\-]\d{2}:?\d{2})$/.test(value)
  const iso = hasTimezone ? value : `${value}Z`
  return new Date(iso)
}

