function getDateRange(period) {
  const now = new Date()
  const start = new Date(now)

  switch (period) {
    case 'today':
      start.setHours(0, 0, 0, 0)
      return { gte: start, lte: now }
    case 'week': {
      const day = start.getDay()
      const diff = day === 0 ? 6 : day - 1
      start.setDate(start.getDate() - diff)
      start.setHours(0, 0, 0, 0)
      return { gte: start, lte: now }
    }
    case 'month':
      start.setDate(1)
      start.setHours(0, 0, 0, 0)
      return { gte: start, lte: now }
    case 'all':
    default:
      return undefined
  }
}

module.exports = { getDateRange }
