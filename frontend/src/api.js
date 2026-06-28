import axios from 'axios'

const BASE = 'http://localhost:8000'
const MOCK = 'http://localhost:8000/mock'

export const getMonthlyAnalytics = (month) =>
  axios.get(`${MOCK}/analytics/monthly`, { params: { month, size: 100 } }).then(r => r.data)

export const getYearlyAnalytics = (year) =>
  axios.get(`${BASE}/analytics/yearly`, { params: { year, size: 100 } }).then(r => r.data)

export const searchProfiles = (params) =>
  axios.get(`${MOCK}/profiles/detailed-search`, { params }).then(r => r.data)

export const getWorkDuration = () =>
  axios.get(`${MOCK}/profiles/work-duration`).then(r => r.data)

export const getProfiles = (size = 100) =>
  axios.get(`${BASE}/profiles`, { params: { size } }).then(r => r.data)
