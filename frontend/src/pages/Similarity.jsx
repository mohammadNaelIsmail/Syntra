import { useEffect, useState } from 'react'
import { getMonthlyAnalytics } from '../api'

export default function Similarity() {
  const [data, setData]     = useState(null)
  const [month, setMonth]   = useState('')
  const [loading, setLoading] = useState(false)

  const load = () => {
    setLoading(true)
    getMonthlyAnalytics(month || undefined)
      .then(d => setData(d[0] || null))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  const Table = ({ title, rows }) => (
    <div style={{ background: '#1e293b', borderRadius: 12, padding: '1.5rem', flex: 1 }}>
      <h3 style={{ color: '#38bdf8', margin: '0 0 1rem' }}>{title}</h3>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
        <thead>
          <tr>{['Person','Most Similar To'].map(h => (
            <th key={h} style={{ textAlign: 'left', padding: '0.5rem 1rem', color: '#94a3b8', borderBottom: '1px solid #334155' }}>{h}</th>
          ))}</tr>
        </thead>
        <tbody>
          {rows?.map((r,i) => (
            <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
              <td style={{ padding: '0.5rem 1rem', color: '#e2e8f0' }}>{r.person}</td>
              <td style={{ padding: '0.5rem 1rem', color: '#34d399' }}>{r.most_similar_person}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )

  return (
    <div>
      <h2 style={{ color: '#38bdf8' }}>Person Similarity</h2>

      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1.5rem' }}>
        <input placeholder="Month (e.g. 2023-01)" value={month} onChange={e => setMonth(e.target.value)}
          style={{ padding: '0.5rem 1rem', borderRadius: 8, border: 'none', background: '#334155', color: '#e2e8f0', width: 200 }}/>
        <button onClick={load} style={{ padding: '0.5rem 1.5rem', borderRadius: 8, background: '#38bdf8', border: 'none', color: '#0f172a', fontWeight: 700, cursor: 'pointer' }}>
          Load
        </button>
      </div>

      {loading && <p>Loading...</p>}
      {!loading && !data && <p style={{ color: '#64748b' }}>No data available.</p>}

      {data && (
        <div style={{ display: 'flex', gap: '1.5rem' }}>
          <Table title="By Skills"     rows={data.similarity_by_skills}/>
          <Table title="By Companies"  rows={data.similarity_by_company}/>
        </div>
      )}
    </div>
  )
}
