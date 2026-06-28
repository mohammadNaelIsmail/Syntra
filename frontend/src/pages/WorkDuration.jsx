import { useEffect, useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { getWorkDuration } from '../api'

export default function WorkDuration() {
  const [data, setData]     = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getWorkDuration().then(setData).finally(() => setLoading(false))
  }, [])

  const chartData = data.slice(0,20).map(d => ({ name: d.person_id, months: d.total_months, company: d.company }))

  return (
    <div>
      <h2 style={{ color: '#38bdf8' }}>Longest Work Duration per Person</h2>
      {loading && <p>Loading...</p>}
      {!loading && data.length === 0 && <p style={{ color: '#64748b' }}>No data available.</p>}

      {data.length > 0 && <>
        <div style={{ background: '#1e293b', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={chartData} layout="vertical">
              <XAxis type="number" tick={{ fill: '#94a3b8' }} label={{ value: 'Months', position: 'insideBottom', fill: '#94a3b8' }}/>
              <YAxis dataKey="name" type="category" tick={{ fill: '#94a3b8', fontSize: 11 }} width={80}/>
              <Tooltip contentStyle={{ background: '#1e293b', border: 'none' }} formatter={(v, _, p) => [`${v} months at ${p.payload.company}`]}/>
              <Bar dataKey="months" fill="#818cf8" radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
            <thead>
              <tr>{['Person','Name','Company','Months','Duration'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '0.6rem 1rem', color: '#38bdf8', borderBottom: '1px solid #334155' }}>{h}</th>
              ))}</tr>
            </thead>
            <tbody>
              {data.map((r,i) => (
                <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                  {[r.person_id, r.name, r.company, r.total_months, r.duration].map((v,j) => (
                    <td key={j} style={{ padding: '0.6rem 1rem', color: '#e2e8f0' }}>{v}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>}
    </div>
  )
}
