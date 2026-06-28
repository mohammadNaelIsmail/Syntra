import { useEffect, useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend } from 'recharts'
import { getMonthlyAnalytics } from '../api'

const COLORS = ['#38bdf8','#818cf8','#34d399','#fb923c','#f472b6','#a78bfa','#facc15','#4ade80','#f87171','#22d3ee']

const Card = ({ title, children }) => (
  <div style={{ background: '#1e293b', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
    <h3 style={{ margin: '0 0 1rem', color: '#38bdf8' }}>{title}</h3>
    {children}
  </div>
)

export default function Dashboard() {
  const [data, setData]     = useState(null)
  const [month, setMonth]   = useState('')
  const [loading, setLoading] = useState(false)

  const load = () => {
    setLoading(true)

    getMonthlyAnalytics(month || undefined)
        .then(d => {
          console.log("Response:", d)
          setData(d[0] || null)
        })
        .catch(err => {
          console.error(err)
        })
        .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  return (
    <div>
      <h2 style={{ color: '#38bdf8' }}>Monthly Dashboard</h2>

      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1.5rem' }}>
        <input
          placeholder="Month (e.g. 2023-01)"
          value={month}
          onChange={e => setMonth(e.target.value)}
          style={{ padding: '0.5rem 1rem', borderRadius: 8, border: 'none', background: '#334155', color: '#e2e8f0', width: 200 }}
        />
        <button onClick={load} style={{ padding: '0.5rem 1.5rem', borderRadius: 8, background: '#38bdf8', border: 'none', color: '#0f172a', fontWeight: 700, cursor: 'pointer' }}>
          Load
        </button>
      </div>

      {loading && <p>Loading...</p>}
      {!loading && !data && <p style={{ color: '#64748b' }}>No data. Start the API and Elasticsearch first.</p>}

      {data && <>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
          <Card title={`Top Skills — ${data.month}`}>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={data.skills_people_count?.slice(0,10)}>
                <XAxis dataKey="skill" tick={{ fill: '#94a3b8', fontSize: 11 }} angle={-30} textAnchor="end" height={60}/>
                <YAxis tick={{ fill: '#94a3b8' }}/>
                <Tooltip contentStyle={{ background: '#1e293b', border: 'none' }}/>
                <Bar dataKey="count" fill="#38bdf8" radius={[4,4,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </Card>

          <Card title={`Top Companies — ${data.month}`}>
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={data.companies_people_count?.slice(0,8)} dataKey="count" nameKey="company" cx="50%" cy="50%" outerRadius={100} label>
                  {data.companies_people_count?.slice(0,8).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]}/>)}
                </Pie>
                <Legend wrapperStyle={{ color: '#94a3b8' }}/>
                <Tooltip contentStyle={{ background: '#1e293b', border: 'none' }}/>
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </div>

        <Card title="Best of the Month">
          <p>🏆 Best by Skills: <strong style={{ color: '#34d399' }}>{data.best_person_in_month_by_skills}</strong></p>
          <p>🏢 Best by Companies: <strong style={{ color: '#fb923c' }}>{data.best_person_in_month_by_company}</strong></p>
        </Card>
      </>}
    </div>
  )
}
