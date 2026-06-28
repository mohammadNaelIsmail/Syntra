import { useState } from 'react'
import { searchProfiles } from '../api'

export default function Search() {
  const [form, setForm]     = useState({ company: '', skill: '', date_from: '', date_to: '' })
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)

  const search = () => {
    setLoading(true)
    const params = Object.fromEntries(Object.entries(form).filter(([,v]) => v))
    searchProfiles(params).then(setResults).finally(() => setLoading(false))
  }

  return (
    <div>
      <h2 style={{ color: '#38bdf8' }}>Search Profiles</h2>

      <div style={{ background: '#1e293b', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
        {[['company','Company'],['skill','Skill'],['date_from','From (YYYY-MM-DD)'],['date_to','To (YYYY-MM-DD)']].map(([k,label]) => (
          <div key={k}>
            <label style={{ color: '#94a3b8', fontSize: 13 }}>{label}</label>
            <input value={form[k]} onChange={e => setForm(f => ({...f, [k]: e.target.value}))}
              style={{ display: 'block', width: '100%', marginTop: 4, padding: '0.5rem', borderRadius: 8, border: 'none', background: '#334155', color: '#e2e8f0' }}/>
          </div>
        ))}
        <button onClick={search} style={{ gridColumn: 'span 2', padding: '0.6rem', borderRadius: 8, background: '#38bdf8', border: 'none', color: '#0f172a', fontWeight: 700, cursor: 'pointer' }}>
          Search
        </button>
      </div>

      {loading && <p>Searching...</p>}
      <p style={{ color: '#64748b' }}>{results.length} results</p>

      <div style={{ overflowX: 'auto' }}>
        {results.length > 0 && (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
            <thead>
              <tr>{['Person','Name','Company','From','To','Duration'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '0.6rem 1rem', color: '#38bdf8', borderBottom: '1px solid #334155' }}>{h}</th>
              ))}</tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                  {[r.person_id, r.name, r.company, r.date_from, r.date_to, r.duration].map((v,j) => (
                    <td key={j} style={{ padding: '0.6rem 1rem', color: '#e2e8f0' }}>{v}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
