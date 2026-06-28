import { Link, useLocation } from 'react-router-dom'

const links = [
  { to: '/',           label: 'Dashboard'   },
  { to: '/search',     label: 'Search'      },
  { to: '/duration',   label: 'Work Duration'},
  { to: '/similarity', label: 'Similarity'  },
]

export default function Navbar() {
  const { pathname } = useLocation()
  return (
    <nav style={{ background: '#1e293b', padding: '0 2rem', display: 'flex', alignItems: 'center', gap: '2rem', height: 56 }}>
      <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: 18 }}>LinkHarvester</span>
      {links.map(l => (
        <Link key={l.to} to={l.to} style={{
          color: pathname === l.to ? '#38bdf8' : '#94a3b8',
          textDecoration: 'none', fontWeight: 500
        }}>{l.label}</Link>
      ))}
    </nav>
  )
}
