import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navbar      from './components/Navbar'
import Dashboard   from './pages/Dashboard'
import Search      from './pages/Search'
import WorkDuration from './pages/WorkDuration'
import Similarity  from './pages/Similarity'

export default function App() {
  return (
    <BrowserRouter>
      <div style={{ flex: 1 }}>
        <Navbar />
        <div style={{ padding: '2rem' }}>
          <Routes>
            <Route path="/"           element={<Dashboard />} />
            <Route path="/search"     element={<Search />} />
            <Route path="/duration"   element={<WorkDuration />} />
            <Route path="/similarity" element={<Similarity />} />
          </Routes>
        </div>
      </div>
    </BrowserRouter>
  )
}
