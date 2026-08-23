import { useEffect, useState, useCallback } from 'react'
import './App.css'

const CONTROL_PLANE_URL = import.meta.env.VITE_CONTROL_PLANE_URL || 'http://localhost:8100'

type FleetEntry = {
  key: string
  store_id: string
  service_name: string
  version: number
  error_rate_pct: number
  healthy: boolean
  last_seen: string
}

type ConfigVersion = {
  service_name: string
  version: number
  canary_pct: number
  created_at: string
  promoted_at?: string
  rolled_back: boolean
}

function useFleet(pollMs: number) {
  const [fleet, setFleet] = useState<FleetEntry[]>([])
  const [connected, setConnected] = useState(false)

  const refresh = useCallback(async () => {
    try {
      const res = await fetch(`${CONTROL_PLANE_URL}/fleet`)
      if (!res.ok) throw new Error(String(res.status))
      const data = await res.json()
      setFleet(data ?? [])
      setConnected(true)
    } catch {
      setConnected(false)
    }
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, pollMs)
    return () => clearInterval(id)
  }, [refresh, pollMs])

  return { fleet, connected, refresh }
}

function timeAgo(iso: string) {
  const diff = Date.now() - new Date(iso).getTime()
  const s = Math.floor(diff / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  return `${m}m ago`
}

function StatusDot({ healthy }: { healthy: boolean }) {
  return <span className={`dot ${healthy ? 'dot-ok' : 'dot-bad'}`} />
}

function FleetTable({ fleet }: { fleet: FleetEntry[] }) {
  if (fleet.length === 0) {
    return <p className="muted">No health reports yet — start vertex-agent against a running store to populate this.</p>
  }
  return (
    <table className="table">
      <thead>
        <tr>
          <th></th>
          <th>Store</th>
          <th>Service</th>
          <th>Version</th>
          <th>Error rate</th>
          <th>Last seen</th>
        </tr>
      </thead>
      <tbody>
        {fleet.map((f) => (
          <tr key={f.key} className={f.healthy ? '' : 'row-bad'}>
            <td><StatusDot healthy={f.healthy} /></td>
            <td>{f.store_id}</td>
            <td className="mono">{f.service_name}</td>
            <td>v{f.version}</td>
            <td>{f.error_rate_pct.toFixed(2)}%</td>
            <td className="muted">{timeAgo(f.last_seen)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function CanaryPanel() {
  const [serviceName, setServiceName] = useState('vertex-core')
  const [versions, setVersions] = useState<ConfigVersion[]>([])
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${CONTROL_PLANE_URL}/configs/${serviceName}/versions`)
      if (res.ok) setVersions(await res.json())
    } catch {
      /* control plane / config service not reachable yet */
    }
  }, [serviceName])

  useEffect(() => {
    load()
    const id = setInterval(load, 4000)
    return () => clearInterval(id)
  }, [load])

  async function publishCanary() {
    setBusy(true)
    setMsg(null)
    try {
      const res = await fetch(`${CONTROL_PLANE_URL}/configs/${serviceName}?canary_pct=25`, {
        method: 'POST',
        body: JSON.stringify({ demo: true, ts: Date.now() }),
      })
      setMsg(res.ok ? 'Published new version at 25% canary.' : `Failed: ${res.status}`)
      await load()
    } finally {
      setBusy(false)
    }
  }

  async function promote(version: number) {
    setBusy(true)
    setMsg(null)
    try {
      const res = await fetch(`${CONTROL_PLANE_URL}/configs/${serviceName}/promote?version=${version}`, { method: 'POST' })
      setMsg(res.ok ? `Promoted v${version} to 100%.` : `Failed: ${res.status}`)
      await load()
    } finally {
      setBusy(false)
    }
  }

  async function rollback(version: number) {
    setBusy(true)
    setMsg(null)
    try {
      const res = await fetch(`${CONTROL_PLANE_URL}/configs/${serviceName}/rollback?version=${version}&reason=manual_dashboard`, { method: 'POST' })
      setMsg(res.ok ? `Rolled back v${version}.` : `Failed: ${res.status}`)
      await load()
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="card">
      <div className="card-header">
        <h2>Canary rollout — {serviceName}</h2>
        <select value={serviceName} onChange={(e) => setServiceName(e.target.value)}>
          {['vertex-core', 'vertex-intervention', 'vertex-weight'].map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>
      <button disabled={busy} onClick={publishCanary}>Publish new version (25% canary)</button>
      {msg && <p className="muted">{msg}</p>}
      <table className="table">
        <thead>
          <tr><th>Version</th><th>Canary %</th><th>Status</th><th>Actions</th></tr>
        </thead>
        <tbody>
          {versions.slice().reverse().map((v) => (
            <tr key={v.version}>
              <td>v{v.version}</td>
              <td>{v.canary_pct}%</td>
              <td>
                {v.rolled_back ? <span className="badge badge-bad">rolled back</span>
                  : v.canary_pct >= 100 ? <span className="badge badge-ok">promoted</span>
                  : <span className="badge badge-warn">canary</span>}
              </td>
              <td>
                {!v.rolled_back && v.canary_pct < 100 && (
                  <>
                    <button disabled={busy} onClick={() => promote(v.version)}>Promote</button>{' '}
                    <button disabled={busy} onClick={() => rollback(v.version)}>Rollback</button>
                  </>
                )}
              </td>
            </tr>
          ))}
          {versions.length === 0 && (
            <tr><td colSpan={4} className="muted">No versions published yet.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

export default function App() {
  const { fleet, connected } = useFleet(4000)
  const healthyCount = fleet.filter((f) => f.healthy).length

  return (
    <div className="app">
      <header className="app-header">
        <h1>Vertex SCO Platform</h1>
        <span className={`conn ${connected ? 'conn-ok' : 'conn-bad'}`}>
          {connected ? 'Connected to vertex-control-plane' : 'Control plane unreachable'}
        </span>
      </header>

      <section className="summary">
        <div className="stat">
          <span className="stat-value">{fleet.length}</span>
          <span className="stat-label">tracked deployments</span>
        </div>
        <div className="stat">
          <span className="stat-value">{healthyCount}/{fleet.length || 0}</span>
          <span className="stat-label">healthy</span>
        </div>
      </section>

      <div className="grid">
        <div className="card">
          <h2>Fleet health</h2>
          <FleetTable fleet={fleet} />
        </div>
        <CanaryPanel />
      </div>

      <footer className="app-footer">
        Vertex SCO Platform — architecture, flaw fixes, and service catalog in <code>docs/ARCHITECTURE.md</code>
      </footer>
    </div>
  )
}
