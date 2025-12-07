import { useState, useRef, useCallback } from 'react'
import './FailoverTest.css'

const BROKER_PORTS = [8081, 8082, 8083, 8084]

function LeaderFailoverTest({ nodes, leader }) {
  const [config, setConfig] = useState({
    pollInterval: 100, // ms between leader checks
  })
  const [results, setResults] = useState(null)
  const [running, setRunning] = useState(false)
  const [phase, setPhase] = useState('')
  const [liveData, setLiveData] = useState({ checks: 0, currentLeader: null })
  const abortRef = useRef(false)

  const runLeaderFailoverTest = useCallback(async () => {
    if (!leader) {
      alert('No cluster leader detected')
      return
    }

    const oldLeader = leader
    const oldLeaderPort = 8080 + oldLeader

    setRunning(true)
    setResults(null)
    abortRef.current = false

    const testData = {
      failureDetectedAt: null,
      electionStartedAt: null,
      electionCompletedAt: null,
      checksBeforeFailure: 0,
      checksAfterFailure: 0,
    }

    let checks = 0

    const checkLeader = async (port) => {
      try {
        const res = await fetch(`http://localhost:${port}/admin/leader`, {
          signal: AbortSignal.timeout(1000)
        })
        if (res.ok) {
          const data = await res.json()
          return { alive: true, isLeader: data.this_broker_is_leader, leaderId: data.leader_id }
        }
      } catch (e) {}
      return { alive: false, isLeader: false, leaderId: null }
    }

    // Phase 1: Verify current leader is alive
    setPhase(`Monitoring leader P${oldLeader}... (stop the broker now!)`)
    
    const baselineEnd = Date.now() + 3000
    while (Date.now() < baselineEnd && !abortRef.current) {
      const status = await checkLeader(oldLeaderPort)
      checks++
      setLiveData({ checks, currentLeader: status.alive ? oldLeader : null })
      if (!status.alive) break
      testData.checksBeforeFailure = checks
      await new Promise(r => setTimeout(r, config.pollInterval))
    }

    // Phase 2: Wait for leader failure detection
    const failureStart = Date.now()
    let failureDetected = false
    let consecutiveFailures = 0

    while (!failureDetected && !abortRef.current && (Date.now() - failureStart) < 30000) {
      const status = await checkLeader(oldLeaderPort)
      checks++
      setLiveData({ checks, currentLeader: status.alive ? oldLeader : '?' })
      
      if (!status.alive) {
        consecutiveFailures++
        if (consecutiveFailures >= 2 && !testData.failureDetectedAt) {
          testData.failureDetectedAt = Date.now()
          failureDetected = true
          setPhase('Leader failure detected! Waiting for election...')
        }
      } else {
        consecutiveFailures = 0
      }
      await new Promise(r => setTimeout(r, config.pollInterval))
    }

    if (!failureDetected) {
      setPhase('Timeout waiting for failure detection')
      setRunning(false)
      return
    }

    // Phase 3: Wait for new leader election
    testData.electionStartedAt = Date.now()
    let newLeader = null
    let electionTimeout = Date.now() + 15000

    while (!newLeader && Date.now() < electionTimeout && !abortRef.current) {
      for (const port of BROKER_PORTS) {
        if (port === oldLeaderPort) continue
        const status = await checkLeader(port)
        checks++
        if (status.isLeader) {
          newLeader = port - 8080
          testData.electionCompletedAt = Date.now()
          setPhase(`New leader elected: P${newLeader}`)
          setLiveData({ checks, currentLeader: newLeader })
          break
        }
      }
      if (!newLeader) {
        await new Promise(r => setTimeout(r, config.pollInterval))
      }
    }

    if (!newLeader) {
      setPhase('Timeout waiting for new leader')
      setRunning(false)
      return
    }

    // Phase 4: Verify stability
    setPhase('Verifying leader stability...')
    const stabilityEnd = Date.now() + 2000
    let stable = true
    while (Date.now() < stabilityEnd && !abortRef.current) {
      const status = await checkLeader(8080 + newLeader)
      checks++
      testData.checksAfterFailure++
      if (!status.isLeader) {
        stable = false
        break
      }
      setLiveData({ checks, currentLeader: newLeader })
      await new Promise(r => setTimeout(r, config.pollInterval))
    }

    // Calculate results
    const failureDetectionTime = testData.failureDetectedAt 
      ? (testData.failureDetectedAt - failureStart) / 1000 
      : null
    const electionTime = testData.electionCompletedAt && testData.electionStartedAt
      ? testData.electionCompletedAt - testData.electionStartedAt
      : null
    const totalRecoveryTime = testData.electionCompletedAt && testData.failureDetectedAt
      ? (testData.electionCompletedAt - testData.failureDetectedAt) / 1000
      : null

    setResults({
      failureDetectionTime: failureDetectionTime?.toFixed(2),
      electionTime: electionTime,
      totalRecoveryTime: totalRecoveryTime?.toFixed(2),
      oldLeader: oldLeader,
      newLeader: newLeader,
      stable: stable ? 'Yes' : 'No',
      totalChecks: checks,
    })

    setPhase('Test complete!')
    setRunning(false)
  }, [config, leader])

  const stopTest = () => {
    abortRef.current = true
  }

  const copyLatex = () => {
    if (!results) return
    const latex = `\\item Failure Detection Time: \\textit{${results.failureDetectionTime} sec}
\\item Election Time: \\textit{${results.electionTime} ms}
\\item Total Recovery Time (RTO): \\textit{${results.totalRecoveryTime} sec}
\\item Leader Stability After Election: \\textit{${results.stable}}
\\item Failover: \\textit{P${results.oldLeader} → P${results.newLeader}}`
    navigator.clipboard.writeText(latex)
    alert('LaTeX copied to clipboard!')
  }

  return (
    <div className="panel failover-test">
      <div className="panel-header">
        <h2 className="panel-title">Leader Failover Test</h2>
      </div>

      <div className="test-description">
        Measures Bully election performance when cluster leader fails.
        Stop the current leader broker (P{leader || '?'}) during the test.
      </div>

      <div className="test-config">
        <div className="config-row">
          <label>Current Leader</label>
          <span className="config-value">P{leader || 'None'}</span>
        </div>

        <div className="config-row">
          <label>Poll interval (ms)</label>
          <input 
            type="number" 
            value={config.pollInterval}
            onChange={e => setConfig({...config, pollInterval: parseInt(e.target.value) || 100})}
            min="50" max="1000"
            disabled={running}
          />
        </div>
      </div>

      <div className="test-actions">
        {!running ? (
          <button className="test-btn" onClick={runLeaderFailoverTest} disabled={!leader}>
            Run Leader Failover Test
          </button>
        ) : (
          <button className="test-btn stop" onClick={stopTest}>Stop</button>
        )}
      </div>

      {running && (
        <div className="live-stats">
          <div className="phase">{phase}</div>
          <div className="live-row">
            <span>Checks: <strong>{liveData.checks}</strong></span>
            <span>Leader: <strong>P{liveData.currentLeader}</strong></span>
          </div>
        </div>
      )}

      {results && (
        <div className="test-results">
          <div className="results-header">
            <h4>Results</h4>
            <button className="copy-btn" onClick={copyLatex}>Copy LaTeX</button>
          </div>

          <div className="results-grid">
            <div className="result-item">
              <span className="result-label">Failure Detection</span>
              <span className="result-value">{results.failureDetectionTime} sec</span>
            </div>
            <div className="result-item">
              <span className="result-label">Election Time</span>
              <span className="result-value">{results.electionTime} ms</span>
            </div>
            <div className="result-item highlight">
              <span className="result-label">Total Recovery (RTO)</span>
              <span className="result-value">{results.totalRecoveryTime} sec</span>
            </div>
            <div className="result-item">
              <span className="result-label">Leader Stable</span>
              <span className={`result-value ${results.stable === 'Yes' ? 'success' : 'error'}`}>
                {results.stable}
              </span>
            </div>
          </div>

          <div className="result-detail">
            <span className="detail-label">Failover:</span>
            <span className="detail-value">P{results.oldLeader} → P{results.newLeader}</span>
          </div>

          <div className="result-detail">
            <span className="detail-label">Total Checks:</span>
            <span className="detail-value">{results.totalChecks}</span>
          </div>
        </div>
      )}
    </div>
  )
}

export default LeaderFailoverTest
