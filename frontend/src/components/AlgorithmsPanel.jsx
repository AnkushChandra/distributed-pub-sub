import React, { useState, useEffect } from 'react'
import './AlgorithmsPanel.css'

const BROKER_PORTS = [8081, 8082, 8083, 8084]

function AlgorithmsPanel({ nodes, leader }) {
  const [stats, setStats] = useState({})
  const [activeTab, setActiveTab] = useState('bully')

  useEffect(() => {
    const fetchStats = async () => {
      const allStats = {}
      await Promise.all(
        BROKER_PORTS.map(async (port) => {
          const brokerId = port - 8080
          try {
            const res = await fetch(`http://localhost:${port}/stats`, {
              signal: AbortSignal.timeout(2000)
            })
            if (res.ok) {
              allStats[brokerId] = await res.json()
            }
          } catch {}
        })
      )
      setStats(allStats)
    }

    fetchStats()
    const interval = setInterval(fetchStats, 2000)
    return () => clearInterval(interval)
  }, [])

  const nodeList = Object.values(nodes).sort((a, b) => a.id - b.id)
  
  // Aggregate bully metrics
  const bullyMetrics = { 
    elections_started: 0, elections_won: 0, election_messages_sent: 0,
    election_messages_received: 0, heartbeats_sent: 0, heartbeats_received: 0,
    coordinator_changes: 0, failures_detected: 0, last_election_duration_ms: 0,
    total_election_rounds: 0, completed_elections: 0, last_election_rounds: 0
  }
  for (const brokerStats of Object.values(stats)) {
    const m = brokerStats.election?.metrics || {}
    bullyMetrics.elections_started += m.elections_started || 0
    bullyMetrics.elections_won += m.elections_won || 0
    bullyMetrics.election_messages_sent += m.election_messages_sent || 0
    bullyMetrics.election_messages_received += m.election_messages_received || 0
    bullyMetrics.heartbeats_sent += m.heartbeats_sent || 0
    bullyMetrics.heartbeats_received += m.heartbeats_received || 0
    bullyMetrics.coordinator_changes += m.coordinator_changes || 0
    bullyMetrics.failures_detected += m.failures_detected || 0
    bullyMetrics.total_election_rounds += m.total_election_rounds || 0
    bullyMetrics.completed_elections += m.completed_elections || 0
    if (m.last_election_rounds > bullyMetrics.last_election_rounds) {
      bullyMetrics.last_election_rounds = m.last_election_rounds
    }
    if (m.last_election_duration_ms > bullyMetrics.last_election_duration_ms) {
      bullyMetrics.last_election_duration_ms = m.last_election_duration_ms
    }
  }
  // Calculate average rounds per election
  const avgRoundsPerElection = bullyMetrics.completed_elections > 0 
    ? (bullyMetrics.total_election_rounds / bullyMetrics.completed_elections).toFixed(1) 
    : 0

  // Aggregate gossip metrics
  const gossipMetrics = { rounds: 0, messages_sent: 0, messages_received: 0, 
    members_discovered: 0, status_changes: 0, merges_applied: 0,
    propagation_tests: 0, avg_propagation_rounds: 0, last_propagation_rounds: 0 }
  let gossipData = {}
  for (const brokerStats of Object.values(stats)) {
    const g = brokerStats.gossip || {}
    const m = g.metrics || {}
    gossipMetrics.rounds += m.rounds || 0
    gossipMetrics.messages_sent += m.messages_sent || 0
    gossipMetrics.messages_received += m.messages_received || 0
    gossipMetrics.members_discovered += m.members_discovered || 0
    gossipMetrics.status_changes += m.status_changes || 0
    gossipMetrics.merges_applied += m.merges_applied || 0
    gossipMetrics.propagation_tests += m.propagation_tests || 0
    // Take the latest avg from any node that has run tests
    if ((m.avg_propagation_rounds || 0) > 0) {
      gossipMetrics.avg_propagation_rounds = m.avg_propagation_rounds
      gossipMetrics.last_propagation_rounds = m.last_propagation_rounds || 0
    }
    if (Object.keys(g.members || {}).length > Object.keys(gossipData.members || {}).length) {
      gossipData = g
    }
  }

  // Aggregate replication metrics
  const replMetrics = { push_attempts: 0, push_success: 0, push_failed: 0,
    pull_attempts: 0, pull_success: 0, pull_failed: 0, records_pushed: 0, records_pulled: 0,
    total_push_latency_ms: 0, total_pull_latency_ms: 0 }
  for (const brokerStats of Object.values(stats)) {
    const m = brokerStats.replication?.metrics || {}
    replMetrics.push_attempts += m.push_attempts || 0
    replMetrics.push_success += m.push_success || 0
    replMetrics.push_failed += m.push_failed || 0
    replMetrics.pull_attempts += m.pull_attempts || 0
    replMetrics.pull_success += m.pull_success || 0
    replMetrics.pull_failed += m.pull_failed || 0
    replMetrics.records_pushed += m.records_pushed || 0
    replMetrics.records_pulled += m.records_pulled || 0
    replMetrics.total_push_latency_ms += m.total_push_latency_ms || 0
    replMetrics.total_pull_latency_ms += m.total_pull_latency_ms || 0
  }
  const avgPushLatency = replMetrics.push_success > 0 ? 
    (replMetrics.total_push_latency_ms / replMetrics.push_success).toFixed(2) : 0
  const avgPullLatency = replMetrics.pull_success > 0 ? 
    (replMetrics.total_pull_latency_ms / replMetrics.pull_success).toFixed(2) : 0

  return (
    <div className="panel algorithms-panel">
      <div className="panel-header">
        <h2 className="panel-title">Algorithm Metrics</h2>
      </div>

      <div className="algo-tabs">
        <button className={`algo-tab ${activeTab === 'bully' ? 'active' : ''}`}
          onClick={() => setActiveTab('bully')}>Bully</button>
        <button className={`algo-tab ${activeTab === 'gossip' ? 'active' : ''}`}
          onClick={() => setActiveTab('gossip')}>Gossip</button>
        <button className={`algo-tab ${activeTab === 'replication' ? 'active' : ''}`}
          onClick={() => setActiveTab('replication')}>Replication</button>
      </div>

      <div className="algo-content">
        {activeTab === 'bully' && (
          <div className="algo-section">
            <div className="metrics-grid-4">
              <div className="metric-card highlight">
                <span className="metric-label">Leader</span>
                <span className="metric-value">P{leader || '?'}</span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Elections</span>
                <span className="metric-value">{bullyMetrics.elections_started}</span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Duration</span>
                <span className="metric-value">{bullyMetrics.last_election_duration_ms.toFixed(0)}ms</span>
              </div>
              <div className="metric-card highlight">
                <span className="metric-label">Avg Rounds</span>
                <span className="metric-value">{avgRoundsPerElection}</span>
              </div>
            </div>

            <div className="metrics-table">
              <div className="metrics-row">
                <span className="metrics-label">Election Msgs Sent</span>
                <span className="metrics-value">{bullyMetrics.election_messages_sent}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Election Msgs Recv</span>
                <span className="metrics-value">{bullyMetrics.election_messages_received}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Heartbeats Sent</span>
                <span className="metrics-value">{bullyMetrics.heartbeats_sent}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Heartbeats Recv</span>
                <span className="metrics-value">{bullyMetrics.heartbeats_received}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Leader Changes</span>
                <span className="metrics-value">{bullyMetrics.coordinator_changes}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Failures Detected</span>
                <span className="metrics-value error">{bullyMetrics.failures_detected}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Last Election Rounds</span>
                <span className="metrics-value">{bullyMetrics.last_election_rounds}</span>
              </div>
            </div>

            <div className="node-status-grid">
              {nodeList.map(node => {
                const nodeMetrics = stats[node.id]?.election?.metrics || {}
                return (
                  <div key={node.id} className={`node-status ${node.alive ? 'alive' : 'dead'} ${node.id === leader ? 'leader' : ''}`}>
                    <span className="node-label">P{node.id}</span>
                    <span className="node-stat">Won: {nodeMetrics.elections_won || 0}</span>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {activeTab === 'gossip' && (
          <div className="algo-section">
            <div className="metrics-grid-4">
              <div className="metric-card">
                <span className="metric-label">Rounds</span>
                <span className="metric-value">{gossipMetrics.rounds}</span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Msgs Sent</span>
                <span className="metric-value">{gossipMetrics.messages_sent}</span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Msgs Recv</span>
                <span className="metric-value">{gossipMetrics.messages_received}</span>
              </div>
              <div className="metric-card highlight">
                <span className="metric-label">Avg Propagation</span>
                <span className="metric-value">{gossipMetrics.avg_propagation_rounds.toFixed(1)} rounds</span>
              </div>
            </div>

            <div className="metrics-table">
              <div className="metrics-row">
                <span className="metrics-label">Members Discovered</span>
                <span className="metrics-value">{gossipMetrics.members_discovered}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Status Changes</span>
                <span className="metrics-value">{gossipMetrics.status_changes}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Merges Applied</span>
                <span className="metrics-value">{gossipMetrics.merges_applied}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Interval</span>
                <span className="metrics-value">{gossipData.interval_sec || 0.5}s</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Fanout</span>
                <span className="metrics-value">{gossipData.fanout || 3}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Propagation Tests</span>
                <span className="metrics-value">{gossipMetrics.propagation_tests}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Last Propagation</span>
                <span className="metrics-value">{gossipMetrics.last_propagation_rounds} rounds</span>
              </div>
            </div>

            <div className="gossip-members-mini">
              <h4>Membership</h4>
              <div className="members-row">
                {Object.entries(gossipData.members || {}).map(([mid, m]) => (
                  <div key={mid} className={`member-badge ${m.status}`}>
                    P{mid}
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {activeTab === 'replication' && (
          <div className="algo-section">
            <div className="metrics-grid-3">
              <div className="metric-card">
                <span className="metric-label">Pushed</span>
                <span className="metric-value">{replMetrics.records_pushed}</span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Pulled</span>
                <span className="metric-value">{replMetrics.records_pulled}</span>
              </div>
              <div className="metric-card highlight">
                <span className="metric-label">Avg Latency</span>
                <span className="metric-value">{avgPushLatency}ms</span>
              </div>
            </div>

            <div className="metrics-table">
              <div className="metrics-row">
                <span className="metrics-label">Push Attempts</span>
                <span className="metrics-value">{replMetrics.push_attempts}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Push Success</span>
                <span className="metrics-value success">{replMetrics.push_success}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Push Failed</span>
                <span className="metrics-value error">{replMetrics.push_failed}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Pull Attempts</span>
                <span className="metrics-value">{replMetrics.pull_attempts}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Pull Success</span>
                <span className="metrics-value success">{replMetrics.pull_success}</span>
              </div>
              <div className="metrics-row">
                <span className="metrics-label">Pull Failed</span>
                <span className="metrics-value error">{replMetrics.pull_failed}</span>
              </div>
            </div>

            <div className="repl-success-rate">
              <span className="rate-label">Success Rate</span>
              <div className="rate-bar">
                <div className="rate-fill" style={{ 
                  width: `${replMetrics.push_attempts > 0 ? (replMetrics.push_success / replMetrics.push_attempts * 100) : 100}%` 
                }}/>
              </div>
              <span className="rate-value">
                {replMetrics.push_attempts > 0 ? 
                  (replMetrics.push_success / replMetrics.push_attempts * 100).toFixed(1) : 100}%
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default AlgorithmsPanel
