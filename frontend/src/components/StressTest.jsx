import React, { useState, useRef, useCallback } from 'react'
import './StressTest.css'

function StressTest({ topics, nodes, leader }) {
  const [config, setConfig] = useState({
    topic: '',
    messagesPerSec: 10,
    duration: 10,
    messageSize: 100,
  })
  const [results, setResults] = useState(null)
  const [running, setRunning] = useState(false)
  const [liveStats, setLiveStats] = useState({ sent: 0, success: 0, failed: 0, latencies: [] })
  const abortRef = useRef(false)

  const runPublishTest = useCallback(async () => {
    if (!config.topic) {
      alert('Select a topic first')
      return
    }

    setRunning(true)
    abortRef.current = false
    const stats = { sent: 0, success: 0, failed: 0, latencies: [], startTime: Date.now() }
    setLiveStats({ sent: 0, success: 0, failed: 0, latencies: [] })

    const leaderPort = leader ? 8080 + leader : 8081
    const interval = 1000 / config.messagesPerSec
    const endTime = Date.now() + config.duration * 1000
    const payload = 'x'.repeat(config.messageSize)

    const publish = async () => {
      if (abortRef.current || Date.now() > endTime) return false
      
      const start = performance.now()
      stats.sent++
      
      try {
        const res = await fetch(`http://localhost:${leaderPort}/topics/${config.topic}/publish`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ value: payload, key: `stress-${stats.sent}` }),
          signal: AbortSignal.timeout(5000)
        })
        
        const latency = performance.now() - start
        stats.latencies.push(latency)
        
        if (res.ok) {
          stats.success++
        } else {
          stats.failed++
        }
      } catch (e) {
        stats.failed++
      }
      
      setLiveStats({ ...stats, latencies: [...stats.latencies] })
      return true
    }

    while (Date.now() < endTime && !abortRef.current) {
      const batchStart = Date.now()
      await publish()
      const elapsed = Date.now() - batchStart
      if (elapsed < interval) {
        await new Promise(r => setTimeout(r, interval - elapsed))
      }
    }

    const duration = (Date.now() - stats.startTime) / 1000
    const latencies = stats.latencies.sort((a, b) => a - b)
    
    setResults({
      totalSent: stats.sent,
      success: stats.success,
      failed: stats.failed,
      duration: duration.toFixed(2),
      throughput: (stats.success / duration).toFixed(2),
      avgLatency: latencies.length ? (latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(2) : 0,
      p50: latencies.length ? latencies[Math.floor(latencies.length * 0.5)]?.toFixed(2) : 0,
      p95: latencies.length ? latencies[Math.floor(latencies.length * 0.95)]?.toFixed(2) : 0,
      p99: latencies.length ? latencies[Math.floor(latencies.length * 0.99)]?.toFixed(2) : 0,
    })
    
    setRunning(false)
  }, [config, leader])

  const stopTest = () => {
    abortRef.current = true
  }

  const topicList = Object.keys(topics)

  return (
    <div className="panel stress-test">
      <div className="panel-header">
        <h2 className="panel-title">Stress Test</h2>
      </div>

      <div className="test-config">
        <div className="config-row">
          <label>Topic</label>
          <select 
            value={config.topic} 
            onChange={e => setConfig({...config, topic: e.target.value})}
            disabled={running}
          >
            <option value="">Select...</option>
            {topicList.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>

        <div className="config-grid">
          <div className="config-item">
            <label>Msg/sec</label>
            <input 
              type="number" 
              value={config.messagesPerSec}
              onChange={e => setConfig({...config, messagesPerSec: parseInt(e.target.value) || 1})}
              min="1" max="1000"
              disabled={running}
            />
          </div>
          <div className="config-item">
            <label>Duration</label>
            <input 
              type="number" 
              value={config.duration}
              onChange={e => setConfig({...config, duration: parseInt(e.target.value) || 1})}
              min="1" max="300"
              disabled={running}
            />
          </div>
          <div className="config-item">
            <label>Size (B)</label>
            <input 
              type="number" 
              value={config.messageSize}
              onChange={e => setConfig({...config, messageSize: parseInt(e.target.value) || 10})}
              min="10" max="10000"
              disabled={running}
            />
          </div>
        </div>
      </div>

      <div className="test-actions">
        {!running ? (
          <button className="test-btn" onClick={runPublishTest}>Run Test</button>
        ) : (
          <button className="test-btn stop" onClick={stopTest}>Stop</button>
        )}
      </div>

      {running && (
        <div className="live-stats">
          <div className="live-row">
            <span className="live-item">Sent: <strong>{liveStats.sent}</strong></span>
            <span className="live-item success">OK: <strong>{liveStats.success}</strong></span>
            <span className="live-item error">Fail: <strong>{liveStats.failed}</strong></span>
          </div>
          <div className="progress-bar">
            <div className="progress-fill" style={{ 
              width: `${(liveStats.sent / (config.messagesPerSec * config.duration)) * 100}%` 
            }}/>
          </div>
        </div>
      )}

      {results && !running && (
        <div className="test-results">
          <div className="results-grid">
            <div className="result-item highlight">
              <span className="result-label">Throughput</span>
              <span className="result-value">{results.throughput}/s</span>
            </div>
            <div className="result-item highlight">
              <span className="result-label">Avg Latency</span>
              <span className="result-value">{results.avgLatency}ms</span>
            </div>
            <div className="result-item">
              <span className="result-label">Success</span>
              <span className="result-value success">{results.success}</span>
            </div>
            <div className="result-item">
              <span className="result-label">Failed</span>
              <span className="result-value error">{results.failed}</span>
            </div>
            <div className="result-item">
              <span className="result-label">P50</span>
              <span className="result-value">{results.p50}ms</span>
            </div>
            <div className="result-item">
              <span className="result-label">P95</span>
              <span className="result-value">{results.p95}ms</span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default StressTest
