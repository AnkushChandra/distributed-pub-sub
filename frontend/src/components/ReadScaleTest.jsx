import React, { useState, useRef, useCallback } from 'react'
import './ReadScaleTest.css'

const BROKER_PORTS = [8081, 8082, 8083, 8084]

function ReadScaleTest({ topics, leader }) {
  const [config, setConfig] = useState({
    topic: '',
    duration: 10,
    subscriberCounts: [1, 5, 10, 20],
  })
  const [results, setResults] = useState([])
  const [running, setRunning] = useState(false)
  const [currentTest, setCurrentTest] = useState(null)
  const [liveStats, setLiveStats] = useState({ subscribers: 0, totalMessages: 0, elapsed: 0 })
  const abortRef = useRef(false)

  const runSubscriberTest = useCallback(async (numSubscribers, durationSec) => {
    const stats = {
      subscribers: numSubscribers,
      messagesPerSubscriber: new Array(numSubscribers).fill(0),
      startTime: Date.now(),
    }

    // Create subscriber workers
    const runSubscriber = async (subscriberId) => {
      const port = BROKER_PORTS[subscriberId % BROKER_PORTS.length]
      let offset = 0
      let messages = 0
      const endTime = Date.now() + durationSec * 1000

      while (Date.now() < endTime && !abortRef.current) {
        try {
          const res = await fetch(
            `http://localhost:${port}/poll?topic=${config.topic}&from_offset=${offset}&max_records=100&timeout_ms=1000&consumer_id=scale-test-${subscriberId}`,
            { signal: AbortSignal.timeout(3000) }
          )
          if (res.ok) {
            const data = await res.json()
            const records = data.records || []
            messages += records.length
            if (records.length > 0) {
              offset = data.next_offset
            }
            stats.messagesPerSubscriber[subscriberId] = messages
          }
        } catch (e) {
          // Continue on error
        }
      }
      return messages
    }

    // Run all subscribers in parallel
    const promises = []
    for (let i = 0; i < numSubscribers; i++) {
      promises.push(runSubscriber(i))
    }

    // Update live stats periodically
    const statsInterval = setInterval(() => {
      const total = stats.messagesPerSubscriber.reduce((a, b) => a + b, 0)
      const elapsed = (Date.now() - stats.startTime) / 1000
      setLiveStats({ subscribers: numSubscribers, totalMessages: total, elapsed })
    }, 500)

    await Promise.all(promises)
    clearInterval(statsInterval)

    const totalMessages = stats.messagesPerSubscriber.reduce((a, b) => a + b, 0)
    const actualDuration = (Date.now() - stats.startTime) / 1000
    const throughput = totalMessages / actualDuration

    const perSubscriberThroughput = throughput / numSubscribers

    return {
      subscribers: numSubscribers,
      totalMessages,
      duration: actualDuration.toFixed(2),
      throughput: throughput.toFixed(2),
      perSubscriberThroughput: perSubscriberThroughput.toFixed(2),
      avgPerSubscriber: (totalMessages / numSubscribers).toFixed(0),
    }
  }, [config.topic])

  const runFullTest = useCallback(async () => {
    if (!config.topic) {
      alert('Select a topic first')
      return
    }

    setRunning(true)
    setResults([])
    abortRef.current = false

    const testResults = []

    for (const count of config.subscriberCounts) {
      if (abortRef.current) break

      setCurrentTest(count)
      setLiveStats({ subscribers: count, totalMessages: 0, elapsed: 0 })

      const result = await runSubscriberTest(count, config.duration)
      testResults.push(result)
      setResults([...testResults])
    }

    setCurrentTest(null)
    setRunning(false)
  }, [config, runSubscriberTest])

  const stopTest = () => {
    abortRef.current = true
  }

  const topicList = Object.keys(topics)

  const copyLatex = () => {
    const latex = results.map(r => 
      `\\item ${r.subscribers} Subscriber${r.subscribers > 1 ? 's' : ''}: \\textit{${r.throughput} msg/sec} (${r.perSubscriberThroughput} msg/sec per subscriber)`
    ).join('\n')
    navigator.clipboard.writeText(latex)
    alert('LaTeX copied to clipboard!')
  }

  return (
    <div className="panel read-scale-test">
      <div className="panel-header">
        <h2 className="panel-title">Read Scaling Test</h2>
      </div>

      <div className="test-description">
        Measures aggregate read throughput as concurrent subscribers increase.
        Subscribers are distributed across replicas.
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

        <div className="config-row">
          <label>Duration per test (sec)</label>
          <input 
            type="number" 
            value={config.duration}
            onChange={e => setConfig({...config, duration: parseInt(e.target.value) || 5})}
            min="5" max="60"
            disabled={running}
          />
        </div>

        <div className="config-row">
          <label>Subscriber counts</label>
          <input 
            type="text" 
            value={config.subscriberCounts.join(', ')}
            onChange={e => {
              const counts = e.target.value.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n) && n > 0)
              if (counts.length > 0) setConfig({...config, subscriberCounts: counts})
            }}
            disabled={running}
            placeholder="1, 5, 10, 20"
          />
        </div>
      </div>

      <div className="test-actions">
        {!running ? (
          <button className="test-btn" onClick={runFullTest}>Run Scaling Test</button>
        ) : (
          <button className="test-btn stop" onClick={stopTest}>Stop</button>
        )}
      </div>

      {running && currentTest && (
        <div className="live-stats">
          <div className="current-test">
            Testing with <strong>{currentTest}</strong> subscriber{currentTest > 1 ? 's' : ''}...
          </div>
          <div className="live-row">
            <span>Messages: <strong>{liveStats.totalMessages}</strong></span>
            <span>Elapsed: <strong>{liveStats.elapsed.toFixed(1)}s</strong></span>
            <span>Rate: <strong>{liveStats.elapsed > 0 ? (liveStats.totalMessages / liveStats.elapsed).toFixed(0) : 0}</strong> msg/s</span>
          </div>
        </div>
      )}

      {results.length > 0 && (
        <div className="test-results">
          <div className="results-header">
            <h4>Results</h4>
            <button className="copy-btn" onClick={copyLatex}>Copy LaTeX</button>
          </div>
          
          <table className="results-table">
            <thead>
              <tr>
                <th>Subscribers</th>
                <th>Total Msgs</th>
                <th>Aggregate</th>
                <th>Per Subscriber</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={i}>
                  <td>{r.subscribers}</td>
                  <td>{r.totalMessages}</td>
                  <td className="highlight">{r.throughput} msg/s</td>
                  <td className="per-sub">{r.perSubscriberThroughput} msg/s</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="scaling-chart">
            <div className="chart-title">Throughput Scaling</div>
            <div className="chart-bars">
              {results.map((r, i) => {
                const maxThroughput = Math.max(...results.map(x => parseFloat(x.throughput)))
                const height = maxThroughput > 0 ? (parseFloat(r.throughput) / maxThroughput) * 100 : 0
                return (
                  <div key={i} className="bar-container">
                    <div className="bar" style={{ height: `${height}%` }}>
                      <span className="bar-value">{r.throughput}</span>
                    </div>
                    <span className="bar-label">{r.subscribers}</span>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default ReadScaleTest
