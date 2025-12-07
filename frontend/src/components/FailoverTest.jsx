import React, { useState, useRef, useCallback } from 'react'
import './FailoverTest.css'

const BROKER_PORTS = [8081, 8082, 8083, 8084]

function FailoverTest({ topics, nodes, leader }) {
  const [config, setConfig] = useState({
    topic: '',
    publishRate: 10, // msgs per second during test
  })
  const [results, setResults] = useState(null)
  const [running, setRunning] = useState(false)
  const [phase, setPhase] = useState('')
  const [liveData, setLiveData] = useState({ published: 0, lastOffset: -1 })
  const abortRef = useRef(false)

  const runFailoverTest = useCallback(async () => {
    if (!config.topic) {
      alert('Select a topic first')
      return
    }

    // Find the topic owner
    const topic = topics[config.topic]
    if (!topic) {
      alert('Topic not found')
      return
    }
    const ownerPort = 8080 + topic.owner

    setRunning(true)
    setResults(null)
    abortRef.current = false

    const testData = {
      publishedOffsets: [],
      failureDetectedAt: null,
      electionStartedAt: null,
      electionCompletedAt: null,
      newLeaderAt: null,
      firstSuccessAfterFailover: null,
      messagesBeforeFailover: 0,
      messagesAfterFailover: 0,
      offsetGaps: [],
    }

    // Phase 1: Publish messages to establish baseline
    setPhase('Publishing baseline messages...')
    const publishInterval = 1000 / config.publishRate
    let lastOffset = -1
    let published = 0

    const publish = async (port) => {
      try {
        const res = await fetch(`http://localhost:${port}/topics/${config.topic}/publish`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ value: `failover-test-${Date.now()}`, key: `ft-${published}` }),
          signal: AbortSignal.timeout(2000)
        })
        if (res.ok) {
          const data = await res.json()
          const offset = data.offset
          if (lastOffset >= 0 && offset !== lastOffset + 1) {
            testData.offsetGaps.push({ expected: lastOffset + 1, got: offset })
          }
          lastOffset = offset
          testData.publishedOffsets.push({ offset, time: Date.now() })
          published++
          setLiveData({ published, lastOffset: offset })
          return true
        }
      } catch (e) {
        return false
      }
      return false
    }

    // Publish for 3 seconds to establish baseline
    const baselineEnd = Date.now() + 3000
    while (Date.now() < baselineEnd && !abortRef.current) {
      await publish(ownerPort)
      await new Promise(r => setTimeout(r, publishInterval))
    }
    testData.messagesBeforeFailover = published

    // Phase 2: Simulate owner failure (stop publishing to owner, detect failure)
    setPhase(`Simulating failure of owner P${topic.owner}... (stop the broker now!)`)
    
    // Wait for user to kill the broker - detect by failed publishes
    const failureStart = Date.now()
    let failureDetected = false
    let consecutiveFailures = 0
    
    while (!failureDetected && !abortRef.current && (Date.now() - failureStart) < 30000) {
      const success = await publish(ownerPort)
      if (!success) {
        consecutiveFailures++
        if (consecutiveFailures >= 3 && !testData.failureDetectedAt) {
          testData.failureDetectedAt = Date.now()
          failureDetected = true
          setPhase('Failure detected! Waiting for election...')
        }
      } else {
        consecutiveFailures = 0
      }
      await new Promise(r => setTimeout(r, publishInterval))
    }

    if (!failureDetected) {
      setPhase('Timeout waiting for failure detection')
      setRunning(false)
      return
    }

    // Phase 3: Wait for new topic owner (check metadata for topic ownership change)
    testData.electionStartedAt = Date.now()
    let newTopicOwner = null
    let electionTimeout = Date.now() + 15000

    while (!newTopicOwner && Date.now() < electionTimeout && !abortRef.current) {
      // Check all other brokers for updated metadata showing new topic owner
      for (const port of BROKER_PORTS) {
        if (port === ownerPort) continue
        try {
          const res = await fetch(`http://localhost:${port}/metadata`, {
            signal: AbortSignal.timeout(1000)
          })
          if (res.ok) {
            const data = await res.json()
            const topicBrokers = data.topics?.[config.topic]
            if (topicBrokers && topicBrokers.length > 0) {
              const currentOwner = topicBrokers[0]
              // New owner must be different from the failed owner
              if (currentOwner !== topic.owner) {
                newTopicOwner = currentOwner
                testData.electionCompletedAt = Date.now()
                testData.newLeaderAt = Date.now()
                setPhase(`New topic owner: P${newTopicOwner}`)
                break
              }
            }
          }
        } catch (e) {}
      }
      if (!newTopicOwner) {
        await new Promise(r => setTimeout(r, 200))
      }
    }

    if (!newTopicOwner) {
      setPhase('Timeout waiting for new topic owner')
      setRunning(false)
      return
    }

    // Phase 4: Resume publishing to new topic owner
    setPhase('Resuming publishing to new owner...')
    const newOwnerPort = 8080 + newTopicOwner
    const resumeStart = Date.now()
    let firstSuccessTime = null
    let resumeAttempts = 0

    while (!firstSuccessTime && resumeAttempts < 50 && !abortRef.current) {
      const success = await publish(newOwnerPort)
      resumeAttempts++
      if (success) {
        firstSuccessTime = Date.now()
        testData.firstSuccessAfterFailover = firstSuccessTime
      }
      await new Promise(r => setTimeout(r, 100))
    }

    // Continue publishing for a bit to verify stability
    const stabilityEnd = Date.now() + 3000
    while (Date.now() < stabilityEnd && !abortRef.current) {
      await publish(newOwnerPort)
      await new Promise(r => setTimeout(r, publishInterval))
    }
    testData.messagesAfterFailover = published - testData.messagesBeforeFailover

    // Calculate results
    const failureDetectionTime = testData.failureDetectedAt 
      ? (testData.failureDetectedAt - failureStart) / 1000 
      : null
    const electionTime = testData.electionCompletedAt && testData.electionStartedAt
      ? testData.electionCompletedAt - testData.electionStartedAt
      : null
    const totalRecoveryTime = testData.firstSuccessAfterFailover && testData.failureDetectedAt
      ? (testData.firstSuccessAfterFailover - testData.failureDetectedAt) / 1000
      : null
    const messageLoss = testData.offsetGaps.length > 0
    const offsetContinuity = testData.offsetGaps.length === 0 
      ? 'Continuous - no gaps detected'
      : `${testData.offsetGaps.length} gap(s) detected`

    setResults({
      failureDetectionTime: failureDetectionTime?.toFixed(2),
      electionTime: electionTime,
      totalRecoveryTime: totalRecoveryTime?.toFixed(2),
      messageLoss: messageLoss ? 'Yes' : 'No',
      offsetContinuity,
      messagesBeforeFailover: testData.messagesBeforeFailover,
      messagesAfterFailover: testData.messagesAfterFailover,
      totalMessages: published,
      offsetGaps: testData.offsetGaps,
      oldOwner: topic.owner,
      newOwner: newTopicOwner,
    })

    setPhase('Test complete!')
    setRunning(false)
  }, [config, topics])

  const stopTest = () => {
    abortRef.current = true
  }

  const topicList = Object.keys(topics)

  const copyLatex = () => {
    if (!results) return
    const latex = `\\item Failure Detection Time: \\textit{${results.failureDetectionTime} sec}
\\item Election Time: \\textit{${results.electionTime} ms}
\\item Total Recovery Time (RTO): \\textit{${results.totalRecoveryTime} sec}
\\item Message Loss During Failover: \\textit{${results.messageLoss}}
\\item Offset Continuity After Failover: \\textit{${results.offsetContinuity}}`
    navigator.clipboard.writeText(latex)
    alert('LaTeX copied to clipboard!')
  }

  return (
    <div className="panel failover-test">
      <div className="panel-header">
        <h2 className="panel-title">Failover Test</h2>
      </div>

      <div className="test-description">
        Measures failure detection, election time, and recovery metrics.
        You'll need to manually stop the topic owner broker during the test.
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
            {topicList.map(t => (
              <option key={t} value={t}>{t} (Owner: P{topics[t]?.owner})</option>
            ))}
          </select>
        </div>

        <div className="config-row">
          <label>Publish rate (msg/s)</label>
          <input 
            type="number" 
            value={config.publishRate}
            onChange={e => setConfig({...config, publishRate: parseInt(e.target.value) || 1})}
            min="1" max="100"
            disabled={running}
          />
        </div>
      </div>

      <div className="test-actions">
        {!running ? (
          <button className="test-btn" onClick={runFailoverTest}>Run Failover Test</button>
        ) : (
          <button className="test-btn stop" onClick={stopTest}>Stop</button>
        )}
      </div>

      {running && (
        <div className="live-stats">
          <div className="phase">{phase}</div>
          <div className="live-row">
            <span>Published: <strong>{liveData.published}</strong></span>
            <span>Last Offset: <strong>{liveData.lastOffset}</strong></span>
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
              <span className="result-label">Message Loss</span>
              <span className={`result-value ${results.messageLoss === 'No' ? 'success' : 'error'}`}>
                {results.messageLoss}
              </span>
            </div>
          </div>

          <div className="result-detail">
            <span className="detail-label">Offset Continuity:</span>
            <span className="detail-value">{results.offsetContinuity}</span>
          </div>

          <div className="result-detail">
            <span className="detail-label">Failover:</span>
            <span className="detail-value">P{results.oldOwner} → P{results.newOwner}</span>
          </div>

          <div className="result-detail">
            <span className="detail-label">Messages:</span>
            <span className="detail-value">
              {results.messagesBeforeFailover} before, {results.messagesAfterFailover} after
            </span>
          </div>
        </div>
      )}
    </div>
  )
}

export default FailoverTest
