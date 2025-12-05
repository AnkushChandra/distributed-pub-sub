import React, { useState, useEffect } from 'react'
import './TopicsPanel.css'

function TopicsPanel({ topics, nodes, selectedBroker }) {
  const [topicOffsets, setTopicOffsets] = useState({})
  const [expandedTopic, setExpandedTopic] = useState(null)

  useEffect(() => {
    const fetchOffsets = async () => {
      const offsets = {}
      
      for (const [topicName, topic] of Object.entries(topics)) {
        offsets[topicName] = {}
        const allReplicas = [topic.owner, ...topic.followers]
        
        await Promise.all(
          allReplicas.map(async (brokerId) => {
            const port = 8080 + brokerId
            try {
              const res = await fetch(`http://localhost:${port}/internal/topics/${topicName}/status`, {
                signal: AbortSignal.timeout(1500)
              })
              if (res.ok) {
                const data = await res.json()
                offsets[topicName][brokerId] = data.latest_offset
              } else {
                offsets[topicName][brokerId] = null
              }
            } catch {
              offsets[topicName][brokerId] = null
            }
          })
        )
      }
      
      setTopicOffsets(offsets)
    }

    if (Object.keys(topics).length > 0) {
      fetchOffsets()
      const interval = setInterval(fetchOffsets, 3000)
      return () => clearInterval(interval)
    }
  }, [topics])

  const topicList = Object.values(topics)

  const getReplicaStatus = (topicName, brokerId) => {
    const topic = topics[topicName]
    const node = nodes[brokerId]
    const offset = topicOffsets[topicName]?.[brokerId]
    const ownerOffset = topicOffsets[topicName]?.[topic.owner]
    
    const isAlive = node?.alive && topic.alive?.[brokerId] !== false
    const inIsr = topic.isr?.includes(brokerId)
    const isOwner = topic.owner === brokerId
    const lag = ownerOffset !== null && offset !== null ? ownerOffset - offset : null

    return { isAlive, inIsr, isOwner, offset, lag }
  }

  return (
    <div className="panel topics-panel">
      <div className="panel-header">
        <h2 className="panel-title">Topics</h2>
        <span className="panel-badge">{topicList.length} topics</span>
      </div>

      {topicList.length === 0 ? (
        <div className="empty-state">
          <p>No topics created yet</p>
        </div>
      ) : (
        <div className="topics-list">
          {topicList.map(topic => {
            const allReplicas = [topic.owner, ...topic.followers]
            const isExpanded = expandedTopic === topic.name
            const hasDeadReplica = allReplicas.some(id => {
              const status = getReplicaStatus(topic.name, id)
              return !status.isAlive
            })

            return (
              <div 
                key={topic.name} 
                className={`topic-card ${hasDeadReplica ? 'has-dead' : ''} ${isExpanded ? 'expanded' : ''}`}
              >
                <div 
                  className="topic-header"
                  onClick={() => setExpandedTopic(isExpanded ? null : topic.name)}
                >
                  <div className="topic-name">
                    <span className="expand-icon">{isExpanded ? '−' : '+'}</span>
                    <span className="mono">{topic.name}</span>
                    {hasDeadReplica && <span className="warning-dot"></span>}
                  </div>
                  <div className="topic-meta">
                    <span className="replica-count">{allReplicas.length} replicas</span>
                    <span className="isr-count">ISR: {topic.isr?.length || 0}</span>
                  </div>
                </div>

                <div className="replicas-grid">
                  {allReplicas.map(brokerId => {
                    const status = getReplicaStatus(topic.name, brokerId)
                    return (
                      <div 
                        key={brokerId}
                        className={`replica-card ${status.isOwner ? 'owner' : 'follower'} ${status.isAlive ? 'alive' : 'dead'} ${status.inIsr ? 'in-isr' : ''}`}
                      >
                        <div className="replica-header">
                          <span className={`status-indicator ${status.isAlive ? 'alive' : 'dead'}`}></span>
                          <span className="replica-id mono">P{brokerId}</span>
                          {status.isOwner && <span className="owner-tag">OWNER</span>}
                        </div>
                        <div className="replica-offset">
                          <span className="offset-label">Offset</span>
                          <span className="offset-value mono">
                            {status.offset !== null ? status.offset : '—'}
                          </span>
                        </div>
                        {!status.isOwner && status.lag !== null && status.lag > 0 && (
                          <div className="replica-lag">
                            <span className="lag-label">Lag</span>
                            <span className="lag-value mono">-{status.lag}</span>
                          </div>
                        )}
                        <div className="replica-badges">
                          {status.inIsr && <span className="isr-badge">ISR</span>}
                          {!status.isAlive && <span className="dead-badge">OFFLINE</span>}
                        </div>
                      </div>
                    )
                  })}
                </div>

                {isExpanded && (
                  <div className="topic-details">
                    <div className="detail-row">
                      <span className="detail-label">Min Sync Followers</span>
                      <span className="detail-value mono">{topic.minSyncFollowers}</span>
                    </div>
                    {topic.lastPublish && (
                      <div className="detail-row">
                        <span className="detail-label">Last Publish</span>
                        <span className="detail-value mono">
                          {new Date(topic.lastPublish.timestamp * 1000).toLocaleTimeString()}
                        </span>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default TopicsPanel
