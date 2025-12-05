import React, { useState, useEffect, useCallback } from 'react'
import ClusterView from './components/ClusterView'
import TopicsPanel from './components/TopicsPanel'
import AlgorithmsPanel from './components/AlgorithmsPanel'
import MetadataPanel from './components/MetadataPanel'
import EventLog from './components/EventLog'
import StressTest from './components/StressTest'
import './App.css'

const BROKER_PORTS = [8081, 8082, 8083, 8084]

function App() {
  const [clusterData, setClusterData] = useState({
    nodes: {},
    topics: {},
    metadata: null,
    isr: {},
    leader: null,
    version: 0
  })
  const [events, setEvents] = useState([])
  const [selectedBroker, setSelectedBroker] = useState(8001)

  const addEvent = useCallback((type, message, data = null) => {
    setEvents(prev => [{
      id: Date.now(),
      time: new Date().toLocaleTimeString(),
      type,
      message,
      data
    }, ...prev].slice(0, 50))
  }, [])

  const fetchFromBroker = useCallback(async (port, endpoint) => {
    try {
      const res = await fetch(`http://localhost:${port}${endpoint}`, { 
        timeout: 2000,
        signal: AbortSignal.timeout(2000)
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } catch (e) {
      return null
    }
  }, [])

  const refreshCluster = useCallback(async () => {
    const nodes = {}
    let metadata = null
    let isr = {}
    let leader = null
    let version = 0

    // Fetch from all brokers in parallel
    const results = await Promise.all(
      BROKER_PORTS.map(async (port) => {
        const brokerId = port - 8080
        const [leaderInfo, metaInfo, isrInfo, metricsInfo] = await Promise.all([
          fetchFromBroker(port, '/admin/leader'),
          fetchFromBroker(port, '/metadata'),
          fetchFromBroker(port, '/admin/isr'),
          fetchFromBroker(port, '/metrics')
        ])
        return { port, brokerId, leaderInfo, metaInfo, isrInfo, metricsInfo }
      })
    )

    for (const { port, brokerId, leaderInfo, metaInfo, isrInfo, metricsInfo } of results) {
      const isAlive = leaderInfo !== null
      const isLeader = leaderInfo?.this_broker_is_leader || false
      
      nodes[brokerId] = {
        id: brokerId,
        port,
        alive: isAlive,
        isLeader,
        leaderId: leaderInfo?.leader_id,
        ownedTopics: metricsInfo?.owned_topics || [],
        leases: metricsInfo?.leases || {}
      }

      if (isLeader && metaInfo) {
        metadata = metaInfo
        leader = brokerId
        version = metaInfo.version || 0
      }
      if (isLeader && isrInfo) {
        isr = isrInfo.topics || {}
      }
    }

    // Build topics from metadata
    const topics = {}
    if (metadata?.topics) {
      for (const [topicName, brokers] of Object.entries(metadata.topics)) {
        const owner = brokers[0]
        const followers = brokers.slice(1)
        const topicIsr = isr[topicName] || {}
        
        topics[topicName] = {
          name: topicName,
          owner,
          followers,
          isr: topicIsr.isr || [],
          alive: topicIsr.alive || {},
          minSyncFollowers: topicIsr.min_sync_followers || 1,
          lastPublish: topicIsr.last_publish
        }
      }
    }

    setClusterData(prev => {
      // Detect changes for event log
      if (prev.leader !== leader && leader !== null) {
        addEvent('election', `Leader changed to Broker ${leader}`, { oldLeader: prev.leader, newLeader: leader })
      }
      
      for (const [id, node] of Object.entries(nodes)) {
        const prevNode = prev.nodes[id]
        if (prevNode && prevNode.alive !== node.alive) {
          addEvent(node.alive ? 'node-up' : 'node-down', 
            `Broker ${id} ${node.alive ? 'came online' : 'went offline'}`)
        }
      }

      if (version > prev.version && prev.version > 0) {
        addEvent('metadata', `Metadata updated to version ${version}`)
      }

      return { nodes, topics, metadata, isr, leader, version }
    })
  }, [fetchFromBroker, addEvent])

  useEffect(() => {
    refreshCluster()
    const interval = setInterval(refreshCluster, 2000)
    return () => clearInterval(interval)
  }, [refreshCluster])

  return (
    <div className="app">
      <header className="header">
        <div className="header-left">
          <h1>Distributed Pub/Sub</h1>
          <span className="version mono">v{clusterData.version}</span>
        </div>
        <div className="header-right">
          <select 
            value={selectedBroker} 
            onChange={(e) => setSelectedBroker(Number(e.target.value))}
            className="broker-select"
          >
            {BROKER_PORTS.map(port => (
              <option key={port} value={port}>
                Broker {port - 8080} (:{port})
              </option>
            ))}
          </select>
          <button onClick={refreshCluster} className="refresh-btn">↻ Refresh</button>
        </div>
      </header>

      <main className="main">
        <div className="left-panel">
          <ClusterView 
            nodes={clusterData.nodes} 
            leader={clusterData.leader}
            topics={clusterData.topics}
          />
          <AlgorithmsPanel 
            nodes={clusterData.nodes}
            leader={clusterData.leader}
            topics={clusterData.topics}
          />
        </div>
        
        <div className="center-panel">
          <TopicsPanel 
            topics={clusterData.topics}
            nodes={clusterData.nodes}
            selectedBroker={selectedBroker}
          />
        </div>

        <div className="right-panel">
          <StressTest 
            topics={clusterData.topics}
            nodes={clusterData.nodes}
            leader={clusterData.leader}
          />
          <MetadataPanel 
            metadata={clusterData.metadata}
            isr={clusterData.isr}
            leader={clusterData.leader}
          />
          <EventLog events={events} />
        </div>
      </main>
    </div>
  )
}

export default App
