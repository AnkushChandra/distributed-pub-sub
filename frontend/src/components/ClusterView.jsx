import React from 'react'
import './ClusterView.css'

function ClusterView({ nodes, leader, topics }) {
  const nodeList = Object.values(nodes).sort((a, b) => a.id - b.id)
  
  const getNodeTopics = (nodeId) => {
    const owned = []
    const following = []
    for (const [name, topic] of Object.entries(topics)) {
      if (topic.owner === nodeId) owned.push(name)
      else if (topic.followers.includes(nodeId)) following.push(name)
    }
    return { owned, following }
  }

  return (
    <div className="panel cluster-view">
      <div className="panel-header">
        <h2 className="panel-title">Cluster Nodes</h2>
        <span className="panel-badge">{nodeList.filter(n => n.alive).length}/{nodeList.length} online</span>
      </div>
      
      <div className="cluster-grid">
        {nodeList.map(node => {
          const { owned, following } = getNodeTopics(node.id)
          return (
            <div 
              key={node.id} 
              className={`node-card ${node.alive ? 'alive' : 'dead'} ${node.isLeader ? 'leader' : ''}`}
            >
              <div className="node-header">
                <div className="node-id">
                  <span className={`status-dot ${node.alive ? 'alive' : 'dead'}`}></span>
                  <span className="mono">P{node.id}</span>
                </div>
                {node.isLeader && <span className="leader-badge">LEADER</span>}
              </div>
              
              <div className="node-port mono">:{node.port}</div>
              
              <div className="node-stats">
                <div className="stat">
                  <span className="stat-label">Owns</span>
                  <span className="stat-value">{owned.length}</span>
                </div>
                <div className="stat">
                  <span className="stat-label">Follows</span>
                  <span className="stat-value">{following.length}</span>
                </div>
              </div>

              {!node.alive && (
                <div className="node-offline">OFFLINE</div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default ClusterView
