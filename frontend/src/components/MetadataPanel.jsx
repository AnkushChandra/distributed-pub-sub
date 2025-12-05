import React from 'react'
import './MetadataPanel.css'

function MetadataPanel({ metadata, isr, leader }) {
  return (
    <div className="panel metadata-panel">
      <div className="panel-header">
        <h2 className="panel-title">Metadata</h2>
        <span className="panel-badge">v{metadata?.version || 0}</span>
      </div>

      <div className="metadata-content">
        <div className="meta-section">
          <h3 className="meta-section-title">Cluster</h3>
          <div className="meta-grid">
            <div className="meta-item">
              <span className="meta-label">Leader</span>
              <span className="meta-value highlight">P{leader || '?'}</span>
            </div>
            <div className="meta-item">
              <span className="meta-label">Version</span>
              <span className="meta-value mono">{metadata?.version || 0}</span>
            </div>
            <div className="meta-item">
              <span className="meta-label">Topics</span>
              <span className="meta-value mono">{Object.keys(metadata?.topics || {}).length}</span>
            </div>
          </div>
        </div>

        <div className="meta-section">
          <h3 className="meta-section-title">Placements</h3>
          <div className="placements-list">
            {Object.entries(metadata?.topics || {}).map(([topic, brokers]) => (
              <div key={topic} className="placement-item">
                <span className="placement-topic mono">{topic}</span>
                <div className="placement-brokers">
                  {brokers.map((bid, i) => (
                    <span key={bid} className={`placement-broker ${i === 0 ? 'owner' : 'follower'}`}>
                      P{bid}
                    </span>
                  ))}
                </div>
              </div>
            ))}
            {Object.keys(metadata?.topics || {}).length === 0 && (
              <div className="empty-placements">No topics</div>
            )}
          </div>
        </div>

        <div className="meta-section">
          <h3 className="meta-section-title">ISR Status</h3>
          <div className="isr-list">
            {Object.entries(isr || {}).map(([topic, info]) => (
              <div key={topic} className="isr-item">
                <span className="isr-topic mono">{topic}</span>
                <div className="isr-members">
                  {(info.isr || []).map(bid => (
                    <span key={bid} className={`isr-member ${info.alive?.[bid] ? 'alive' : 'dead'}`}>
                      P{bid}
                    </span>
                  ))}
                </div>
              </div>
            ))}
            {Object.keys(isr || {}).length === 0 && (
              <div className="empty-isr">No ISR data</div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default MetadataPanel
