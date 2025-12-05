import React from 'react'
import './EventLog.css'

function EventLog({ events }) {
  const getEventClass = (type) => {
    switch (type) {
      case 'election': return 'election'
      case 'node-up': return 'success'
      case 'node-down': return 'error'
      case 'metadata': return 'info'
      default: return 'default'
    }
  }

  return (
    <div className="panel event-log">
      <div className="panel-header">
        <h2 className="panel-title">Event Log</h2>
        <span className="panel-badge">{events.length}</span>
      </div>

      <div className="events-list">
        {events.length === 0 ? (
          <div className="empty-events">
            <p>Watching for events...</p>
          </div>
        ) : (
          events.map(event => (
            <div key={event.id} className={`event-item ${getEventClass(event.type)}`}>
              <div className="event-content">
                <div className="event-message">{event.message}</div>
                <div className="event-time mono">{event.time}</div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}

export default EventLog
