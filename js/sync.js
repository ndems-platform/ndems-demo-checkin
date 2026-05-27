// =====================================================
//  sync.js — MQTT Realtime sync (cross-device)
//  ✅ No backend required, perfect for GitHub Pages
// =====================================================

const SyncManager = (() => {
  let _client = null;
  const BROKER_URL = 'wss://broker.emqx.io:8084/mqtt';
  const TOPIC_PREFIX = 'ndems-ghpages-2026/';
  
  let _checkins = {};
  let _onUpdateCb = null;
  let _onIncidentCb = null;
  let _incidentCache = undefined; // undefined=not yet received, null=explicitly empty

  async function _ensureClient() {
    if (_client) return _client;
    
    return new Promise((resolve) => {
      const connectMQTT = () => {
        _client = mqtt.connect(BROKER_URL, { keepalive: 60 });
        _client.on('connect', () => {
          _client.subscribe(TOPIC_PREFIX + 'checkins/+');
          _client.subscribe(TOPIC_PREFIX + 'incident');
          resolve(_client);
        });
        
        _client.on('message', (topic, message) => {
          const payload = message.toString();
          if (topic.startsWith(TOPIC_PREFIX + 'checkins/')) {
            const id = topic.split('/').pop();
            if (!payload || payload === "") {
              delete _checkins[id];
            } else {
              try {
                _checkins[id] = JSON.parse(payload);
              } catch(e) {}
            }
            if (_onUpdateCb) _onUpdateCb({ ..._checkins });
          } else if (topic === TOPIC_PREFIX + 'incident') {
            try {
              const inc = payload && payload !== "" ? JSON.parse(payload) : null;
              _incidentCache = inc; // Always cache locally
              if (_onIncidentCb) _onIncidentCb(inc);
            } catch(e) {}
          }
        });
      };

      if (typeof mqtt === 'undefined') {
        const script = document.createElement('script');
        script.src = 'https://unpkg.com/mqtt/dist/mqtt.min.js';
        script.onload = connectMQTT;
        document.head.appendChild(script);
      } else {
        connectMQTT();
      }
    });
  }

  async function init(onUpdateCb) {
    _onUpdateCb = onUpdateCb;
    await _ensureClient();
    console.log('[Sync] ✅ MQTT Realtime connected');
    // Broadcast initial state
    if (_onUpdateCb) _onUpdateCb({ ..._checkins });
  }

  async function save(checkinObj) {
    const client = await _ensureClient();
    client.publish(TOPIC_PREFIX + 'checkins/' + checkinObj.id, JSON.stringify(checkinObj), { retain: true });
  }

  async function remove(id) {
    const client = await _ensureClient();
    client.publish(TOPIC_PREFIX + 'checkins/' + String(id), "", { retain: true });
  }

  async function saveIncident(incObj) {
    const client = await _ensureClient();
    _incidentCache = incObj; // Update local cache immediately
    client.publish(TOPIC_PREFIX + 'incident', JSON.stringify(incObj), { retain: true });
  }

  async function getIncident() {
    await _ensureClient();
    // If we already have cached data from a previous message, return it
    if (_incidentCache !== undefined) {
      return _incidentCache;
    }
    // Otherwise wait for retained message to arrive
    return new Promise((resolve) => {
      let resolved = false;
      const prevCb = _onIncidentCb;
      const cb = (inc) => {
        if (!resolved) {
          resolved = true;
          resolve(inc);
        }
        // Restore previous callback (e.g. watchIncident)
        if (prevCb) {
          _onIncidentCb = prevCb;
          prevCb(inc);
        }
      };
      _onIncidentCb = cb;
      setTimeout(() => {
        if (!resolved) {
          resolved = true;
          _onIncidentCb = prevCb || null;
          resolve(_incidentCache !== undefined ? _incidentCache : null);
        }
      }, 3000);
    });
  }

  async function watchIncident(cb) {
    _onIncidentCb = cb;
    await _ensureClient();
  }

  return { init, save, remove, saveIncident, getIncident, watchIncident };
})();
