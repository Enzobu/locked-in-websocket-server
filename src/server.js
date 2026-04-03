import process from 'node:process';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';

const config = {
  host: process.env.WS_HOST || '0.0.0.0',
  port: Number(process.env.WS_PORT || 30174),
  redisUrl: process.env.REDIS_URL || 'redis://redis:6379',
  outboxPrefix: process.env.OUTBOX_PREFIX || 'locker_ws:outbox:',
  responsePrefix: process.env.RESPONSE_PREFIX || 'locker_ws:response:',
  responseTtlSeconds: Number(process.env.RESPONSE_TTL_SECONDS || 15),
  flushIntervalMs: Number(process.env.FLUSH_INTERVAL_MS || 100),
  flushBatchSize: Number(process.env.FLUSH_BATCH_SIZE || 20)
};

const redis = new Redis(config.redisUrl);
const socketsByDeviceId = new Map();
const deviceIdBySocket = new Map();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function sendJson(ws, payload) {
  if (ws.readyState !== ws.OPEN) {
    return false;
  }

  const serialized = JSON.stringify(payload);
  ws.send(serialized);
  return true;
}

function closeExistingDeviceSocket(deviceId, nextSocket) {
  const existingSocket = socketsByDeviceId.get(deviceId);

  if (!existingSocket || existingSocket === nextSocket) {
    return;
  }

  log('Closing previous socket for device', deviceId);
  existingSocket.close(1000, 'replaced');
}

function attachDevice(deviceId, ws) {
  const previousDeviceId = deviceIdBySocket.get(ws);
  if (previousDeviceId && previousDeviceId !== deviceId) {
    socketsByDeviceId.delete(previousDeviceId);
  }

  closeExistingDeviceSocket(deviceId, ws);
  socketsByDeviceId.set(deviceId, ws);
  deviceIdBySocket.set(ws, deviceId);

  log('Registered device', deviceId);
}

function detachDevice(ws) {
  const deviceId = deviceIdBySocket.get(ws);
  if (!deviceId) {
    return;
  }

  if (socketsByDeviceId.get(deviceId) === ws) {
    socketsByDeviceId.delete(deviceId);
  }

  deviceIdBySocket.delete(ws);
  log('Disconnected device', deviceId);
}

async function storeResponse(payload, ws) {
  const correlationId = typeof payload.correlationId === 'string' ? payload.correlationId.trim() : '';

  if (!correlationId) {
    sendJson(ws, {
      type: 'error',
      status: 'failed',
      error: 'missing_correlation_id'
    });
    return;
  }

  const deviceId = deviceIdBySocket.get(ws);
  if (deviceId && !payload.deviceId) {
    payload.deviceId = deviceId;
  }

  const key = `${config.responsePrefix}${correlationId}`;
  await redis.setex(key, config.responseTtlSeconds, JSON.stringify(payload));
  log('Stored response', correlationId, payload.status || 'unknown');
}

function handleIdentify(ws, payload) {
  const deviceId = typeof payload.deviceId === 'string' ? payload.deviceId.trim() : '';

  if (!deviceId) {
    sendJson(ws, {
      type: 'error',
      status: 'failed',
      error: 'missing_device_id'
    });
    return;
  }

  attachDevice(deviceId, ws);

  sendJson(ws, {
    type: 'registered',
    deviceId,
    status: 'success'
  });
}

async function handleMessage(ws, rawMessage) {
  const payload = safeJsonParse(rawMessage.toString());

  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    sendJson(ws, {
      type: 'error',
      status: 'failed',
      error: 'invalid_json'
    });
    return;
  }

  const type = typeof payload.type === 'string' ? payload.type.trim() : '';
  const action = typeof payload.action === 'string' ? payload.action.trim() : '';

  if (type === 'identify' || type === 'register' || type === 'hello') {
    handleIdentify(ws, payload);
    return;
  }

  if (type === 'command_result') {
    await storeResponse(payload, ws);
    return;
  }

  if (type === 'ping' || action === 'ping') {
    sendJson(ws, { type: 'pong' });
    return;
  }

  if (type === 'pong') {
    return;
  }

  sendJson(ws, {
    type: 'error',
    status: 'failed',
    error: 'unsupported_message_type'
  });
}

async function flushPendingCommands() {
  const entries = [...socketsByDeviceId.entries()];

  for (const [deviceId, ws] of entries) {
    if (ws.readyState !== ws.OPEN) {
      detachDevice(ws);
      continue;
    }

    const outboxKey = `${config.outboxPrefix}${deviceId}`;

    for (let i = 0; i < config.flushBatchSize; i += 1) {
      const rawCommand = await redis.lpop(outboxKey);

      if (!rawCommand) {
        break;
      }

      const payload = safeJsonParse(rawCommand);
      if (!payload) {
        log('Skipping invalid outbox payload for', deviceId, rawCommand);
        continue;
      }

      const sent = sendJson(ws, payload);
      log('Sent command to', deviceId, payload);

      if (!sent) {
        await redis.lpush(outboxKey, rawCommand);
        detachDevice(ws);
        break;
      }
    }
  }
}

const wss = new WebSocketServer({
  host: config.host,
  port: config.port,
  path: '/'
});

wss.on('connection', (ws, request) => {
  const ip = request.socket.remoteAddress;
  log('Connected', ip);

  sendJson(ws, {
    type: 'welcome',
    message: 'connected'
  });

  ws.on('message', async (message) => {
    log('Message', message.toString());

    try {
      await handleMessage(ws, message);
    } catch (error) {
      log('Message handler error', error);

      sendJson(ws, {
        type: 'error',
        status: 'failed',
        error: 'internal_error'
      });
    }
  });

  ws.on('close', () => {
    detachDevice(ws);
  });

  ws.on('error', (error) => {
    log('Socket error', error.message);
    detachDevice(ws);
  });
});

wss.on('listening', () => {
  log(`WS listening on ws://${config.host}:${config.port}`);
});

wss.on('error', (error) => {
  log('Server error', error);
  process.exitCode = 1;
});

const flushTimer = setInterval(() => {
  flushPendingCommands().catch((error) => {
    log('Flush error', error);
  });
}, config.flushIntervalMs);

async function shutdown(signal) {
  log('Received signal', signal);
  clearInterval(flushTimer);

  for (const ws of socketsByDeviceId.values()) {
    try {
      ws.close(1001, 'shutdown');
    } catch {
      // ignore
    }
  }

  try {
    wss.close();
  } catch {
    // ignore
  }

  try {
    await redis.quit();
  } catch {
    redis.disconnect();
  }

  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT').catch(() => process.exit(1));
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM').catch(() => process.exit(1));
});
