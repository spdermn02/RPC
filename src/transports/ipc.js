'use strict';

const net = require('net');
const EventEmitter = require('events');
const { uuid } = require('../util');

const OPCodes = {
  HANDSHAKE: 0,
  FRAME: 1,
  CLOSE: 2,
  PING: 3,
  PONG: 4,
};

function getIPCPath(id) {
  if (process.platform === 'win32') {
    return `\\\\?\\pipe\\discord-ipc-${id}`;
  }
  const { env: { XDG_RUNTIME_DIR, TMPDIR, TMP, TEMP } } = process;
  const prefix = XDG_RUNTIME_DIR || TMPDIR || TMP || TEMP || '/tmp';
  return `${prefix.replace(/\/$/, '')}/discord-ipc-${id}`;
}

function getIPC(id = 0) {
  return new Promise((resolve, reject) => {
    const path = getIPCPath(id);
    const onerror = () => {
      if (id < 10) {
        resolve(getIPC(id + 1));
      } else {
        reject(new Error('Could not connect'));
      }
    };
    const sock = net.createConnection(path, () => {
      sock.removeListener('error', onerror);
      resolve(sock);
    });
    sock.once('error', onerror);
  });
}

function encode(op, data) {
  data = JSON.stringify(data);
  const len = Buffer.byteLength(data);
  const packet = Buffer.alloc(8 + len);
  packet.writeInt32LE(op, 0);
  packet.writeInt32LE(len, 4);
  packet.write(data, 8, len);
  return packet;
}

// Frames from Discord are only trusted to be well formed, not to be small. Anything larger
// than this is treated as a corrupt stream and the connection is dropped.
const MAX_FRAME_BYTES = 16 * 1024 * 1024;

/**
 * Pull every complete frame out of `state.buffer`. Partial frames stay buffered until the rest
 * arrives, and several frames in one read are all delivered. State lives on the transport, not
 * the module, so two clients can never splice each other's frames together.
 * Returns false when the stream is unrecoverable.
 */
function decode(state, chunk, callback) {
  state.buffer = state.buffer.length ? Buffer.concat([state.buffer, chunk]) : chunk;
  while (state.buffer.length >= 8) {
    const op = state.buffer.readInt32LE(0);
    const len = state.buffer.readInt32LE(4);
    if (len < 0 || len > MAX_FRAME_BYTES) {
      state.buffer = Buffer.alloc(0);
      return false;
    }
    if (state.buffer.length < 8 + len) {
      break;
    }
    const raw = state.buffer.subarray(8, 8 + len).toString();
    state.buffer = state.buffer.subarray(8 + len);
    let data;
    try {
      data = JSON.parse(raw);
    } catch (err) {
      continue;
    }
    callback({ op, data }); // eslint-disable-line callback-return
  }
  return true;
}

class IPCTransport extends EventEmitter {
  constructor(client) {
    super();
    this.client = client;
    this.socket = null;
    this.decodeState = { buffer: Buffer.alloc(0) };
  }

  async connect() {
    const socket = this.socket = await getIPC();
    socket.on('close', this.onClose.bind(this));
    socket.on('error', this.onClose.bind(this));
    this.emit('open');
    socket.write(encode(OPCodes.HANDSHAKE, {
      v: 1,
      client_id: this.client.clientId,
    }));
    socket.pause();
    socket.on('readable', () => {
      let chunk;
      while ((chunk = socket.read()) !== null) {
        const ok = decode(this.decodeState, chunk, ({ op, data }) => {
          switch (op) {
            case OPCodes.PING:
              this.send(data, OPCodes.PONG);
              break;
            case OPCodes.FRAME:
              if (!data) {
                return;
              }
              this.emit('message', data);
              break;
            case OPCodes.CLOSE:
              this.emit('close', data);
              break;
            default:
              break;
          }
        });
        if (!ok) {
          socket.destroy();
          return;
        }
      }
    });
  }

  onClose(e) {
    this.emit('close', e);
  }

  send(data, op = OPCodes.FRAME) {
    if (!this.socket || this.socket.destroyed || !this.socket.writable) {
      return false;
    }
    this.socket.write(encode(op, data));
    return true;
  }

  async close() {
    // Never connected, or already gone: there is nothing to close, and waiting for a
    // 'close' event that will never come would leave the caller hanging forever.
    if (!this.socket || this.socket.destroyed) {
      return undefined;
    }
    return new Promise((r) => {
      this.once('close', r);
      this.send({}, OPCodes.CLOSE);
      this.socket.end();
    });
  }

  ping() {
    this.send(uuid(), OPCodes.PING);
  }
}

module.exports = IPCTransport;
module.exports.encode = encode;
module.exports.decode = decode;
