import net from 'node:net'
import tls from 'node:tls'
import http from 'node:http'
import https from 'node:https'
import { pipeline } from 'node:stream'
import { PASSTHROUGH_BASE_URL } from './proxy.mjs'
import { loadOrCreateCa, issueLeaf } from './ca.mjs'
import { connectVia, proxyFromEnv } from './upstream.mjs'

/**
 * HTTPS proxy 模式：讓 proxy 埠也接受 `CONNECT`，Claude Code 設 `HTTPS_PROXY` 就能接進來。
 *
 * 為什麼需要：Claude Desktop 的 Code 分頁自己決定 API 位址，不理 settings.json 裡的
 * `ANTHROPIC_BASE_URL`，卻會照讀 `HTTPS_PROXY` 與 `NODE_EXTRA_CA_CERTS`（見 docs/https-proxy.md）。
 *
 *   CONNECT api.anthropic.com:443   用本機 CA 簽的證書解開 TLS，再按路徑分：
 *     /v1/messages*                 交給 proxy 原本的 request handler —— 分流、改寫、流量記錄同一套
 *     其他路徑、WebSocket upgrade    原樣轉發給 Anthropic，不記錄
 *   CONNECT 其他任何目的地           純 TCP 隧道，不解密（git、npm、WebFetch、OAuth 登入都走這裡）
 */

export const INTERCEPT_HOST = 'api.anthropic.com'
const INTERCEPT_PORT = 443

/**
 * 只有這個前綴交給 proxy 的 handler。`ANTHROPIC_BASE_URL` 模式下 router 只收得到這兩種
 * （`/v1/messages` 與 `/v1/messages/count_tokens`，實測 4666 筆裡沒有別的），這裡刻意對齊：
 * MITM 看得到的 OAuth、feature flag、每幾秒一批的遙測如果也記下來，流量記錄與進條的統計就被淹掉。
 */
const ROUTED_PREFIX = '/v1/messages'

const ESTABLISHED = 'HTTP/1.1 200 Connection Established\r\n\r\n'

/** 原樣轉發時不能照抄的 header：逐跳的、以及 Node 會依實際傳輸方式自己補上的 */
const RELAY_DROP = new Set([
  'host', 'connection', 'keep-alive', 'proxy-connection', 'proxy-authorization', 'te', 'trailer', 'upgrade',
  'transfer-encoding',
])

function relayHeaders(headers) {
  const out = {}
  for (const [name, value] of Object.entries(headers)) if (!RELAY_DROP.has(name)) out[name] = value
  return out
}

/** `host:port` 或 `[v6]:port`。解不出來、或沒帶埠（CONNECT 規定要帶）就是 null */
function parseAuthority(authority) {
  const match = /^(\[[^\]]+\]|[^:[\]]+):(\d{1,5})$/.exec(authority ?? '')
  if (!match) return null
  const port = Number(match[2])
  if (port < 1 || port > 65535) return null
  return { host: match[1].replace(/^\[|\]$/g, '').toLowerCase(), port }
}

const portOf = (url) => Number(url.port) || (url.protocol === 'https:' ? 443 : 80)

function tunnel(socket, head, host, port, egress) {
  let established = false
  // onReady 在「到目標的通道真的通了」之後才被呼叫；經代理時那是收到 200 之後
  const upstream = connectVia(port, host, egress, (err, upstreamSocket) => {
    if (err) {
      // 還沒回 200 就還能告訴 client 是上游連不上
      socket.end('HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n')
      return
    }
    established = true
    socket.write(ESTABLISHED)
    if (head.length) upstreamSocket.write(head)
    upstreamSocket.pipe(socket)
    socket.pipe(upstreamSocket)
    upstreamSocket.on('close', () => socket.destroy())
    socket.on('close', () => upstreamSocket.destroy())
  })
  // 連線在 onReady 之前就失效時，socket 的 error 也只在這段期間有意義
  socket.on('close', () => {
    if (!established) upstream.destroy()
  })
}

/** 不緩衝、不改 body，逐塊雙向轉發 */
function relay(req, res, upstream) {
  const client = upstream.protocol === 'https:' ? https : http
  const out = client.request({
    protocol: upstream.protocol,
    hostname: upstream.hostname,
    port: portOf(upstream),
    path: req.url,
    method: req.method,
    headers: { ...relayHeaders(req.headers), host: upstream.host },
  }, (answer) => {
    res.writeHead(answer.statusCode, relayHeaders(answer.headers))
    pipeline(answer, res, () => {})
  })
  out.on('error', () => {
    if (!res.headersSent) res.writeHead(502, { 'content-length': 0 }).end()
    else res.destroy()
  })
  res.on('close', () => {
    if (!res.writableEnded) out.destroy()
  })
  pipeline(req, out, () => {})
}

/**
 * WebSocket 之類的 upgrade：把請求頭原樣重寫到上游，之後兩邊直接對接。
 * Claude Code 的 voice mode 會對 `/api/ws/speech_to_text/voice_stream` 開 WebSocket（v2.1.281 執行檔內可見），
 * 沒接住的話開了 proxy 模式 voice mode 就壞。
 */
function relayUpgrade(req, socket, head, upstream, egress) {
  socket.on('error', () => {})
  const secure = upstream.protocol === 'https:'
  // 先建 TCP 通道（可能經代理）。onReady 被呼叫時通道已經通了，
  // 所以下面不需要再等一次 `connect` —— 那個事件在回報前就發完了。
  connectVia(portOf(upstream), upstream.hostname, egress, (err, raw) => {
    if (err) {
      socket.destroy()
      return
    }
    const conn = secure ? tls.connect({ socket: raw, servername: upstream.hostname }) : raw
    const handshake = () => {
      const lines = [`${req.method} ${req.url} HTTP/1.1`]
      for (let i = 0; i < req.rawHeaders.length; i += 2) {
        const name = req.rawHeaders[i]
        lines.push(`${name}: ${name.toLowerCase() === 'host' ? upstream.host : req.rawHeaders[i + 1]}`)
      }
      conn.write(`${lines.join('\r\n')}\r\n\r\n`)
      if (head.length) conn.write(head)
      conn.pipe(socket)
      socket.pipe(conn)
    }
    // TLS 要等 secureConnect；明文通道在 onReady 時已經就緒，直接開始
    if (secure) conn.once('secureConnect', handshake)
    else handshake()
    conn.on('error', () => socket.destroy())
    conn.on('close', () => socket.destroy())
    socket.on('close', () => conn.destroy())
  })
}

/**
 * 在 proxy server 上掛 CONNECT 處理，回傳卸下它的方法。
 *
 * 解開後的 `/v1/messages*` 以 `proxy.emit('request', …)` 交回 proxy 的 handler，而不是另外包一份：
 * 這樣分流邏輯只有一份，兩種接入方式不會各自長歪。
 *
 * @param {import('node:http').Server} proxy createProxyServer 回傳的 server
 * @param {{ certPem: string, keyPem: string }} leaf INTERCEPT_HOST 的伺服器證書（src/ca.mjs 的 issueLeaf）
 * @param {object} [options]
 * @param {string} [options.upstreamBaseUrl] 測試用：把原樣轉發的那條線指到假上游
 * @param {(message: string) => void} [options.warn] 握手失敗之類使用者要知道的事
 * @returns {() => void} 卸下：移除 CONNECT 監聽，並斷開所有還開著的 CONNECT 連線
 */
export function attachConnect(proxy, leaf, { upstreamBaseUrl = PASSTHROUGH_BASE_URL, warn = (m) => console.error(m) } = {}) {
  const upstream = new URL(upstreamBaseUrl)
  const secureContext = tls.createSecureContext({ key: leaf.keyPem, cert: leaf.certPem })
  const open = new Set()
  let detached = false
  // 隧道與 WebSocket 的上游連線也走同一個代理：部署在只允許經代理出網的機器上時，
  // net.connect 的直連會被宿主防火牆 DROP，這些請求就全部失效
  const egress = proxyFromEnv()

  const decrypted = http.createServer((req, res) => {
    if (req.url.startsWith(ROUTED_PREFIX)) proxy.emit('request', req, res)
    else relay(req, res, upstream)
  })
  decrypted.on('upgrade', (req, socket, head) => relayUpgrade(req, socket, head, upstream, egress))
  // 解開的請求由這一台解析 HTTP，逾時看的是它的設定，理由跟 src/index.mjs 裡 proxy 那台一樣：
  // Claude Code 會等串流等很久，兩輪對話之間的閒置也遠不只 Node 預設的 5 秒
  decrypted.headersTimeout = 0
  decrypted.requestTimeout = 0
  decrypted.timeout = 0
  decrypted.keepAliveTimeout = 5 * 60_000

  const onConnect = (req, socket, head) => {
    // client 那頭隨時可能 reset；沒人接的 error 事件會直接砍掉整個 process
    socket.on('error', () => {})
    open.add(socket)
    socket.on('close', () => open.delete(socket))

    const target = parseAuthority(req.url)
    if (!target) {
      socket.end('HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n')
      return
    }

    if (target.host !== INTERCEPT_HOST || target.port !== INTERCEPT_PORT) {
      tunnel(socket, head, target.host, target.port, egress)
      return
    }

    // client 要等到 200 才開始 TLS 握手，head 照理是空的。不空的話沒辦法接：
    // 下面的 TLSSocket 直接讀 socket 底層的 handle，unshift 回 JS 緩衝區的資料它看不到
    if (head.length) {
      socket.destroy()
      return
    }
    socket.write(ESTABLISHED)
    // 自己包 TLSSocket，不把 socket 丟給 https.Server：從 http server 的 connect 事件拿到的 socket，
    // 交給 https.Server 之後握手失敗時什麼事件都不發（實測 Node 24），使用者就收不到下面這個提示
    const secure = new tls.TLSSocket(socket, { isServer: true, secureContext })
    let handshaken = false
    secure.on('secure', () => { handshaken = true })
    secure.on('error', () => {})
    secure.on('close', () => {
      // TLS 1.3 下 client 驗不過證書時只是關掉連線，server 端看不到錯誤，只看得到「還沒握完手就關了」。
      // 最常見的原因是 Claude Code 沒拿到 NODE_EXTRA_CA_CERTS，不講的話使用者只看到 Claude Code 連不上。
      // 卸下時是我們自己斷的線，不算
      if (!handshaken && !detached) {
        warn(`✗ a client closed the ${INTERCEPT_HOST} connection before the TLS handshake finished — if Claude Code cannot connect, check that NODE_EXTRA_CA_CERTS points at the router's CA`)
      }
    })
    decrypted.emit('connection', secure)
  }
  proxy.on('connect', onConnect)

  return () => {
    detached = true
    proxy.removeListener('connect', onConnect)
    // 關掉就是真的關掉：還開著的隧道與解開的連線一起斷，不留到它們自己結束
    for (const socket of open) socket.destroy()
    open.clear()
  }
}

/**
 * HTTPS proxy 模式的開關，執行中隨時切換，不用重啟。
 *
 * 關著的時候 router 跟沒有這個功能時一樣：沒有 CONNECT 監聽、不讀 CA；從沒打開過的話連 CA 檔案都
 * 不存在。打開時才讀取或產生 CA。guard 的例外看的是 proxy 的 getRuntime().httpsProxy，由 src/index.mjs
 * 接到這裡的 `enabled`。
 *
 * test/connect.test.mjs 守著的是這個函式本身（關閉時不掛監聽、不產生 CA；開、關、並行開、執行中關掉
 * 斷隧道）與 proxy 的 guard 在 runtime 回報開或關時的行為（測試用的 runtime 是寫死的）。src/index.mjs
 * 把兩者接起來的那段（getRuntime 讀 `enabled`、setConfig 先切換再存、存失敗切回去）沒有測試：
 * 那個檔案一 import 就綁埠啟動。
 *
 * @param {import('node:http').Server} proxy
 * @param {{ dir: string, warn?: (message: string) => void, upstreamBaseUrl?: string }} options
 *   `dir` 是 CA 落檔的目錄，跟 config.json 放在一起
 */
export function createHttpsProxy(proxy, { dir, warn, upstreamBaseUrl }) {
  let active = null
  let pending = Promise.resolve()

  async function toggle(enabled) {
    if (enabled && !active) {
      const ca = await loadOrCreateCa(dir, INTERCEPT_HOST)
      const detach = attachConnect(proxy, issueLeaf(ca, INTERCEPT_HOST), { warn, upstreamBaseUrl })
      active = { certPath: ca.certPath, detach }
      return { changed: true, created: ca.created }
    }
    if (!enabled && active) {
      active.detach()
      active = null
      return { changed: true }
    }
    return { changed: false }
  }

  return {
    get enabled() { return active !== null },
    get certPath() { return active?.certPath ?? null },
    /**
     * @returns {Promise<{ changed: boolean, created?: boolean }>} `created`：這次打開時新產生了 CA
     */
    apply(enabled) {
      // 依序執行：打開要 await 讀寫 CA，兩次同時打開會都看到「還沒開」而掛上兩個監聽
      const run = pending.then(() => toggle(enabled))
      pending = run.catch(() => {})
      return run
    },
  }
}
