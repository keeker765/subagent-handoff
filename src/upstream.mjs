import net from 'node:net'

/**
 * 為上游連線建立「經代理」的 TCP 通道。
 *
 * 為什麼需要：這個 router 的 CONNECT 隧道與 WebSocket 轉發（connect.mjs）原本直接用
 * `net.connect` / `tls.connect`，而 Node 的這兩個 API 完全不看 `HTTP_PROXY`。
 * 部署在只允許經代理出網的機器上時（宿主用 nftables 把其他路徑全 DROP），
 * 這些連線會全部失敗；就算不被擋，也會以伺服器自己的真實 IP 出去。
 *
 * 代理取自 `HTTPS_PROXY`／`HTTP_PROXY`（大寫優先，與 curl 一致）。兩個都沒設時
 * 退回直連，行為與原版完全相同 —— 本機開發與 `npm test` 不受影響。
 *
 * 這裡刻意不偽裝 socket 的 `connect` 事件。同一條 socket 上，原生 `connect`
 * （連上代理）與「隧道已通」是兩件事，而呼叫端無法分辨是哪一個；試過用包裝層
 * 或改寫 emit 去掩蓋，都會在 pipe 與 tls.connect 上出問題。所以改成明確的
 * callback：連線**到目標**之後才呼叫 `onReady`。
 */

/** 從環境變數讀代理；沒有就回 null，呼叫端據此退回直連 */
export function proxyFromEnv(env = process.env) {
  const raw = env.HTTPS_PROXY || env.https_proxy || env.HTTP_PROXY || env.http_proxy
  if (!raw) return null
  try {
    const url = new URL(raw)
    const port = Number(url.port) || (url.protocol === 'https:' ? 443 : 80)
    return { host: url.hostname, port, noProxy: parseNoProxy(env) }
  } catch {
    return null
  }
}

/**
 * 把 `NO_PROXY` 拆成規則。每條記住主機（或後綴）與可選的埠限制。
 *
 * 不遵守它的話，凡是有代理的部署環境，連本地假上游、`127.0.0.1`、
 * docker 內部服務名都會被送進代理 —— 那些位址本來就不該出去。
 *
 * 形式與 curl 一致：`*`（全部繞過）、`host`、`.suffix`、`host:port`。
 *
 * @returns {{ all: boolean, rules: Array<{ suffix: boolean, host: string, port: number | null }> }}
 */
export function parseNoProxy(env = process.env) {
  const raw = env.NO_PROXY || env.no_proxy || ''
  const out = { all: false, rules: [] }
  for (const piece of raw.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)) {
    // `*` 是「所有目的地都直連」
    if (piece === '*') {
      out.all = true
      continue
    }
    // `host:port`：只在那個埠上繞過
    const colon = piece.lastIndexOf(':')
    const hasPort = colon > 0 && /^\d+$/.test(piece.slice(colon + 1))
    const hostPart = hasPort ? piece.slice(0, colon) : piece
    const suffix = hostPart.startsWith('.')
    const host = suffix ? hostPart.slice(1) : hostPart
    if (!host) continue
    out.rules.push({ suffix, host, port: hasPort ? Number(piece.slice(colon + 1)) : null })
  }
  return out
}

/**
 * 這個目的地該不該繞過代理。
 *
 * @param {string} host 目標主機名或 IP
 * @param {number} port 目標埠
 * @param {{ all: boolean, rules: Array<{ suffix: boolean, host: string, port: number | null }> }} noProxy
 *   `parseNoProxy` 的回傳值
 * @returns {boolean}
 */
export function bypassProxy(host, port, noProxy) {
  if (noProxy.all) return true
  const name = host.toLowerCase()
  return noProxy.rules.some((rule) => {
    // 帶埠的規則只管那個埠
    if (rule.port !== null && rule.port !== port) return false
    // 後綴規則連子網域一起涵蓋
    return rule.suffix ? name === rule.host || name.endsWith(`.${rule.host}`) : name === rule.host
  })
}


/**
 * 連到 `host:port`；設了代理就先對代理發 `CONNECT`，隧道建立後才回報。
 *
 * @param {number} port 目標埠
 * @param {string} host 目標主機
 * @param {{ host: string, port: number } | null} proxy 代理位址；null 表示直連
 * @param {(err: Error | null, socket: import('node:net').Socket | null) => void} onReady
 *   成功時 `(null, socket)`，失敗時 `(err, null)`。socket 在成功之後完全歸呼叫端，
 *   本函式不再碰它的事件。
 * @returns {import('node:net').Socket} 底層 socket，呼叫端可在回報前就 destroy 它
 */
export function connectVia(port, host, proxy, onReady) {
  // 沒設代理，或目標在 NO_PROXY 裡：原版行為，直接連
  if (!proxy || bypassProxy(host, port, proxy.noProxy ?? { all: false, rules: [] })) {
    const socket = net.connect(port, host)
    // onReady 只回報「連上了或失敗了」這一個事實。呼叫端不能靠再等一次
    // `connect` 事件：那時它已經發完了（下面代理分支同理，回報時機都是
    // 事件過後）。排到次輪事件循環才回報，讓呼叫端在本輪 emit 走完之後
    // 再掛自己的監聽。
    socket.once('connect', () => setImmediate(() => onReady(null, socket)))
    socket.once('error', (err) => setImmediate(() => onReady(err, null)))
    return socket
  }

  const authority = net.isIPv6(host) ? `[${host}]:${port}` : `${host}:${port}`
  const socket = net.connect(proxy.port, proxy.host)
  let handshake = ''
  let done = false

  // 這個監聽只吃到握手結束為止，之後呼叫端自己接管資料
  const onReply = (chunk) => {
    handshake += chunk.toString('latin1')
    // 只需要狀態行與標頭；收到空行就算握手完成，之後的位元組屬於隧道內容
    const end = handshake.indexOf('\r\n\r\n')
    if (end === -1) return
    done = true
    socket.removeListener('data', onReply)

    const statusLine = handshake.slice(0, handshake.indexOf('\r\n'))
    const code = Number(statusLine.split(' ')[1])
    if (code !== 200) {
      socket.destroy()
      setImmediate(() => onReady(new Error(`proxy refused CONNECT ${authority}: ${statusLine}`), null))
      return
    }
    // 標頭之後若已夾了位元組，塞回可讀緩衝，呼叫端才不會漏掉開頭
    const rest = Buffer.from(handshake.slice(end + 4), 'latin1')
    if (rest.length) socket.unshift(rest)
    setImmediate(() => onReady(null, socket))
  }

  socket.on('connect', () => socket.write(`CONNECT ${authority} HTTP/1.1\r\nHost: ${authority}\r\n\r\n`))
  socket.on('data', onReply)
  // 還沒等到應答就斷線：回報錯誤，別讓呼叫端靜靜掛住
  socket.on('close', () => {
    if (done) return
    done = true
    setImmediate(() => onReady(new Error(`proxy closed before replying to CONNECT ${authority}`), null))
  })
  return socket
}
