import net from 'node:net'
import http from 'node:http'
import { Duplex } from 'node:stream'

/**
 * 為上游連線建立「經代理」的 TCP 通道。
 *
 * 為什麼需要：這個 router 的 CONNECT 隧道與 WebSocket 轉發（connect.mjs）原本直接用
 * `net.connect` / `tls.connect`，而 Node 的這兩個 API 完全不看 `HTTP_PROXY`。
 * 部署在只允許經代理出網的機器上時（宿主用 nftables 把其他路徑全 DROP），
 * 這些連線會全部失敗；就算不被擋，也會以伺服器自己的真實 IP 出去。
 *
 * 作法是把目標主機交給上游代理，讓代理去連：對代理發一則
 * `CONNECT host:port`，收到 200 之後那條 socket 就是到目標的原始位元組通道。
 *
 * 代理取自 `HTTPS_PROXY`／`HTTP_PROXY`（大寫優先，與 curl 一致）。兩個都沒設時
 * 退回直連，行為與原版完全相同 —— 本機開發與 `npm test` 不受影響。
 */

/** 從環境變數讀代理；沒有就回 null，呼叫端據此退回直連 */
export function proxyFromEnv(env = process.env) {
  const raw = env.HTTPS_PROXY || env.https_proxy || env.HTTP_PROXY || env.http_proxy
  if (!raw) return null
  try {
    const url = new URL(raw)
    const port = Number(url.port) || (url.protocol === 'https:' ? 443 : 80)
    return { host: url.hostname, port }
  } catch {
    return null
  }
}

/**
 * 在 `raw` 之上包一層只轉發位元組的 Duplex。
 *
 * 為什麼要包而不是直接回傳原 socket：原生 socket 連上**代理**時會自己發一次
 * `connect`，而呼叫端（tunnel、relayUpgrade、以及它們的測試）都會在回傳值上掛
 * `connect` 監聽。若把原 socket 直接給出去，那個事件會被誤認為「隧道通了」，
 * 呼叫端於是提前寫出 `200 Connection Established`。包一層之後，外層物件只在
 * 代理回了 200 之後才發 `connect`，兩條路徑的事件順序才一致。
 *
 * @param {net.Socket} raw 到代理（或目標）的原始連線
 * @returns {Duplex}
 */
function wrap(raw) {
  const outer = new Duplex({
    read() {},
    write(chunk, encoding, callback) {
      if (raw.destroyed) {
        callback(new Error('upstream socket destroyed'))
        return
      }
      raw.write(chunk, encoding, callback)
    },
    final(callback) {
      raw.end()
      callback()
    },
    destroy(error, callback) {
      raw.destroy()
      callback(error)
    },
  })
  // 代理來的位元組原樣交給消費者；結束與錯誤也照傳
  raw.on('data', (chunk) => {
    if (!outer.push(chunk)) raw.pause()
  })
  raw.on('end', () => outer.push(null))
  raw.on('error', (err) => outer.destroy(err))
  raw.on('close', () => outer.destroy())
  outer.on('drain', () => raw.resume())
  return outer
}

/**
 * 開一條到 `host:port` 的 TCP 連線；設了代理就經代理，否則直連。
 *
 * 回傳的物件在代理路徑上會等 `CONNECT` 的 200 之後才對外發 `connect`，
 * 直連路徑則沿用原生行為。呼叫端不需要判斷走的是哪一條。
 *
 * @param {number} port 目標埠
 * @param {string} host 目標主機
 * @param {{ host: string, port: number } | null} [proxy] 代理位址；null 表示直連
 * @returns {Duplex}
 */
export function connectVia(port, host, proxy) {
  // 沒設代理：原版行為，直接連
  if (!proxy) return net.connect(port, host)

  const socket = net.connect(proxy.port, proxy.host)
  const outer = wrap(socket)
  // 原生 connect 只代表連上代理，不對外透出
  socket.on('connect', () => {
    socket.write(
      `CONNECT ${net.isIPv6(host) ? `[${host}]:${port}` : `${host}:${port}`} HTTP/1.1\r\n` +
      `Host: ${net.isIPv6(host) ? `[${host}]:${port}` : `${host}:${port}`}\r\n\r\n`,
    )
  })

  let response = ''
  const onReply = (chunk) => {
    response += chunk.toString('latin1')
    // 只需要狀態行與標頭；收到空行就算握手完成，之後的位元組屬於隧道內容
    const end = response.indexOf('\r\n\r\n')
    if (end === -1) return
    socket.pause()
    socket.removeListener('data', onReply)

    const statusLine = response.slice(0, response.indexOf('\r\n'))
    const code = Number(statusLine.split(' ')[1])
    if (code !== 200) {
      outer.destroy(new Error(`proxy refused CONNECT ${host}:${port}: ${statusLine}`))
      return
    }
    // 標頭之後若已夾了位元組，先交給消費者，免得漏掉開頭
    const rest = Buffer.from(response.slice(end + 4), 'latin1')
    if (rest.length) outer.push(rest)
    socket.resume()
    outer.emit('connect')
  }
  socket.on('data', onReply)
  return outer
}
