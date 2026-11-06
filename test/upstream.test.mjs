import test from 'node:test'
import assert from 'node:assert/strict'
import net from 'node:net'

import { connectVia, proxyFromEnv } from '../src/upstream.mjs'

/**
 * 起一個假的 HTTP 代理：收到 `CONNECT` 就回固定狀態行，並記下收到的請求行。
 * 回傳 `{ port, seen, close }` —— `seen` 在連線結束後才有內容。
 */
async function fakeProxy(statusLine = 'HTTP/1.1 200 Connection Established') {
  const seen = []
  const server = net.createServer((socket) => {
    let buffer = ''
    socket.on('data', (chunk) => {
      buffer += chunk.toString('latin1')
      const end = buffer.indexOf('\r\n\r\n')
      if (end === -1) return
      seen.push(buffer.slice(0, buffer.indexOf('\r\n')))
      socket.write(`${statusLine}\r\n\r\n`)
    })
    socket.on('error', () => {})
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  return {
    port: server.address().port,
    seen,
    close: () => new Promise((resolve) => server.close(resolve)),
  }
}

/** 等一次 `connect` 事件，逾時就讓測試失敗而不是掛住 */
function waitConnect(socket, ms = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('connect 逾時')), ms)
    socket.once('connect', () => {
      clearTimeout(timer)
      resolve()
    })
    socket.once('error', (err) => {
      clearTimeout(timer)
      reject(err)
    })
  })
}

test('proxyFromEnv：HTTPS_PROXY 優先於 HTTP_PROXY', () => {
  const got = proxyFromEnv({ HTTPS_PROXY: 'http://a.example:8080', HTTP_PROXY: 'http://b.example:3128' })
  assert.deepEqual(got, { host: 'a.example', port: 8080 })
})

test('proxyFromEnv：兩個都沒設時回 null（呼叫端據此直連）', () => {
  assert.equal(proxyFromEnv({}), null)
})

test('proxyFromEnv：位址解析不了時回 null 而不是丟例外', () => {
  assert.equal(proxyFromEnv({ HTTPS_PROXY: 'not a url' }), null)
})

test('connectVia：沒給代理時直接連目標，不經 CONNECT', async () => {
  const target = net.createServer((s) => s.end())
  await new Promise((resolve) => target.listen(0, '127.0.0.1', resolve))
  const socket = connectVia(target.address().port, '127.0.0.1', null)
  await waitConnect(socket)
  socket.destroy()
  await new Promise((resolve) => target.close(resolve))
})

test('connectVia：給了代理時先對代理發 CONNECT，目標寫在請求行裡', async () => {
  const proxy = await fakeProxy()
  const socket = connectVia(8443, 'api.example.com', { host: '127.0.0.1', port: proxy.port })
  await waitConnect(socket)
  // 等代理那側把請求行收完
  await new Promise((resolve) => setTimeout(resolve, 100))
  assert.equal(proxy.seen[0], 'CONNECT api.example.com:8443 HTTP/1.1')
  socket.destroy()
  await proxy.close()
})

test('connectVia：代理回非 200 時對外發 error，而不是當成連上了', async () => {
  const proxy = await fakeProxy('HTTP/1.1 502 Bad Gateway')
  const socket = connectVia(443, 'blocked.example.com', { host: '127.0.0.1', port: proxy.port })
  const err = await new Promise((resolve) => {
    socket.once('error', resolve)
    socket.once('connect', () => resolve(new Error('不該回 connect')))
  })
  assert.match(err.message, /proxy refused CONNECT blocked\.example\.com:443/)
  socket.destroy()
  await proxy.close()
})

test('connectVia：代理連不上時把錯誤往上拋，不靜默吞掉', async () => {
  // 一個沒有東西監聽的埠
  const socket = connectVia(443, 'x.example.com', { host: '127.0.0.1', port: 1 })
  const err = await new Promise((resolve) => socket.once('error', resolve))
  assert.ok(err.code === 'ECONNREFUSED' || err.code === 'EACCES', String(err.code))
  socket.destroy()
})

test('connectVia：IPv6 目標在 CONNECT 請求行裡要加方括號', async () => {
  const proxy = await fakeProxy()
  const socket = connectVia(443, '::1', { host: '127.0.0.1', port: proxy.port })
  await waitConnect(socket)
  await new Promise((resolve) => setTimeout(resolve, 100))
  assert.equal(proxy.seen[0], 'CONNECT [::1]:443 HTTP/1.1')
  socket.destroy()
  await proxy.close()
})
