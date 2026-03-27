<script>
  import { onMount } from 'svelte'
  import { invoke } from '@tauri-apps/api/core'

  let serverOrigin = ''
  let status = 'Starting Rust demo server for the built app...'
  let busy = false
  let logs = []

  function pushLog(entry) {
    logs = [{ timestamp: new Date().toLocaleTimeString(), ...entry }, ...logs].slice(0, 10)
  }

  async function runFetchDemo() {
    busy = true
    try {
      const url = `${serverOrigin}/api/inspect?from=fetch&message=hello-from-fetch`
      const response = await fetch(url, {
        headers: {
          'x-demo-client': 'fetch',
          'x-demo-route': 'webview-to-rust',
        },
      })
      const payload = await response.json()
      pushLog({
        client: 'fetch',
        request: `GET ${url}`,
        response: payload,
      })
    } finally {
      busy = false
    }
  }

  async function runXhrDemo() {
    busy = true
    try {
      const url = `${serverOrigin}/api/inspect`
      const response = await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest()
        xhr.open('POST', url)
        xhr.setRequestHeader('content-type', 'application/json')
        xhr.setRequestHeader('x-demo-client', 'xhr')
        xhr.setRequestHeader('x-demo-route', 'webview-to-rust')
        xhr.onload = () => resolve(JSON.parse(xhr.responseText))
        xhr.onerror = () => reject(new Error('XMLHttpRequest failed'))
        xhr.send(JSON.stringify({
          from: 'xhr',
          payload: 'posted-from-the-webview',
        }))
      })

      pushLog({
        client: 'XMLHttpRequest',
        request: `POST ${url}`,
        response,
      })
    } finally {
      busy = false
    }
  }

  async function runBeaconDemo() {
    busy = true
    try {
      const url = `${serverOrigin}/beacon`
      const sent = navigator.sendBeacon(
        url,
        new Blob(
          [JSON.stringify({ from: 'sendBeacon', payload: 'beacon-from-the-webview' })],
          { type: 'application/json' },
        ),
      )

      await new Promise((resolve) => setTimeout(resolve, 200))
      const received = await fetch(`${serverOrigin}/api/beacon-log`).then((response) => response.json())

      pushLog({
        client: 'sendBeacon',
        request: `POST ${url}`,
        response: { sent, received },
      })
    } finally {
      busy = false
    }
  }

  async function runWebSocketDemo() {
    busy = true
    const url = `${serverOrigin.replace('http', 'ws')}/ws`
    try {
      const frames = await new Promise((resolve, reject) => {
        const socket = new WebSocket(url)
        const messages = []
        let settled = false
        let sawError = false

        const finish = (callback) => {
          if (settled) {
            return
          }
          settled = true
          clearTimeout(timeout)
          callback()
        }

        const timeout = setTimeout(() => {
          finish(() => {
            socket.close()
            reject(new Error(`WebSocket demo timed out for ${url}`))
          })
        }, 3000)

        socket.onopen = () => {
          socket.send('hello-from-websocket')
        }

        socket.onmessage = (event) => {
          messages.push(JSON.parse(event.data))
          if (messages.length >= 2) {
            finish(() => {
              socket.close(1000, 'demo-complete')
              resolve(messages)
            })
          }
        }

        socket.onerror = () => {
          sawError = true
        }

        socket.onclose = (event) => {
          if (settled) {
            return
          }
          finish(() => {
            reject(
              new Error(
                `WebSocket closed before demo completed. code=${event.code} reason=${event.reason || 'n/a'} sawError=${sawError} seen=${JSON.stringify(messages)}`,
              ),
            )
          })
        }
      })

      pushLog({
        client: 'WebSocket',
        request: `CONNECT ${url}`,
        response: frames,
      })
    } catch (error) {
      pushLog({
        client: 'WebSocket',
        request: `CONNECT ${url}`,
        response: { error: error instanceof Error ? error.message : String(error) },
      })
      throw error
    } finally {
      busy = false
    }
  }

  async function runEventSourceDemo() {
    busy = true
    try {
      const url = `${serverOrigin}/sse`
      const events = await new Promise((resolve, reject) => {
        const source = new EventSource(url)
        const seen = []
        const timeout = setTimeout(() => {
          source.close()
          reject(new Error('EventSource demo timed out'))
        }, 3000)

        const finishIfReady = () => {
          if (seen.length >= 3) {
            clearTimeout(timeout)
            source.close()
            resolve(seen)
          }
        }

        source.onmessage = (event) => {
          seen.push({ type: 'message', payload: JSON.parse(event.data) })
          finishIfReady()
        }

        source.addEventListener('demo', (event) => {
          seen.push({ type: 'demo', payload: JSON.parse(event.data) })
          finishIfReady()
        })

        source.onerror = () => {
          if (seen.length < 3) {
            clearTimeout(timeout)
            reject(new Error('EventSource demo failed'))
          }
        }
      })

      pushLog({
        client: 'EventSource',
        request: `GET ${url}`,
        response: events,
      })
    } finally {
      busy = false
    }
  }

  async function runAll() {
    await runFetchDemo()
    await runXhrDemo()
    await runBeaconDemo()
    await runWebSocketDemo()
    await runEventSourceDemo()
  }

  onMount(async () => {
    console.log(`[startup] App onMount entered at ${performance.now().toFixed(1)}ms`)
    const invokeStart = performance.now()
    serverOrigin = await invoke('demo_server_origin')
    console.log(
      `[startup] demo_server_origin resolved in ${(performance.now() - invokeStart).toFixed(1)}ms at ${performance.now().toFixed(1)}ms`,
    )
    status = 'Built-app demo ready. Browser APIs now call into Rust, and Rust forwards them to the demo server.'
  })
</script>

<main class="shell">
  <section class="hero">
    <p class="eyebrow">Tauri Plugin Demo</p>
    <h1>Rust-backed networking, exercised end to end.</h1>
    <p class="lede">
      The page uses patched browser networking APIs. Requests leave the webview through the
      plugin, are executed by Rust, and hit Rust HTTP, WebSocket, SSE, and beacon endpoints
      running inside the Tauri app.
    </p>
    <div class="status-card">
      <div>
        <span class="label">Demo server</span>
        <strong>{serverOrigin || 'booting...'}</strong>
      </div>
      <div>
        <span class="label">Status</span>
        <strong>{status}</strong>
      </div>
    </div>
    <div class="actions">
      <button on:click={runFetchDemo} disabled={busy || !serverOrigin}>Run fetch GET</button>
      <button on:click={runXhrDemo} disabled={busy || !serverOrigin}>Run XHR POST</button>
      <button on:click={runBeaconDemo} disabled={busy || !serverOrigin}>Run sendBeacon</button>
      <button on:click={runWebSocketDemo} disabled={busy || !serverOrigin}>Run WebSocket</button>
      <button on:click={runEventSourceDemo} disabled={busy || !serverOrigin}>Run EventSource</button>
      <button class="secondary" on:click={runAll} disabled={busy || !serverOrigin}>Run full demo</button>
    </div>
  </section>

  <section class="flow">
    <div class="flow-card">
      <span>1</span>
      <p>Webview code calls patched <code>fetch</code>, <code>XHR</code>, <code>WebSocket</code>, <code>EventSource</code>, or <code>sendBeacon</code>.</p>
    </div>
    <div class="flow-card">
      <span>2</span>
      <p>The installed shim forwards that traffic into the Rust plugin runtime.</p>
    </div>
    <div class="flow-card">
      <span>3</span>
      <p>Rust routes it into the in-process demo server and returns the observed results to the UI.</p>
    </div>
  </section>

  <section class="log-panel">
    <div class="panel-header">
      <h2>Request / response log</h2>
      <span>{logs.length} captured</span>
    </div>

    {#if logs.length === 0}
      <p class="empty">No traffic captured yet.</p>
    {:else}
      <div class="log-list">
        {#each logs as log}
          <article class="log-entry">
            <header>
              <strong>{log.client}</strong>
              <span>{log.timestamp}</span>
            </header>
            <p class="request-line">{log.request}</p>
            <pre>{JSON.stringify(log.response, null, 2)}</pre>
          </article>
        {/each}
      </div>
    {/if}
  </section>
</main>
