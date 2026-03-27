import { invoke } from '@tauri-apps/api/core'
import { listen, type Event as TauriEvent, type UnlistenFn } from '@tauri-apps/api/event'

type HeaderEntry = { name: string; value: string }

type BodyData =
  | { kind: 'text'; value: string; contentType?: string | null }
  | { kind: 'base64'; value: string; contentType?: string | null }
  | { kind: 'bytes'; value: number[]; contentType?: string | null }
  | { kind: 'json'; value: unknown }
  | { kind: 'formUrlEncoded'; fields: Array<{ name: string; value: string }> }

type HttpRequest = {
  url: string
  method: string
  headers: HeaderEntry[]
  body?: BodyData
  timeoutMs?: number | null
  allowRedirects?: boolean | null
  requestId?: string | null
}

type HttpResponse = {
  status: number
  statusText: string
  url: string
  headers: HeaderEntry[]
  body: number[]
  redirected: boolean
}

type WebSocketConnectResponse = {
  socketId: number
  protocol?: string | null
}

type WebSocketEventPayload = {
  socketId: number
  kind: 'open' | 'message' | 'error' | 'close'
  data?: number[] | null
  text?: string | null
  code?: number | null
  reason?: string | null
  wasClean?: boolean | null
  protocol?: string | null
}

type EventSourceConnectResponse = {
  sourceId: number
}

type EventSourceEventPayload = {
  sourceId: number
  kind: 'open' | 'message' | 'error' | 'close' | 'named'
  event?: string | null
  data?: string | null
  lastEventId?: string | null
  retryMs?: number | null
}

type NetworkRuntimeEventSourceInit = EventSourceInit & {
  headers?: HeadersInit
}

export type InstallNetworkRuntimeOptions = {
  fallbackToNativeOnError?: boolean
}

type NativeRuntime = {
  fetch?: typeof globalThis.fetch
  WebSocket?: typeof globalThis.WebSocket
  XMLHttpRequest?: typeof globalThis.XMLHttpRequest
  EventSource?: typeof globalThis.EventSource
  sendBeacon?: typeof navigator.sendBeacon
}

const PLUGIN = 'plugin:rust-network-runtime'
const WS_EVENT = 'rust-network-runtime://websocket'
const EVENTSOURCE_EVENT = 'rust-network-runtime://eventsource'

let nativeRuntime: NativeRuntime | null = null
let installedOptions: InstallNetworkRuntimeOptions = {}
let websocketListenerPromise: Promise<UnlistenFn> | null = null
let eventSourceListenerPromise: Promise<UnlistenFn> | null = null
const websocketInstances = new Map<number, NetworkRuntimeWebSocket>()
const eventSourceInstances = new Map<number, NetworkRuntimeEventSource>()

function ensureNativeRuntime(): NativeRuntime {
  if (!nativeRuntime) {
    nativeRuntime = {
      fetch: globalThis.fetch?.bind(globalThis),
      WebSocket: globalThis.WebSocket,
      XMLHttpRequest: globalThis.XMLHttpRequest,
      EventSource: globalThis.EventSource,
      sendBeacon: navigator.sendBeacon?.bind(navigator),
    }
  }
  return nativeRuntime
}

function entriesFromHeaders(headers: Headers): HeaderEntry[] {
  return Array.from(headers.entries()).map(([name, value]) => ({ name, value }))
}

function encodeBase64(bytes: Uint8Array): string {
  let binary = ''
  const chunkSize = 0x8000
  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, index + chunkSize)
    binary += String.fromCharCode(...chunk)
  }
  return btoa(binary)
}

function decodeBytes(values?: number[] | null): Uint8Array {
  return new Uint8Array(values ?? [])
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

function abortError(): DOMException {
  return new DOMException('The operation was aborted.', 'AbortError')
}

function resolveProtocol(url: string): string {
  return new URL(url, globalThis.location?.href).protocol
}

function shouldInterceptHttpUrl(url: string): boolean {
  const protocol = resolveProtocol(url)
  return protocol === 'http:' || protocol === 'https:'
}

function shouldInterceptSocketUrl(url: string): boolean {
  const protocol = resolveProtocol(url)
  return protocol === 'ws:' || protocol === 'wss:'
}

function callHandler<T extends Event>(
  target: EventTarget,
  type: string,
  event: T,
  handler?: ((event: T) => unknown) | null,
): boolean {
  const dispatched = target.dispatchEvent(event)
  handler?.(event)
  return dispatched
}

async function bodyDataFromRequest(request: Request): Promise<BodyData | undefined> {
  if (request.method === 'GET' || request.method === 'HEAD') {
    return undefined
  }

  const clone = request.clone()
  const contentType = clone.headers.get('content-type')
  const buffer = await clone.arrayBuffer()
  const bytes = new Uint8Array(buffer)

  if (bytes.byteLength === 0) {
    return undefined
  }

  return {
    kind: 'base64',
    value: encodeBase64(bytes),
    contentType,
  }
}

async function httpRequestFromFetch(input: RequestInfo | URL, init?: RequestInit): Promise<HttpRequest> {
  const request = new Request(input, init)
  return {
    url: request.url,
    method: request.method,
    headers: entriesFromHeaders(new Headers(request.headers)),
    body: await bodyDataFromRequest(request),
    timeoutMs: null,
    allowRedirects: request.redirect !== 'manual' && request.redirect !== 'error',
    requestId: null,
  }
}

async function invokeHttpRequest(request: HttpRequest, signal?: AbortSignal | null): Promise<HttpResponse> {
  if (!signal) {
    return invoke<HttpResponse>(`${PLUGIN}|http_request`, { request })
  }

  if (signal.aborted) {
    throw abortError()
  }

  const requestId = request.requestId ?? crypto.randomUUID()
  request.requestId = requestId

  const abort = () => {
    void invoke<boolean>(`${PLUGIN}|abort_http_request`, {
      payload: { requestId },
    }).catch(() => {})
  }

  signal.addEventListener('abort', abort, { once: true })
  try {
    const response = await invoke<HttpResponse>(`${PLUGIN}|http_request`, { request })
    if (signal.aborted) {
      throw abortError()
    }
    return response
  } finally {
    signal.removeEventListener('abort', abort)
  }
}

function hydrateResponse(payload: HttpResponse): Response {
  const response = new Response(toArrayBuffer(decodeBytes(payload.body)), {
    status: payload.status,
    statusText: payload.statusText,
    headers: payload.headers.map((header) => [header.name, header.value]),
  })

  Object.defineProperty(response, 'url', { value: payload.url })
  Object.defineProperty(response, 'redirected', { value: payload.redirected })
  return response
}

function isRedirect(status: number): boolean {
  return status >= 300 && status < 400
}

async function patchedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const native = ensureNativeRuntime()
  const request = new Request(input, init)

  if (!shouldInterceptHttpUrl(request.url) && native.fetch) {
    return native.fetch(input, init)
  }

  try {
    const payload = await httpRequestFromFetch(request)
    const response = await invokeHttpRequest(payload, request.signal)

    if (request.redirect === 'error' && isRedirect(response.status)) {
      throw new TypeError('Fetch redirect was disallowed by redirect="error".')
    }

    return hydrateResponse(response)
  } catch (error) {
    if (installedOptions.fallbackToNativeOnError && native.fetch) {
      return native.fetch(input, init)
    }
    throw error
  }
}

async function serializeSocketMessage(data: string | ArrayBufferLike | Blob | ArrayBufferView) {
  if (typeof data === 'string') {
    return { kind: 'text' as const, value: data }
  }

  if (data instanceof Blob) {
    return { kind: 'binary' as const, value: Array.from(new Uint8Array(await data.arrayBuffer())) }
  }

  if (ArrayBuffer.isView(data)) {
    return {
      kind: 'binary' as const,
      value: Array.from(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)),
    }
  }

  return { kind: 'binary' as const, value: Array.from(new Uint8Array(data)) }
}

async function ensureWebSocketListener(): Promise<void> {
  if (!websocketListenerPromise) {
    websocketListenerPromise = listen<WebSocketEventPayload>(WS_EVENT, (event) => {
      const socket = websocketInstances.get(event.payload.socketId)
      socket?.handlePayload(event)
    })
  }
  await websocketListenerPromise
}

async function ensureEventSourceListener(): Promise<void> {
  if (!eventSourceListenerPromise) {
    eventSourceListenerPromise = listen<EventSourceEventPayload>(EVENTSOURCE_EVENT, (event) => {
      const source = eventSourceInstances.get(event.payload.sourceId)
      source?.handlePayload(event)
    })
  }
  await eventSourceListenerPromise
}

class NetworkRuntimeWebSocket extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3

  readonly url: string
  readonly extensions = ''
  binaryType: BinaryType = 'blob'
  bufferedAmount = 0
  protocol = ''
  readyState = NetworkRuntimeWebSocket.CONNECTING
  onopen: ((event: Event) => unknown) | null = null
  onmessage: ((event: MessageEvent) => unknown) | null = null
  onerror: ((event: Event) => unknown) | null = null
  onclose: ((event: CloseEvent) => unknown) | null = null

  #socketId: number | null = null

  constructor(url: string | URL, protocols?: string | string[]) {
    super()
    this.url = String(url)
    void this.connect(protocols)
  }

  async connect(protocols?: string | string[]): Promise<void> {
    try {
      if (!shouldInterceptSocketUrl(this.url)) {
        throw new Error(`Network runtime only intercepts ws:// and wss:// URLs: ${this.url}`)
      }
      await ensureWebSocketListener()
      const response = await invoke<WebSocketConnectResponse>(`${PLUGIN}|websocket_connect`, {
        request: {
          url: this.url,
          protocols: Array.isArray(protocols) ? protocols : protocols ? [protocols] : [],
          headers: [],
        },
      })
      this.#socketId = response.socketId
      websocketInstances.set(response.socketId, this)
      this.protocol = response.protocol ?? ''
    } catch {
      this.readyState = NetworkRuntimeWebSocket.CLOSED
      callHandler(this, 'error', new Event('error'), this.onerror)
      callHandler(
        this,
        'close',
        new CloseEvent('close', { code: 1006, reason: 'Failed to connect', wasClean: false }),
        this.onclose,
      )
    }
  }

  handlePayload(event: TauriEvent<WebSocketEventPayload>): void {
    const payload = event.payload
    if (this.#socketId !== payload.socketId) {
      return
    }

    switch (payload.kind) {
      case 'open':
        this.readyState = NetworkRuntimeWebSocket.OPEN
        this.protocol = payload.protocol ?? this.protocol
        callHandler(this, 'open', new Event('open'), this.onopen)
        break
      case 'message': {
        let data: string | Blob | ArrayBuffer = payload.text ?? ''
        if (payload.data?.length) {
          const bytes = decodeBytes(payload.data)
          data = this.binaryType === 'arraybuffer' ? toArrayBuffer(bytes) : new Blob([toArrayBuffer(bytes)])
        }
        callHandler(this, 'message', new MessageEvent('message', { data }), this.onmessage)
        break
      }
      case 'error':
        callHandler(this, 'error', new Event('error'), this.onerror)
        break
      case 'close':
        this.readyState = NetworkRuntimeWebSocket.CLOSED
        if (this.#socketId !== null) {
          websocketInstances.delete(this.#socketId)
        }
        this.#socketId = null
        callHandler(
          this,
          'close',
          new CloseEvent('close', {
            code: payload.code ?? 1000,
            reason: payload.reason ?? '',
            wasClean: payload.wasClean ?? true,
          }),
          this.onclose,
        )
        break
    }
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (this.readyState !== NetworkRuntimeWebSocket.OPEN || this.#socketId === null) {
      throw new DOMException('WebSocket is not open.', 'InvalidStateError')
    }

    void serializeSocketMessage(data).then((message) =>
      invoke(`${PLUGIN}|websocket_send`, {
        request: {
          socketId: this.#socketId,
          message,
        },
      }),
    )
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === NetworkRuntimeWebSocket.CLOSING || this.readyState === NetworkRuntimeWebSocket.CLOSED) {
      return
    }

    this.readyState = NetworkRuntimeWebSocket.CLOSING
    const socketId = this.#socketId
    if (socketId === null) {
      this.readyState = NetworkRuntimeWebSocket.CLOSED
      return
    }

    this.#socketId = null
    websocketInstances.delete(socketId)

    void invoke(`${PLUGIN}|websocket_close`, {
      request: {
        socketId,
        code,
        reason,
      },
    }).catch(() => {})
  }
}

class NetworkRuntimeEventSource extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSED = 2

  readonly url: string
  readonly withCredentials: boolean
  readyState = NetworkRuntimeEventSource.CONNECTING
  onopen: ((event: Event) => unknown) | null = null
  onmessage: ((event: MessageEvent) => unknown) | null = null
  onerror: ((event: Event) => unknown) | null = null

  #sourceId: number | null = null
  #lastEventId = ''

  constructor(url: string | URL, init?: NetworkRuntimeEventSourceInit) {
    super()
    this.url = String(url)
    this.withCredentials = Boolean(init?.withCredentials)
    void this.connect(init)
  }

  async connect(init?: NetworkRuntimeEventSourceInit): Promise<void> {
    try {
      if (!shouldInterceptHttpUrl(this.url)) {
        throw new Error(`Network runtime only intercepts http:// and https:// EventSource URLs: ${this.url}`)
      }
      await ensureEventSourceListener()
      const response = await invoke<EventSourceConnectResponse>(`${PLUGIN}|eventsource_connect`, {
        request: {
          url: this.url,
          headers: init?.headers ? entriesFromHeaders(new Headers(init.headers as HeadersInit)) : [],
          withCredentials: this.withCredentials,
          lastEventId: this.#lastEventId || null,
          retryMs: null,
        },
      })
      this.#sourceId = response.sourceId
      eventSourceInstances.set(response.sourceId, this)
    } catch {
      this.readyState = NetworkRuntimeEventSource.CLOSED
      callHandler(this, 'error', new Event('error'), this.onerror)
    }
  }

  handlePayload(event: TauriEvent<EventSourceEventPayload>): void {
    const payload = event.payload
    if (this.#sourceId !== payload.sourceId) {
      return
    }

    if (payload.lastEventId) {
      this.#lastEventId = payload.lastEventId
    }

    switch (payload.kind) {
      case 'open':
        this.readyState = NetworkRuntimeEventSource.OPEN
        callHandler(this, 'open', new Event('open'), this.onopen)
        break
      case 'message':
      case 'named': {
        const type = payload.kind === 'named' ? payload.event ?? 'message' : 'message'
        const message = new MessageEvent(type, {
          data: payload.data ?? '',
          lastEventId: this.#lastEventId,
          origin: new URL(this.url).origin,
        })
        if (type === 'message') {
          callHandler(this, 'message', message, this.onmessage)
        } else {
          this.dispatchEvent(message)
        }
        break
      }
      case 'error':
        this.readyState = NetworkRuntimeEventSource.CONNECTING
        callHandler(this, 'error', new Event('error'), this.onerror)
        break
      case 'close':
        this.readyState = NetworkRuntimeEventSource.CLOSED
        if (this.#sourceId !== null) {
          eventSourceInstances.delete(this.#sourceId)
        }
        break
    }
  }

  close(): void {
    this.readyState = NetworkRuntimeEventSource.CLOSED
    if (this.#sourceId !== null) {
      const sourceId = this.#sourceId
      eventSourceInstances.delete(sourceId)
      this.#sourceId = null
      void invoke(`${PLUGIN}|eventsource_close`, {
        request: { sourceId },
      })
    }
  }
}

class NetworkRuntimeXMLHttpRequest extends EventTarget {
  static readonly UNSENT = 0
  static readonly OPENED = 1
  static readonly HEADERS_RECEIVED = 2
  static readonly LOADING = 3
  static readonly DONE = 4

  readyState = NetworkRuntimeXMLHttpRequest.UNSENT
  response: unknown = null
  responseText = ''
  responseType: XMLHttpRequestResponseType = ''
  responseURL = ''
  status = 0
  statusText = ''
  timeout = 0
  withCredentials = false
  onreadystatechange: ((event: Event) => unknown) | null = null
  onloadstart: ((event: ProgressEvent) => unknown) | null = null
  onprogress: ((event: ProgressEvent) => unknown) | null = null
  onload: ((event: ProgressEvent) => unknown) | null = null
  onloadend: ((event: ProgressEvent) => unknown) | null = null
  onabort: ((event: ProgressEvent) => unknown) | null = null
  onerror: ((event: ProgressEvent) => unknown) | null = null
  ontimeout: ((event: ProgressEvent) => unknown) | null = null

  #method = 'GET'
  #url = ''
  #headers = new Headers()
  #responseHeaders = new Headers()
  #requestId: string | null = null

  open(method: string, url: string | URL, async = true): void {
    if (!async) {
      throw new Error('Synchronous XMLHttpRequest is not supported.')
    }

    this.#method = method.toUpperCase()
    this.#url = String(url)
    this.#headers = new Headers()
    this.#responseHeaders = new Headers()
    this.readyState = NetworkRuntimeXMLHttpRequest.OPENED
    callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange)
  }

  setRequestHeader(name: string, value: string): void {
    this.#headers.append(name, value)
  }

  getResponseHeader(name: string): string | null {
    return this.#responseHeaders.get(name)
  }

  getAllResponseHeaders(): string {
    return Array.from(this.#responseHeaders.entries())
      .map(([name, value]) => `${name}: ${value}\r\n`)
      .join('')
  }

  overrideMimeType(): void {}

  abort(): void {
    if (!this.#requestId) {
      return
    }

    const requestId = this.#requestId
    this.#requestId = null
    void invoke(`${PLUGIN}|abort_http_request`, {
      payload: { requestId },
    })
    this.readyState = NetworkRuntimeXMLHttpRequest.DONE
    const event = new ProgressEvent('abort')
    callHandler(this, 'abort', event, this.onabort)
    callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend)
  }

  async send(body?: Document | XMLHttpRequestBodyInit | null): Promise<void> {
    if (this.readyState !== NetworkRuntimeXMLHttpRequest.OPENED) {
      throw new DOMException('XMLHttpRequest is not opened.', 'InvalidStateError')
    }

    if (!shouldInterceptHttpUrl(this.#url)) {
      throw new Error(`Network runtime only intercepts http:// and https:// XMLHttpRequest URLs: ${this.#url}`)
    }

    this.#requestId = crypto.randomUUID()
    callHandler(this, 'loadstart', new ProgressEvent('loadstart'), this.onloadstart)

    try {
      const request = new Request(this.#url, {
        method: this.#method,
        headers: this.#headers,
        body: body as BodyInit | null | undefined,
      })

      const response = await invokeHttpRequest(
        {
          url: request.url,
          method: request.method,
          headers: entriesFromHeaders(new Headers(request.headers)),
          body: await bodyDataFromRequest(request),
          timeoutMs: this.timeout || null,
          allowRedirects: true,
          requestId: this.#requestId,
        },
        null,
      )

      this.#responseHeaders = new Headers(response.headers.map((header) => [header.name, header.value]))
      this.status = response.status
      this.statusText = response.statusText
      this.responseURL = response.url
      this.readyState = NetworkRuntimeXMLHttpRequest.HEADERS_RECEIVED
      callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange)

      const bytes = decodeBytes(response.body)
      this.readyState = NetworkRuntimeXMLHttpRequest.LOADING
      callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange)
      callHandler(
        this,
        'progress',
        new ProgressEvent('progress', { lengthComputable: true, loaded: bytes.byteLength, total: bytes.byteLength }),
        this.onprogress,
      )

      this.responseText = new TextDecoder().decode(bytes)
      this.response = toXhrResponse(this.responseType, bytes, this.responseText)
      this.readyState = NetworkRuntimeXMLHttpRequest.DONE
      callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange)
      callHandler(this, 'load', new ProgressEvent('load'), this.onload)
      callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend)
    } catch (error) {
      this.readyState = NetworkRuntimeXMLHttpRequest.DONE
      if (error instanceof DOMException && error.name === 'AbortError') {
        callHandler(this, 'abort', new ProgressEvent('abort'), this.onabort)
      } else if (String(error).includes('deadline') || String(error).includes('timed out')) {
        callHandler(this, 'timeout', new ProgressEvent('timeout'), this.ontimeout)
      } else {
        callHandler(this, 'error', new ProgressEvent('error'), this.onerror)
      }
      callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend)
    } finally {
      this.#requestId = null
    }
  }
}

function toXhrResponse(type: XMLHttpRequestResponseType, bytes: Uint8Array, text: string): unknown {
  switch (type) {
    case '':
    case 'text':
      return text
    case 'json':
      return text ? JSON.parse(text) : null
    case 'arraybuffer':
      return toArrayBuffer(bytes)
    case 'blob':
      return new Blob([toArrayBuffer(bytes)])
    default:
      return text
  }
}

function patchedSendBeacon(url: string | URL, data?: BodyInit | null): boolean {
  const native = ensureNativeRuntime()
  const targetUrl = String(url)

  if (!shouldInterceptHttpUrl(targetUrl) && native.sendBeacon) {
    return native.sendBeacon(url, data)
  }

  try {
    const request = new Request(targetUrl, {
      method: 'POST',
      body: data ?? null,
    })

    void bodyDataFromRequest(request).then((body) =>
      invoke<boolean>(`${PLUGIN}|send_beacon`, {
        request: {
          url: request.url,
          method: request.method,
          headers: entriesFromHeaders(new Headers(request.headers)),
          body,
          timeoutMs: null,
          allowRedirects: true,
          requestId: null,
        },
      }),
    )

    return true
  } catch {
    if (installedOptions.fallbackToNativeOnError && nativeRuntime?.sendBeacon) {
      return nativeRuntime.sendBeacon(url, data)
    }
    return false
  }
}

export function installNetworkRuntime(options: InstallNetworkRuntimeOptions = {}): void {
  ensureNativeRuntime()
  installedOptions = options

  globalThis.fetch = patchedFetch as typeof globalThis.fetch
  globalThis.WebSocket = NetworkRuntimeWebSocket as unknown as typeof WebSocket
  globalThis.XMLHttpRequest = NetworkRuntimeXMLHttpRequest as unknown as typeof XMLHttpRequest
  globalThis.EventSource = NetworkRuntimeEventSource as unknown as typeof EventSource
  Object.defineProperty(navigator, 'sendBeacon', {
    configurable: true,
    writable: true,
    value: patchedSendBeacon,
  })
}

export function restoreNativeNetworkRuntime(): void {
  const native = ensureNativeRuntime()
  if (native.fetch) {
    globalThis.fetch = native.fetch
  }
  if (native.WebSocket) {
    globalThis.WebSocket = native.WebSocket
  }
  if (native.XMLHttpRequest) {
    globalThis.XMLHttpRequest = native.XMLHttpRequest
  }
  if (native.EventSource) {
    globalThis.EventSource = native.EventSource
  }
  if (native.sendBeacon) {
    Object.defineProperty(navigator, 'sendBeacon', {
      configurable: true,
      writable: true,
      value: native.sendBeacon,
    })
  }
}
