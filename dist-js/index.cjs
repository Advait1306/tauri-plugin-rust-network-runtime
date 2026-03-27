'use strict';

var core = require('@tauri-apps/api/core');
var event = require('@tauri-apps/api/event');

/******************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */
/* global Reflect, Promise, SuppressedError, Symbol, Iterator */


function __classPrivateFieldGet(receiver, state, kind, f) {
    if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a getter");
    if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
    return kind === "m" ? f : kind === "a" ? f.call(receiver) : f ? f.value : state.get(receiver);
}

function __classPrivateFieldSet(receiver, state, value, kind, f) {
    if (kind === "m") throw new TypeError("Private method is not writable");
    if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a setter");
    if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
    return (kind === "a" ? f.call(receiver, value) : f ? f.value = value : state.set(receiver, value)), value;
}

typeof SuppressedError === "function" ? SuppressedError : function (error, suppressed, message) {
    var e = new Error(message);
    return e.name = "SuppressedError", e.error = error, e.suppressed = suppressed, e;
};

var _NetworkRuntimeWebSocket_socketId, _NetworkRuntimeEventSource_sourceId, _NetworkRuntimeEventSource_lastEventId, _NetworkRuntimeXMLHttpRequest_method, _NetworkRuntimeXMLHttpRequest_url, _NetworkRuntimeXMLHttpRequest_headers, _NetworkRuntimeXMLHttpRequest_responseHeaders, _NetworkRuntimeXMLHttpRequest_requestId;
const PLUGIN = 'plugin:rust-network-runtime';
const WS_EVENT = 'rust-network-runtime://websocket';
const EVENTSOURCE_EVENT = 'rust-network-runtime://eventsource';
let nativeRuntime = null;
let installedOptions = {};
let websocketListenerPromise = null;
let eventSourceListenerPromise = null;
const websocketInstances = new Map();
const eventSourceInstances = new Map();
function ensureNativeRuntime() {
    if (!nativeRuntime) {
        nativeRuntime = {
            fetch: globalThis.fetch?.bind(globalThis),
            WebSocket: globalThis.WebSocket,
            XMLHttpRequest: globalThis.XMLHttpRequest,
            EventSource: globalThis.EventSource,
            sendBeacon: navigator.sendBeacon?.bind(navigator),
        };
    }
    return nativeRuntime;
}
function entriesFromHeaders(headers) {
    return Array.from(headers.entries()).map(([name, value]) => ({ name, value }));
}
function encodeBase64(bytes) {
    let binary = '';
    const chunkSize = 0x8000;
    for (let index = 0; index < bytes.length; index += chunkSize) {
        const chunk = bytes.subarray(index, index + chunkSize);
        binary += String.fromCharCode(...chunk);
    }
    return btoa(binary);
}
function decodeBytes(values) {
    return new Uint8Array(values ?? []);
}
function toArrayBuffer(bytes) {
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}
function abortError() {
    return new DOMException('The operation was aborted.', 'AbortError');
}
function resolveProtocol(url) {
    return new URL(url, globalThis.location?.href).protocol;
}
function shouldInterceptHttpUrl(url) {
    const protocol = resolveProtocol(url);
    return protocol === 'http:' || protocol === 'https:';
}
function shouldInterceptSocketUrl(url) {
    const protocol = resolveProtocol(url);
    return protocol === 'ws:' || protocol === 'wss:';
}
function callHandler(target, type, event, handler) {
    const dispatched = target.dispatchEvent(event);
    handler?.(event);
    return dispatched;
}
async function bodyDataFromRequest(request) {
    if (request.method === 'GET' || request.method === 'HEAD') {
        return undefined;
    }
    const clone = request.clone();
    const contentType = clone.headers.get('content-type');
    const buffer = await clone.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    if (bytes.byteLength === 0) {
        return undefined;
    }
    return {
        kind: 'base64',
        value: encodeBase64(bytes),
        contentType,
    };
}
async function httpRequestFromFetch(input, init) {
    const request = new Request(input, init);
    return {
        url: request.url,
        method: request.method,
        headers: entriesFromHeaders(new Headers(request.headers)),
        body: await bodyDataFromRequest(request),
        timeoutMs: null,
        allowRedirects: request.redirect !== 'manual' && request.redirect !== 'error',
        requestId: null,
    };
}
async function invokeHttpRequest(request, signal) {
    if (!signal) {
        return core.invoke(`${PLUGIN}|http_request`, { request });
    }
    if (signal.aborted) {
        throw abortError();
    }
    const requestId = request.requestId ?? crypto.randomUUID();
    request.requestId = requestId;
    const abort = () => {
        void core.invoke(`${PLUGIN}|abort_http_request`, {
            payload: { requestId },
        }).catch(() => { });
    };
    signal.addEventListener('abort', abort, { once: true });
    try {
        const response = await core.invoke(`${PLUGIN}|http_request`, { request });
        if (signal.aborted) {
            throw abortError();
        }
        return response;
    }
    finally {
        signal.removeEventListener('abort', abort);
    }
}
function hydrateResponse(payload) {
    const response = new Response(toArrayBuffer(decodeBytes(payload.body)), {
        status: payload.status,
        statusText: payload.statusText,
        headers: payload.headers.map((header) => [header.name, header.value]),
    });
    Object.defineProperty(response, 'url', { value: payload.url });
    Object.defineProperty(response, 'redirected', { value: payload.redirected });
    return response;
}
function isRedirect(status) {
    return status >= 300 && status < 400;
}
async function patchedFetch(input, init) {
    const native = ensureNativeRuntime();
    const request = new Request(input, init);
    if (!shouldInterceptHttpUrl(request.url) && native.fetch) {
        return native.fetch(input, init);
    }
    try {
        const payload = await httpRequestFromFetch(request);
        const response = await invokeHttpRequest(payload, request.signal);
        if (request.redirect === 'error' && isRedirect(response.status)) {
            throw new TypeError('Fetch redirect was disallowed by redirect="error".');
        }
        return hydrateResponse(response);
    }
    catch (error) {
        if (installedOptions.fallbackToNativeOnError && native.fetch) {
            return native.fetch(input, init);
        }
        throw error;
    }
}
async function serializeSocketMessage(data) {
    if (typeof data === 'string') {
        return { kind: 'text', value: data };
    }
    if (data instanceof Blob) {
        return { kind: 'binary', value: Array.from(new Uint8Array(await data.arrayBuffer())) };
    }
    if (ArrayBuffer.isView(data)) {
        return {
            kind: 'binary',
            value: Array.from(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)),
        };
    }
    return { kind: 'binary', value: Array.from(new Uint8Array(data)) };
}
async function ensureWebSocketListener() {
    if (!websocketListenerPromise) {
        websocketListenerPromise = event.listen(WS_EVENT, (event) => {
            const socket = websocketInstances.get(event.payload.socketId);
            socket?.handlePayload(event);
        });
    }
    await websocketListenerPromise;
}
async function ensureEventSourceListener() {
    if (!eventSourceListenerPromise) {
        eventSourceListenerPromise = event.listen(EVENTSOURCE_EVENT, (event) => {
            const source = eventSourceInstances.get(event.payload.sourceId);
            source?.handlePayload(event);
        });
    }
    await eventSourceListenerPromise;
}
class NetworkRuntimeWebSocket extends EventTarget {
    constructor(url, protocols) {
        super();
        this.extensions = '';
        this.binaryType = 'blob';
        this.bufferedAmount = 0;
        this.protocol = '';
        this.readyState = NetworkRuntimeWebSocket.CONNECTING;
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        this.onclose = null;
        _NetworkRuntimeWebSocket_socketId.set(this, null);
        this.url = String(url);
        void this.connect(protocols);
    }
    async connect(protocols) {
        try {
            if (!shouldInterceptSocketUrl(this.url)) {
                throw new Error(`Network runtime only intercepts ws:// and wss:// URLs: ${this.url}`);
            }
            await ensureWebSocketListener();
            const response = await core.invoke(`${PLUGIN}|websocket_connect`, {
                request: {
                    url: this.url,
                    protocols: Array.isArray(protocols) ? protocols : protocols ? [protocols] : [],
                    headers: [],
                },
            });
            __classPrivateFieldSet(this, _NetworkRuntimeWebSocket_socketId, response.socketId, "f");
            websocketInstances.set(response.socketId, this);
            this.protocol = response.protocol ?? '';
        }
        catch {
            this.readyState = NetworkRuntimeWebSocket.CLOSED;
            callHandler(this, 'error', new Event('error'), this.onerror);
            callHandler(this, 'close', new CloseEvent('close', { code: 1006, reason: 'Failed to connect', wasClean: false }), this.onclose);
        }
    }
    handlePayload(event) {
        const payload = event.payload;
        if (__classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f") !== payload.socketId) {
            return;
        }
        switch (payload.kind) {
            case 'open':
                this.readyState = NetworkRuntimeWebSocket.OPEN;
                this.protocol = payload.protocol ?? this.protocol;
                callHandler(this, 'open', new Event('open'), this.onopen);
                break;
            case 'message': {
                let data = payload.text ?? '';
                if (payload.data?.length) {
                    const bytes = decodeBytes(payload.data);
                    data = this.binaryType === 'arraybuffer' ? toArrayBuffer(bytes) : new Blob([toArrayBuffer(bytes)]);
                }
                callHandler(this, 'message', new MessageEvent('message', { data }), this.onmessage);
                break;
            }
            case 'error':
                callHandler(this, 'error', new Event('error'), this.onerror);
                break;
            case 'close':
                this.readyState = NetworkRuntimeWebSocket.CLOSED;
                if (__classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f") !== null) {
                    websocketInstances.delete(__classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f"));
                }
                __classPrivateFieldSet(this, _NetworkRuntimeWebSocket_socketId, null, "f");
                callHandler(this, 'close', new CloseEvent('close', {
                    code: payload.code ?? 1000,
                    reason: payload.reason ?? '',
                    wasClean: payload.wasClean ?? true,
                }), this.onclose);
                break;
        }
    }
    send(data) {
        if (this.readyState !== NetworkRuntimeWebSocket.OPEN || __classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f") === null) {
            throw new DOMException('WebSocket is not open.', 'InvalidStateError');
        }
        void serializeSocketMessage(data).then((message) => core.invoke(`${PLUGIN}|websocket_send`, {
            request: {
                socketId: __classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f"),
                message,
            },
        }));
    }
    close(code, reason) {
        if (this.readyState === NetworkRuntimeWebSocket.CLOSING || this.readyState === NetworkRuntimeWebSocket.CLOSED) {
            return;
        }
        this.readyState = NetworkRuntimeWebSocket.CLOSING;
        const socketId = __classPrivateFieldGet(this, _NetworkRuntimeWebSocket_socketId, "f");
        if (socketId === null) {
            this.readyState = NetworkRuntimeWebSocket.CLOSED;
            return;
        }
        __classPrivateFieldSet(this, _NetworkRuntimeWebSocket_socketId, null, "f");
        websocketInstances.delete(socketId);
        void core.invoke(`${PLUGIN}|websocket_close`, {
            request: {
                socketId,
                code,
                reason,
            },
        }).catch(() => { });
    }
}
_NetworkRuntimeWebSocket_socketId = new WeakMap();
NetworkRuntimeWebSocket.CONNECTING = 0;
NetworkRuntimeWebSocket.OPEN = 1;
NetworkRuntimeWebSocket.CLOSING = 2;
NetworkRuntimeWebSocket.CLOSED = 3;
class NetworkRuntimeEventSource extends EventTarget {
    constructor(url, init) {
        super();
        this.readyState = NetworkRuntimeEventSource.CONNECTING;
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        _NetworkRuntimeEventSource_sourceId.set(this, null);
        _NetworkRuntimeEventSource_lastEventId.set(this, '');
        this.url = String(url);
        this.withCredentials = Boolean(init?.withCredentials);
        void this.connect(init);
    }
    async connect(init) {
        try {
            if (!shouldInterceptHttpUrl(this.url)) {
                throw new Error(`Network runtime only intercepts http:// and https:// EventSource URLs: ${this.url}`);
            }
            await ensureEventSourceListener();
            const response = await core.invoke(`${PLUGIN}|eventsource_connect`, {
                request: {
                    url: this.url,
                    headers: init?.headers ? entriesFromHeaders(new Headers(init.headers)) : [],
                    withCredentials: this.withCredentials,
                    lastEventId: __classPrivateFieldGet(this, _NetworkRuntimeEventSource_lastEventId, "f") || null,
                    retryMs: null,
                },
            });
            __classPrivateFieldSet(this, _NetworkRuntimeEventSource_sourceId, response.sourceId, "f");
            eventSourceInstances.set(response.sourceId, this);
        }
        catch {
            this.readyState = NetworkRuntimeEventSource.CLOSED;
            callHandler(this, 'error', new Event('error'), this.onerror);
        }
    }
    handlePayload(event) {
        const payload = event.payload;
        if (__classPrivateFieldGet(this, _NetworkRuntimeEventSource_sourceId, "f") !== payload.sourceId) {
            return;
        }
        if (payload.lastEventId) {
            __classPrivateFieldSet(this, _NetworkRuntimeEventSource_lastEventId, payload.lastEventId, "f");
        }
        switch (payload.kind) {
            case 'open':
                this.readyState = NetworkRuntimeEventSource.OPEN;
                callHandler(this, 'open', new Event('open'), this.onopen);
                break;
            case 'message':
            case 'named': {
                const type = payload.kind === 'named' ? payload.event ?? 'message' : 'message';
                const message = new MessageEvent(type, {
                    data: payload.data ?? '',
                    lastEventId: __classPrivateFieldGet(this, _NetworkRuntimeEventSource_lastEventId, "f"),
                    origin: new URL(this.url).origin,
                });
                if (type === 'message') {
                    callHandler(this, 'message', message, this.onmessage);
                }
                else {
                    this.dispatchEvent(message);
                }
                break;
            }
            case 'error':
                this.readyState = NetworkRuntimeEventSource.CONNECTING;
                callHandler(this, 'error', new Event('error'), this.onerror);
                break;
            case 'close':
                this.readyState = NetworkRuntimeEventSource.CLOSED;
                if (__classPrivateFieldGet(this, _NetworkRuntimeEventSource_sourceId, "f") !== null) {
                    eventSourceInstances.delete(__classPrivateFieldGet(this, _NetworkRuntimeEventSource_sourceId, "f"));
                }
                break;
        }
    }
    close() {
        this.readyState = NetworkRuntimeEventSource.CLOSED;
        if (__classPrivateFieldGet(this, _NetworkRuntimeEventSource_sourceId, "f") !== null) {
            const sourceId = __classPrivateFieldGet(this, _NetworkRuntimeEventSource_sourceId, "f");
            eventSourceInstances.delete(sourceId);
            __classPrivateFieldSet(this, _NetworkRuntimeEventSource_sourceId, null, "f");
            void core.invoke(`${PLUGIN}|eventsource_close`, {
                request: { sourceId },
            });
        }
    }
}
_NetworkRuntimeEventSource_sourceId = new WeakMap(), _NetworkRuntimeEventSource_lastEventId = new WeakMap();
NetworkRuntimeEventSource.CONNECTING = 0;
NetworkRuntimeEventSource.OPEN = 1;
NetworkRuntimeEventSource.CLOSED = 2;
class NetworkRuntimeXMLHttpRequest extends EventTarget {
    constructor() {
        super(...arguments);
        this.readyState = NetworkRuntimeXMLHttpRequest.UNSENT;
        this.response = null;
        this.responseText = '';
        this.responseType = '';
        this.responseURL = '';
        this.status = 0;
        this.statusText = '';
        this.timeout = 0;
        this.withCredentials = false;
        this.onreadystatechange = null;
        this.onloadstart = null;
        this.onprogress = null;
        this.onload = null;
        this.onloadend = null;
        this.onabort = null;
        this.onerror = null;
        this.ontimeout = null;
        _NetworkRuntimeXMLHttpRequest_method.set(this, 'GET');
        _NetworkRuntimeXMLHttpRequest_url.set(this, '');
        _NetworkRuntimeXMLHttpRequest_headers.set(this, new Headers());
        _NetworkRuntimeXMLHttpRequest_responseHeaders.set(this, new Headers());
        _NetworkRuntimeXMLHttpRequest_requestId.set(this, null);
    }
    open(method, url, async = true) {
        if (!async) {
            throw new Error('Synchronous XMLHttpRequest is not supported.');
        }
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_method, method.toUpperCase(), "f");
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_url, String(url), "f");
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_headers, new Headers(), "f");
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_responseHeaders, new Headers(), "f");
        this.readyState = NetworkRuntimeXMLHttpRequest.OPENED;
        callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange);
    }
    setRequestHeader(name, value) {
        __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_headers, "f").append(name, value);
    }
    getResponseHeader(name) {
        return __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_responseHeaders, "f").get(name);
    }
    getAllResponseHeaders() {
        return Array.from(__classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_responseHeaders, "f").entries())
            .map(([name, value]) => `${name}: ${value}\r\n`)
            .join('');
    }
    overrideMimeType() { }
    abort() {
        if (!__classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_requestId, "f")) {
            return;
        }
        const requestId = __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_requestId, "f");
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_requestId, null, "f");
        void core.invoke(`${PLUGIN}|abort_http_request`, {
            payload: { requestId },
        });
        this.readyState = NetworkRuntimeXMLHttpRequest.DONE;
        const event = new ProgressEvent('abort');
        callHandler(this, 'abort', event, this.onabort);
        callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend);
    }
    async send(body) {
        if (this.readyState !== NetworkRuntimeXMLHttpRequest.OPENED) {
            throw new DOMException('XMLHttpRequest is not opened.', 'InvalidStateError');
        }
        if (!shouldInterceptHttpUrl(__classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_url, "f"))) {
            throw new Error(`Network runtime only intercepts http:// and https:// XMLHttpRequest URLs: ${__classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_url, "f")}`);
        }
        __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_requestId, crypto.randomUUID(), "f");
        callHandler(this, 'loadstart', new ProgressEvent('loadstart'), this.onloadstart);
        try {
            const request = new Request(__classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_url, "f"), {
                method: __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_method, "f"),
                headers: __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_headers, "f"),
                body: body,
            });
            const response = await invokeHttpRequest({
                url: request.url,
                method: request.method,
                headers: entriesFromHeaders(new Headers(request.headers)),
                body: await bodyDataFromRequest(request),
                timeoutMs: this.timeout || null,
                allowRedirects: true,
                requestId: __classPrivateFieldGet(this, _NetworkRuntimeXMLHttpRequest_requestId, "f"),
            }, null);
            __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_responseHeaders, new Headers(response.headers.map((header) => [header.name, header.value])), "f");
            this.status = response.status;
            this.statusText = response.statusText;
            this.responseURL = response.url;
            this.readyState = NetworkRuntimeXMLHttpRequest.HEADERS_RECEIVED;
            callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange);
            const bytes = decodeBytes(response.body);
            this.readyState = NetworkRuntimeXMLHttpRequest.LOADING;
            callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange);
            callHandler(this, 'progress', new ProgressEvent('progress', { lengthComputable: true, loaded: bytes.byteLength, total: bytes.byteLength }), this.onprogress);
            this.responseText = new TextDecoder().decode(bytes);
            this.response = toXhrResponse(this.responseType, bytes, this.responseText);
            this.readyState = NetworkRuntimeXMLHttpRequest.DONE;
            callHandler(this, 'readystatechange', new Event('readystatechange'), this.onreadystatechange);
            callHandler(this, 'load', new ProgressEvent('load'), this.onload);
            callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend);
        }
        catch (error) {
            this.readyState = NetworkRuntimeXMLHttpRequest.DONE;
            if (error instanceof DOMException && error.name === 'AbortError') {
                callHandler(this, 'abort', new ProgressEvent('abort'), this.onabort);
            }
            else if (String(error).includes('deadline') || String(error).includes('timed out')) {
                callHandler(this, 'timeout', new ProgressEvent('timeout'), this.ontimeout);
            }
            else {
                callHandler(this, 'error', new ProgressEvent('error'), this.onerror);
            }
            callHandler(this, 'loadend', new ProgressEvent('loadend'), this.onloadend);
        }
        finally {
            __classPrivateFieldSet(this, _NetworkRuntimeXMLHttpRequest_requestId, null, "f");
        }
    }
}
_NetworkRuntimeXMLHttpRequest_method = new WeakMap(), _NetworkRuntimeXMLHttpRequest_url = new WeakMap(), _NetworkRuntimeXMLHttpRequest_headers = new WeakMap(), _NetworkRuntimeXMLHttpRequest_responseHeaders = new WeakMap(), _NetworkRuntimeXMLHttpRequest_requestId = new WeakMap();
NetworkRuntimeXMLHttpRequest.UNSENT = 0;
NetworkRuntimeXMLHttpRequest.OPENED = 1;
NetworkRuntimeXMLHttpRequest.HEADERS_RECEIVED = 2;
NetworkRuntimeXMLHttpRequest.LOADING = 3;
NetworkRuntimeXMLHttpRequest.DONE = 4;
function toXhrResponse(type, bytes, text) {
    switch (type) {
        case '':
        case 'text':
            return text;
        case 'json':
            return text ? JSON.parse(text) : null;
        case 'arraybuffer':
            return toArrayBuffer(bytes);
        case 'blob':
            return new Blob([toArrayBuffer(bytes)]);
        default:
            return text;
    }
}
function patchedSendBeacon(url, data) {
    const native = ensureNativeRuntime();
    const targetUrl = String(url);
    if (!shouldInterceptHttpUrl(targetUrl) && native.sendBeacon) {
        return native.sendBeacon(url, data);
    }
    try {
        const request = new Request(targetUrl, {
            method: 'POST',
            body: data ?? null,
        });
        void bodyDataFromRequest(request).then((body) => core.invoke(`${PLUGIN}|send_beacon`, {
            request: {
                url: request.url,
                method: request.method,
                headers: entriesFromHeaders(new Headers(request.headers)),
                body,
                timeoutMs: null,
                allowRedirects: true,
                requestId: null,
            },
        }));
        return true;
    }
    catch {
        if (installedOptions.fallbackToNativeOnError && nativeRuntime?.sendBeacon) {
            return nativeRuntime.sendBeacon(url, data);
        }
        return false;
    }
}
function installNetworkRuntime(options = {}) {
    ensureNativeRuntime();
    installedOptions = options;
    globalThis.fetch = patchedFetch;
    globalThis.WebSocket = NetworkRuntimeWebSocket;
    globalThis.XMLHttpRequest = NetworkRuntimeXMLHttpRequest;
    globalThis.EventSource = NetworkRuntimeEventSource;
    Object.defineProperty(navigator, 'sendBeacon', {
        configurable: true,
        writable: true,
        value: patchedSendBeacon,
    });
}
function restoreNativeNetworkRuntime() {
    const native = ensureNativeRuntime();
    if (native.fetch) {
        globalThis.fetch = native.fetch;
    }
    if (native.WebSocket) {
        globalThis.WebSocket = native.WebSocket;
    }
    if (native.XMLHttpRequest) {
        globalThis.XMLHttpRequest = native.XMLHttpRequest;
    }
    if (native.EventSource) {
        globalThis.EventSource = native.EventSource;
    }
    if (native.sendBeacon) {
        Object.defineProperty(navigator, 'sendBeacon', {
            configurable: true,
            writable: true,
            value: native.sendBeacon,
        });
    }
}

exports.installNetworkRuntime = installNetworkRuntime;
exports.restoreNativeNetworkRuntime = restoreNativeNetworkRuntime;
