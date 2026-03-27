export declare const COMMAND_PREFIX = "plugin:rust-network-runtime|";
export declare const websocketEventName: (socketId: number) => string;
export declare const eventSourceEventName: (sourceId: number) => string;
export interface HeaderEntry {
    name: string;
    value: string;
}
export type BodyData = {
    kind: 'text';
    value: string;
    contentType?: string;
} | {
    kind: 'bytes';
    value: number[];
    contentType?: string;
} | {
    kind: 'json';
    value: unknown;
    contentType?: string;
};
export interface HttpRequestPayload {
    method: string;
    url: string;
    headers: HeaderEntry[];
    body?: BodyData;
    timeoutMs?: number;
    allowRedirects?: boolean;
}
export interface HttpResponsePayload {
    status: number;
    statusText: string;
    url: string;
    headers: HeaderEntry[];
    body: number[];
    redirected: boolean;
}
export interface WebSocketConnectRequestPayload {
    url: string;
    protocols: string[];
    headers: HeaderEntry[];
}
export interface WebSocketConnectResponsePayload {
    socketId: number;
    protocol?: string | null;
}
export interface WebSocketSendRequestPayload {
    socketId: number;
    message: {
        kind: 'text';
        value: string;
    } | {
        kind: 'binary';
        value: number[];
    } | {
        kind: 'close';
        code?: number;
        reason?: string;
    };
}
export interface WebSocketEventPayload {
    socketId: number;
    kind: 'open' | 'message' | 'error' | 'close';
    data?: number[] | null;
    text?: string | null;
    code?: number | null;
    reason?: string | null;
    wasClean?: boolean | null;
    protocol?: string | null;
}
export interface EventSourceConnectRequestPayload {
    url: string;
    headers: HeaderEntry[];
    withCredentials?: boolean;
    lastEventId?: string | null;
    retryMs?: number | null;
}
export interface EventSourceConnectResponsePayload {
    sourceId: number;
}
export interface EventSourceEventPayload {
    sourceId: number;
    kind: 'open' | 'message' | 'error' | 'close' | 'named';
    event?: string | null;
    data?: string | null;
    lastEventId?: string | null;
    retryMs?: number | null;
}
export interface NetworkRuntimeOptions {
    debug?: boolean;
}
export interface NativeNetworkRuntime {
    fetch: typeof globalThis.fetch;
    WebSocket: typeof globalThis.WebSocket;
    XMLHttpRequest: typeof globalThis.XMLHttpRequest;
    EventSource: typeof globalThis.EventSource;
    sendBeacon: Navigator['sendBeacon'];
}
export declare function setNativeRuntime(native: NativeNetworkRuntime): void;
export declare function getNativeRuntime(): NativeNetworkRuntime;
export declare function registerCleanup(fn: () => void): () => void;
export declare function restoreNativeRuntime(): void;
export declare function isInstalled(): boolean;
export declare function markInstalled(value: boolean): void;
export declare function createAbortError(): Error;
export declare function toHeaderEntries(headers?: HeadersInit): HeaderEntry[];
export declare function toHeaders(entries: HeaderEntry[]): Headers;
export declare function toUint8Array(value: unknown): Uint8Array;
export declare function uint8ArrayToNumberArray(bytes: Uint8Array): number[];
export declare function bytesFromNumberArray(bytes: number[]): Uint8Array;
export declare function textFromBytes(bytes: Uint8Array): string;
export declare function bytesFromText(text: string): Uint8Array;
export declare function normalizeBody(body: unknown): Promise<BodyData | undefined>;
export declare function normalizeRequestBody(body: unknown, headers: Headers): Promise<BodyData | undefined>;
export declare function appendContentTypeHeader(headers: Headers, contentType?: string): void;
export declare function invokeHttpRequest(request: HttpRequestPayload): Promise<HttpResponsePayload>;
export declare function invokeWebSocketConnect(request: WebSocketConnectRequestPayload): Promise<WebSocketConnectResponsePayload>;
export declare function invokeWebSocketSend(request: WebSocketSendRequestPayload): Promise<void>;
export declare function invokeWebSocketClose(socketId: number, code?: number, reason?: string): Promise<void>;
export declare function invokeEventSourceConnect(request: EventSourceConnectRequestPayload): Promise<EventSourceConnectResponsePayload>;
export declare function invokeEventSourceClose(sourceId: number): Promise<void>;
export declare function waitForAbort(signal?: AbortSignal): Promise<never>;
export declare function raceWithAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T>;
