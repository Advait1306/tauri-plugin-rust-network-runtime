type WebSocketHandler = ((this: NetworkWebSocket, ev: Event) => unknown) | null;
export declare class NetworkWebSocket extends EventTarget {
    #private;
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    binaryType: BinaryType;
    bufferedAmount: number;
    extensions: string;
    onclose: ((this: NetworkWebSocket, ev: CloseEvent) => unknown) | null;
    onerror: WebSocketHandler;
    onmessage: ((this: NetworkWebSocket, ev: MessageEvent) => unknown) | null;
    onopen: WebSocketHandler;
    protocol: string;
    readyState: number;
    constructor(url: string | URL, protocols?: string | string[]);
    send(data: string | Blob | ArrayBuffer | ArrayBufferView): void;
    close(code?: number, reason?: string): void;
}
export {};
