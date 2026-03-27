type EventSourceHandler = ((this: NetworkEventSource, ev: Event) => unknown) | null;
export interface NetworkEventSourceInit {
    withCredentials?: boolean;
}
export declare class NetworkEventSource extends EventTarget {
    #private;
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    onerror: EventSourceHandler;
    onmessage: ((this: NetworkEventSource, ev: MessageEvent) => unknown) | null;
    onopen: EventSourceHandler;
    readyState: number;
    url: string;
    withCredentials: boolean;
    lastEventId: string;
    constructor(url: string | URL, init?: NetworkEventSourceInit);
    close(): void;
}
export {};
