export type InstallNetworkRuntimeOptions = {
    fallbackToNativeOnError?: boolean;
};
export declare function installNetworkRuntime(options?: InstallNetworkRuntimeOptions): void;
export declare function restoreNativeNetworkRuntime(): void;
