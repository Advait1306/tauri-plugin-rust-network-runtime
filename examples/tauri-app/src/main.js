import "./style.css";
import App from "./App.svelte";
import { mount } from 'svelte';
import { installNetworkRuntime } from 'tauri-plugin-rust-network-runtime-api';

const startupClock = performance.now();
console.log(`[startup] main.js loaded at ${startupClock.toFixed(1)}ms`);

const installStart = performance.now();
installNetworkRuntime();
console.log(
  `[startup] installNetworkRuntime completed in ${(performance.now() - installStart).toFixed(1)}ms`,
);

const app = mount(App, {
  target: document.getElementById("app"),
});
console.log(`[startup] Svelte mount call returned at ${performance.now().toFixed(1)}ms`);

export default app;
