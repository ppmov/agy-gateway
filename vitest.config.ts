import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // src/patch: the CLIProxyAPI-port suites there run under `node --test`
    // (node:test), so only the vitest-shaped Phase-3 tool-bridge suite is
    // picked up here — widening this glob re-collects the node:test files and
    // fails them at the runner level.
    include: ['test/**/*.test.ts', 'src/patch/tool-bridge.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Sequential execution: spawn tests share the fake-agy argv record file
    // pattern and process-tree kill assertions are timing sensitive.
    fileParallelism: false,
  },
})
