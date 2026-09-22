import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 后端端口与 run.py / Dockerfile 的 PORT 默认 9090 保持一致，可用 VITE_BACKEND_PORT 覆盖
const backendPort = process.env.VITE_BACKEND_PORT || '9090'
const backendTarget = `http://127.0.0.1:${backendPort}`

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: backendTarget,
        changeOrigin: true,
      },
      // SocketIO（推送管理页）：websocket 升级必须走 ws 代理
      '/socket.io': {
        target: backendTarget,
        changeOrigin: true,
        ws: true,
      },
    },
  },
})
