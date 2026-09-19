// 一键启动开发环境：Flask 后端(:5000) + Vite 前端(--host/--port 可转发)。
// Kimi Work 预览卡片会以 `npm run dev` 调用本脚本，并通过参数指定宿主与端口。
import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const args = process.argv.slice(2)

function argValue(name, fallback) {
  const i = args.indexOf(name)
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback
}

const host = argValue('--host', 'localhost')
const port = argValue('--port', '5173')

const isWin = process.platform === 'win32'
const python = path.join(root, '.venv', isWin ? 'Scripts/python.exe' : 'bin/python')
const npmCmd = isWin ? 'npm.cmd' : 'npm'

console.log(`[dev] backend  -> http://127.0.0.1:5000 (Flask + SQLite)`)
console.log(`[dev] frontend -> http://${host}:${port} (Vite, /api 代理到 5000)`)

const backend = spawn(python, ['run.py'], {
  cwd: root,
  env: { ...process.env, DEBUG: 'False' },
  stdio: 'inherit',
})

const frontend = spawn(npmCmd, ['--prefix', 'frontend', 'run', 'dev', '--', '--host', host, '--port', port], {
  cwd: root,
  env: process.env,
  stdio: 'inherit',
  // Windows 下 .cmd 批处理必须通过 shell 启动（Node 18+ 安全限制）
  shell: isWin,
})

let shuttingDown = false
function shutdown(code = 0) {
  if (shuttingDown) return
  shuttingDown = true
  backend.kill()
  frontend.kill()
  process.exit(code)
}
process.on('SIGINT', () => shutdown(0))
process.on('SIGTERM', () => shutdown(0))
backend.on('exit', (c) => { console.log('[dev] backend exited', c); shutdown(c ?? 1) })
frontend.on('exit', (c) => { console.log('[dev] frontend exited', c); shutdown(c ?? 1) })
