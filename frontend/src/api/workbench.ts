// ================= 人机协同工作台 /api/workbench（信封 {code,message,data}） =================
import { apiGet, apiPost } from './client'

export interface RadarItem {
  ts_code: string
  name: string
  industry: string
  close: number
  pct_chg: number
  ma20_gap: number | null
  hv20: number | null
  mom20: number | null
  amount_yi: number
  pe_ttm: number | null
  pb: number | null
  signal: 'hot' | 'watch' | 'oversold' | 'good' | 'flat'
}

export interface RadarData {
  as_of: string | null
  items: RadarItem[]
}

export interface DecisionCard {
  id: number
  card_no: string
  ts_code: string
  name: string
  direction: 'buy' | 'sell' | 'reduce'
  strategy_tag: string
  suggested_weight: number
  evidence: string // JSON 字符串
  ai_confidence: number
  status: 'pending' | 'approved' | 'rejected' | 'modified'
  reason: string
  signer: string
  created_at: string | null
  decided_at: string | null
}

export interface AuditRecord {
  id: number
  card_no: string
  action: string
  detail: string
  operator: string
  created_at: string | null
}

export interface Constraints {
  max_single_weight: number
  rules: { rule: string; action: string; level: 'red' | 'orange' }[]
  approved_exposure: Record<string, number>
  approved_total: number
}

export function fetchRadar(): Promise<RadarData> {
  return apiGet<RadarData>('/workbench/radar')
}

export function fetchCards(status?: string): Promise<{ cards: DecisionCard[] }> {
  return apiGet<{ cards: DecisionCard[] }>('/workbench/cards', status ? { status } : undefined)
}

export function createCard(payload: {
  ts_code: string
  direction?: string
  suggested_weight: number
  strategy_tag?: string
  ai_confidence?: number
  reasoning?: string
}): Promise<DecisionCard> {
  return apiPost<DecisionCard>('/workbench/cards', payload)
}

export function decideCard(
  id: number,
  payload: { action: 'approve' | 'reject' | 'modify'; reason: string; signer?: string; suggested_weight?: number },
): Promise<DecisionCard> {
  return apiPost<DecisionCard>(`/workbench/cards/${id}/decide`, payload)
}

export function fetchAudit(cardNo?: string): Promise<{ records: AuditRecord[] }> {
  return apiGet<{ records: AuditRecord[] }>('/workbench/audit', cardNo ? { card_no: cardNo } : undefined)
}

export function fetchConstraints(): Promise<Constraints> {
  return apiGet<Constraints>('/workbench/constraints')
}

/** 解析证据 JSON（后端存的是 JSON 字符串，异常时降级为空对象） */
export function parseEvidence(raw: string): Record<string, unknown> {
  try {
    return JSON.parse(raw) as Record<string, unknown>
  } catch {
    return {}
  }
}
