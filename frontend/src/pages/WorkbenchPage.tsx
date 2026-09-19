import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Check, ShieldAlert, X } from 'lucide-react'
import {
  fetchAudit,
  fetchCards,
  fetchConstraints,
  fetchRadar,
  decideCard,
  parseEvidence,
  type AuditRecord,
  type DecisionCard,
  type RadarItem,
} from '../api/workbench'
import { StockLink } from '../components/stock/StockLink'
import { Badge, Card, Delta, KpiCell, PageHeader, SectionTitle } from '../components/ui'
import { EmptyState, ErrorState, Loading } from '../components/StateViews'

const SIGNAL_META: Record<RadarItem['signal'], { label: string; tone: 'danger' | 'warning' | 'accent' | 'bull' | 'neutral' }> = {
  hot: { label: '动量候选', tone: 'danger' },
  watch: { label: '观察', tone: 'warning' },
  oversold: { label: '超跌', tone: 'accent' },
  good: { label: '防御/趋势稳', tone: 'bull' },
  flat: { label: '平', tone: 'neutral' },
}

const STATUS_META: Record<DecisionCard['status'], { label: string; tone: 'warning' | 'bull' | 'bear' | 'accent' }> = {
  pending: { label: '待审批', tone: 'warning' },
  approved: { label: '已批准', tone: 'bull' },
  rejected: { label: '已驳回', tone: 'bear' },
  modified: { label: '已修改', tone: 'accent' },
}

const DIRECTION_META: Record<DecisionCard['direction'], { label: string; tone: 'bull' | 'bear' | 'warning' }> = {
  buy: { label: '买入', tone: 'bull' },
  sell: { label: '卖出', tone: 'bear' },
  reduce: { label: '减仓', tone: 'warning' },
}

function fmt(v: unknown, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return '--'
  return Number(v).toFixed(digits)
}

/** 审批操作区：理由（必填）+ 签字 + 批准/驳回/修改（可改仓位） */
function GatePanel({ card }: { card: DecisionCard }) {
  const [reason, setReason] = useState('')
  const [signer, setSigner] = useState('')
  const [weight, setWeight] = useState(String(card.suggested_weight))
  const queryClient = useQueryClient()

  const mutation = useMutation({
    mutationFn: (payload: { action: 'approve' | 'reject' | 'modify'; reason: string; signer?: string; suggested_weight?: number }) =>
      decideCard(card.id, payload),
    onSuccess: () => {
      setReason('')
      void queryClient.invalidateQueries({ queryKey: ['wb-cards'] })
      void queryClient.invalidateQueries({ queryKey: ['wb-constraints'] })
      void queryClient.invalidateQueries({ queryKey: ['wb-audit'] })
    },
  })

  const submit = (action: 'approve' | 'reject' | 'modify') => {
    if (!reason.trim()) {
      mutation.reset()
      alert('必须填写审批理由（留痕要求）')
      return
    }
    mutation.mutate({
      action,
      reason: reason.trim(),
      signer: signer.trim() || undefined,
      ...(action === 'modify' ? { suggested_weight: Number(weight) || card.suggested_weight } : {}),
    })
  }

  return (
    <div className="mt-3 rounded-md border border-dashed border-accent/60 bg-accent/5 p-3">
      <div className="text-xs font-semibold text-accent">✍️ 人工审批闸门 · 操作自动留痕（audit_trail）</div>
      <div className="mt-2 grid gap-2 sm:grid-cols-[1fr_140px_120px]">
        <input
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          placeholder="审批理由（必填，留痕 20 年）"
          className="rounded border border-line bg-surface px-2 py-1.5 text-xs outline-none focus:border-accent"
        />
        <input
          value={signer}
          onChange={(e) => setSigner(e.target.value)}
          placeholder="签字人"
          className="rounded border border-line bg-surface px-2 py-1.5 text-xs outline-none focus:border-accent"
        />
        <input
          value={weight}
          onChange={(e) => setWeight(e.target.value)}
          type="number"
          step="0.5"
          min="0"
          max="12"
          title="修改时的目标仓位（%）"
          className="rounded border border-line bg-surface px-2 py-1.5 text-xs outline-none focus:border-accent"
        />
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <button
          onClick={() => submit('approve')}
          disabled={mutation.isPending}
          className="inline-flex items-center gap-1 rounded bg-emerald-600/90 px-3 py-1.5 text-xs font-semibold text-white hover:bg-emerald-600 disabled:opacity-50"
        >
          <Check size={13} /> 批准并下达
        </button>
        <button
          onClick={() => submit('modify')}
          disabled={mutation.isPending}
          className="rounded bg-amber-600/90 px-3 py-1.5 text-xs font-semibold text-white hover:bg-amber-600 disabled:opacity-50"
        >
          ✏️ 修改仓位
        </button>
        <button
          onClick={() => submit('reject')}
          disabled={mutation.isPending}
          className="inline-flex items-center gap-1 rounded border border-red-500/60 px-3 py-1.5 text-xs font-semibold text-red-500 hover:bg-red-500/10 disabled:opacity-50"
        >
          <X size={13} /> 驳回
        </button>
        {mutation.isError && <span className="text-xs text-red-500">{(mutation.error as Error).message}</span>}
        {mutation.isSuccess && <span className="text-xs text-emerald-500">已审批并留痕</span>}
      </div>
    </div>
  )
}

function CardRow({ card }: { card: DecisionCard }) {
  const ev = parseEvidence(card.evidence)
  const status = STATUS_META[card.status]
  const dir = DIRECTION_META[card.direction]
  return (
    <Card className="p-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-[11px] text-muted">{card.card_no}</span>
        {card.strategy_tag && <Badge tone="accent">{card.strategy_tag}</Badge>}
        <Badge tone={dir.tone}>{dir.label}</Badge>
        <StockLink code={card.ts_code} name={card.name} />
        <span className="text-xs text-muted">建议仓位 {card.suggested_weight}%</span>
        <span className="ml-auto flex items-center gap-2">
          <span className="text-xs text-muted">AI 置信度 {fmt(card.ai_confidence)}</span>
          <Badge tone={status.tone}>{status.label}</Badge>
        </span>
      </div>
      <div className="mt-2 grid gap-x-6 gap-y-1 text-xs text-muted sm:grid-cols-4">
        <span>收盘 <b className="text-ink">{fmt(ev.close)}</b></span>
        <span>当日 <Delta value={ev.pct_chg as number | null} /></span>
        <span>HV20 <b className="text-ink">{fmt(ev.hv20, 1)}%</b></span>
        <span>20日动量 <b className="text-ink">{fmt(ev.mom20)}%</b></span>
      </div>
      {typeof ev.reasoning === 'string' && ev.reasoning && (
        <p className="mt-1.5 text-xs leading-relaxed text-muted">💡 {ev.reasoning}</p>
      )}
      {card.status !== 'pending' && (
        <p className="mt-1.5 text-xs text-muted">
          {card.decided_at?.slice(0, 19)} · {card.signer || 'operator'}：{card.reason}
          {card.status === 'modified' && <>（调整后仓位 {card.suggested_weight}%）</>}
        </p>
      )}
      {card.status === 'pending' && <GatePanel card={card} />}
    </Card>
  )
}

function RadarTable({ items }: { items: RadarItem[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-line text-left text-muted">
            <th className="py-2 pr-3 font-medium">信号</th>
            <th className="py-2 pr-3 font-medium">标的</th>
            <th className="py-2 pr-3 text-right font-medium">收盘</th>
            <th className="py-2 pr-3 text-right font-medium">当日</th>
            <th className="py-2 pr-3 text-right font-medium">20日动量</th>
            <th className="py-2 pr-3 text-right font-medium">HV20</th>
            <th className="py-2 pr-3 text-right font-medium">距MA20</th>
            <th className="py-2 pr-3 text-right font-medium">PE(TTM)</th>
            <th className="py-2 pr-3 text-right font-medium">成交额</th>
          </tr>
        </thead>
        <tbody>
          {items.map((it) => {
            const sig = SIGNAL_META[it.signal]
            return (
              <tr key={it.ts_code} className="border-b border-line/50 last:border-0 hover:bg-surface">
                <td className="py-2 pr-3"><Badge tone={sig.tone}>{sig.label}</Badge></td>
                <td className="py-2 pr-3"><StockLink code={it.ts_code} name={it.name} /></td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.close)}</td>
                <td className="py-2 pr-3 text-right"><Delta value={it.pct_chg} /></td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.mom20)}%</td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.hv20, 1)}%</td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.ma20_gap)}%</td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.pe_ttm, 1)}</td>
                <td className="py-2 pr-3 text-right font-mono">{fmt(it.amount_yi, 1)}亿</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export default function WorkbenchPage() {
  const radar = useQuery({ queryKey: ['wb-radar'], queryFn: fetchRadar, refetchInterval: 60_000 })
  const cards = useQuery({ queryKey: ['wb-cards'], queryFn: () => fetchCards() })
  const constraints = useQuery({ queryKey: ['wb-constraints'], queryFn: fetchConstraints })
  const audit = useQuery({ queryKey: ['wb-audit'], queryFn: () => fetchAudit() })

  const items = radar.data?.items ?? []
  const allCards = cards.data?.cards ?? []
  const pending = allCards.filter((c) => c.status === 'pending')
  const cons = constraints.data

  return (
    <div className="space-y-4 p-4">
      <PageHeader
        title="人机协同工作台"
        subtitle={
          radar.data?.as_of
            ? `行情截至 ${radar.data.as_of}（Baostock 真实数据）· AI 置信度与策略打分为演示口径 · 审批全程 SQLite 留痕`
            : '等待行情数据…'
        }
      />

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCell label="待审批决策" value={pending.length} sub="AI 生成 · 人工把关" />
        <KpiCell label="已批准仓位" value={`${fmt(cons?.approved_total, 1)}%`} sub={`单票上限 ${fmt(cons?.max_single_weight, 0)}%`} />
        <KpiCell label="股票池" value={items.length} sub="环境变量 WB_POOL 可配置" />
        <KpiCell label="数据日期" value={radar.data?.as_of ?? '--'} sub="每日收盘后同步" />
      </div>

      <div className="grid gap-4 xl:grid-cols-[1fr_380px]">
        <div className="space-y-4">
          <Card className="p-4">
            <SectionTitle className="mb-2" title="📡 市场雷达 · 股票池" />
            {radar.isLoading ? <Loading /> : radar.isError ? <ErrorState message={radar.error instanceof Error ? radar.error.message : '雷达加载失败'} /> : <RadarTable items={items} />}
          </Card>

          <div className="space-y-3">
            <SectionTitle title="🎯 决策卡队列（批准/驳回/修改均留痕）" />
            {cards.isLoading && <Loading />}
            {allCards.length === 0 && <EmptyState text="暂无决策卡 · AI 策略中枢生成信号后会出现在这里" icon="🎯" />}
            {allCards.map((c) => <CardRow key={c.id} card={c} />)}
          </div>
        </div>

        <div className="space-y-4">
          <Card className="p-4">
            <SectionTitle
              className="mb-2"
              title={
                <span className="flex items-center gap-1.5">
                  <ShieldAlert size={14} className="text-amber-500" /> P0 硬约束（A股口径）
                </span>
              }
            />
            <ul className="space-y-1.5 text-xs">
              {(cons?.rules ?? []).map((r) => (
                <li key={r.rule} className="flex items-start justify-between gap-2 border-b border-line/40 pb-1.5 last:border-0">
                  <span className={r.level === 'red' ? 'text-red-500' : 'text-amber-500'}>{r.rule}</span>
                  <span className="shrink-0 text-muted">{r.action}</span>
                </li>
              ))}
            </ul>
            {cons && Object.keys(cons.approved_exposure).length > 0 && (
              <div className="mt-3 text-xs text-muted">
                已批准仓位分布：
                {Object.entries(cons.approved_exposure).map(([code, w]) => (
                  <div key={code} className="flex justify-between">
                    <StockLink code={code} />
                    <span className="font-mono">{fmt(w, 1)}%</span>
                  </div>
                ))}
              </div>
            )}
          </Card>

          <Card className="p-4">
            <SectionTitle className="mb-2" title="🕐 审计留痕（audit_trail）" />
            {audit.isLoading ? <Loading /> : (
              <ul className="space-y-1.5 text-xs text-muted">
                {(audit.data?.records ?? []).map((r: AuditRecord) => (
                  <li key={r.id} className="border-b border-line/40 pb-1.5 last:border-0">
                    <span className="font-mono">{r.card_no}</span>{' '}
                    <Badge tone={r.action === 'approve' ? 'bull' : r.action === 'reject' ? 'bear' : r.action === 'create' ? 'neutral' : 'accent'}>
                      {r.action}
                    </Badge>{' '}
                    <span>{r.operator}</span>
                    <div className="mt-0.5 opacity-70">{r.created_at?.slice(0, 19)}</div>
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </div>
      </div>
    </div>
  )
}
