import { EventKind, LIMITS, Scenario, ScenarioEvent } from './types';

const EVENT_KINDS: EventKind[] = [
  'read_miss',
  'write_upgrade',
  'message',
  'timeout_retry',
  'ack',
];

export interface ParseResult {
  scenario: Scenario | null;
  errors: string[];
}

function asInt(v: unknown): number | undefined {
  if (typeof v === 'number' && Number.isInteger(v)) return v;
  if (typeof v === 'string' && /^-?\d+$/.test(v.trim())) return parseInt(v, 10);
  return undefined;
}

function asOptInt(v: unknown): number | undefined {
  if (v === null || v === undefined || v === '') return undefined;
  return asInt(v);
}

/**
 * 解析用户导入的 JSON 场景。宽容字段命名（type/kind、cores/coreCount），
 * 严格校验取值，任何错误汇总返回而不是抛出。
 */
export function parseScenario(text: string): ParseResult {
  const errors: string[] = [];
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (e) {
    return { scenario: null, errors: [`JSON 解析失败：${(e as Error).message}`] };
  }
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return { scenario: null, errors: ['顶层必须是对象，例如 {"name":..,"cores":4,"lines":8,"events":[...]}'] };
  }
  const obj = raw as Record<string, unknown>;
  const coreCount = asInt(obj.cores ?? obj.coreCount);
  const lineCount = asInt(obj.lines ?? obj.lineCount);
  if (coreCount === undefined) errors.push('缺少 cores（核心数）字段');
  if (lineCount === undefined) errors.push('缺少 lines（缓存线数）字段');
  if (!Array.isArray(obj.events)) {
    errors.push('events 必须是数组');
    return { scenario: null, errors: errors.length ? errors : ['events 缺失'] };
  }
  const events: ScenarioEvent[] = [];
  const seenIds = new Set<string>();
  (obj.events as unknown[]).forEach((item, idx) => {
    const at = `events[${idx}]`;
    if (typeof item !== 'object' || item === null) {
      errors.push(`${at} 必须是对象`);
      return;
    }
    const e = item as Record<string, unknown>;
    const kind = (e.kind ?? e.type) as EventKind;
    if (!EVENT_KINDS.includes(kind)) {
      errors.push(`${at} 类型非法：${String(e.kind ?? e.type)}（允许 ${EVENT_KINDS.join('/')}）`);
      return;
    }
    let id = typeof e.id === 'string' && e.id.trim() ? e.id.trim() : `evt-${idx + 1}`;
    if (seenIds.has(id)) {
      errors.push(`${at} 事件 id 重复：${id}`);
      return;
    }
    seenIds.add(id);
    const ev: ScenarioEvent = { id, kind };
    const core = asOptInt(e.core);
    const line = asOptInt(e.line);
    const gen = asOptInt(e.gen);
    const to = asOptInt(e.to);
    const from = e.from === null || e.from === 'dir' || e.from === 'directory' ? null : asOptInt(e.from);
    if (core !== undefined) ev.core = core;
    if (line !== undefined) ev.line = line;
    if (gen !== undefined) ev.gen = gen;
    if (to !== undefined) ev.to = to;
    if (from !== undefined) ev.from = from;
    if (typeof e.mid === 'string' && e.mid.trim()) ev.mid = e.mid.trim();
    events.push(ev);
  });
  if (coreCount === undefined || lineCount === undefined || errors.length) {
    return { scenario: null, errors };
  }
  if (coreCount < 1 || coreCount > LIMITS.maxCores) {
    errors.push(`核心数 ${coreCount} 超出允许范围（1..${LIMITS.maxCores}）`);
  }
  if (lineCount < 1 || lineCount > LIMITS.maxLines) {
    errors.push(`缓存线数 ${lineCount} 超出允许范围（1..${LIMITS.maxLines}）`);
  }
  if (events.length === 0) {
    errors.push('事件序列为空');
  }
  if (events.length > LIMITS.maxEvents) {
    errors.push(`事件数 ${events.length} 超过上限 ${LIMITS.maxEvents}`);
  }
  if (errors.length) {
    return { scenario: null, errors };
  }
  return {
    scenario: {
      name: typeof obj.name === 'string' && obj.name.trim() ? obj.name.trim() : '未命名场景',
      coreCount,
      lineCount,
      events,
    },
    errors: [],
  };
}
