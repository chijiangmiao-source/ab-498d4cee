import {
  CacheState,
  DirLineSnapshot,
  LIMITS,
  MessageRecord,
  ReplayResult,
  RunHooks,
  Scenario,
  ScenarioEvent,
  StepSnapshot,
  Violation,
} from './types';
import { checkDirCacheInvariant } from './checker';

interface DirLine {
  owner: number | null;
  sharers: Set<number>;
  memoryValid: boolean;
  gen: number;
  closedGens: number[];
  pendingRead: {
    requester: number;
    needFrom: number | null;
    fetchMid: string | null;
    dataMid: string | null;
  } | null;
  pendingWrite: {
    gen: number;
    requester: number;
    waitSet: number[];
    invDelivered: number[];
    acked: number[];
    invMids: Record<number, string>;
    ackMids: Record<number, string>;
  } | null;
}

/**
 * 确定性消息号：e{事件序号}-{类型}-{目标键}。
 * 用户场景可用 "@AUTO:<事件id>:<data|fetch|inv|invack>:<键>" 引用引擎在该步签发的消息。
 */
function nextMid(step: number, type: string, key: string | number): string {
  return `e${step + 1}-${type}-${key}`;
}

const AUTO_TYPE_MAP: Record<string, string> = {
  data: 'data',
  fetch: 'fetch',
  inv: 'inv',
  invack: 'invack',
};

/**
 * 重放引擎：按事件顺序执行；任一步产生违约即冻结，后续事件不再执行。
 * 纯同步、无 DOM 依赖，可运行于 Web Worker 与 Node。
 */
export class ReplayEngine {
  private scenario: Scenario;
  private eventIndex = new Map<string, number>();
  private dir: DirLine[];
  private caches: CacheState[][];
  private registry = new Map<string, MessageRecord>();
  private steps: StepSnapshot[] = [];
  private frozen = false;
  private violation: Violation | null = null;
  private frozenAtStep: number | null = null;

  constructor(scenario: Scenario) {
    this.scenario = scenario;
    scenario.events.forEach((e, i) => this.eventIndex.set(e.id, i));
    const c = scenario.coreCount;
    this.dir = Array.from({ length: scenario.lineCount }, () => ({
      owner: null,
      sharers: new Set<number>(),
      memoryValid: true,
      gen: 0,
      closedGens: [] as number[],
      pendingRead: null,
      pendingWrite: null,
    }));
    this.caches = Array.from({ length: c }, () =>
      Array.from({ length: scenario.lineCount }, () => 'I' as CacheState),
    );
  }

  async run(hooks: RunHooks = {}): Promise<ReplayResult> {
    const limitViolation = validateLimits(this.scenario);
    if (limitViolation) {
      this.frozen = true;
      this.violation = limitViolation;
      this.frozenAtStep = 0;
      return this.buildResult(false, limitViolation);
    }
    const total = this.scenario.events.length;
    for (let i = 0; i < total; i++) {
      if (hooks.isCancelled?.()) {
        return this.buildResult(true);
      }
      const stop = this.executeOne(i);
      hooks.onStep?.(i, total);
      if (hooks.yieldBetweenSteps) {
        await new Promise<void>((resolveStep) => setTimeout(resolveStep, 0));
      }
      if (stop) break;
    }
    return this.buildResult(false);
  }

  /** 同步执行入口（供 Node 测试与脚本使用） */
  runSync(): ReplayResult {
    const limitViolation = validateLimits(this.scenario);
    if (limitViolation) {
      this.frozen = true;
      this.violation = limitViolation;
      this.frozenAtStep = 0;
      return this.buildResult(false, limitViolation);
    }
    for (let i = 0; i < this.scenario.events.length; i++) {
      if (this.executeOne(i)) break;
    }
    return this.buildResult(false);
  }

  /** 执行单步；返回 true 表示应停止（违约冻结） */
  private executeOne(i: number): boolean {
    const ev = this.scenario.events[i];
    let description: string;
    try {
      description = this.apply(ev, i);
    } catch (e) {
      if ((e as ViolationSignal).v) {
        this.freeze(i, ev, (e as ViolationSignal).v);
        return true;
      }
      throw e;
    }
    const snap = this.snapshot(i, ev, description, null);
    const violation = checkDirCacheInvariant(
      snap.directory,
      snap.caches,
      this.scenario.coreCount,
      this.scenario.lineCount,
      { step: i + 1, eventId: ev.id, mid: ev.mid },
    );
    snap.violation = violation;
    this.steps.push(snap);
    if (violation) {
      this.frozen = true;
      this.violation = violation;
      this.frozenAtStep = i;
      snap.frozen = true;
      return true;
    }
    return false;
  }

  private buildResult(cancelled: boolean, preflightViolation?: Violation): ReplayResult {
    const finalSnapshot = this.steps[this.steps.length - 1] ?? null;
    return {
      scenario: this.scenario.name,
      coreCount: this.scenario.coreCount,
      lineCount: this.scenario.lineCount,
      steps: this.steps,
      frozen: this.frozen,
      violation: preflightViolation ?? this.violation,
      frozenAtStep: this.frozenAtStep,
      finalSnapshot,
      cancelled,
    };
  }

  // ---- 违约辅助 ----

  private fail(v: Omit<Violation, never>): never {
    throw { v } as ViolationSignal;
  }

  private freeze(step: number, ev: ScenarioEvent, v: Violation) {
    const snap = this.snapshot(step, ev, `违约冻结：${v.message}`, v);
    snap.frozen = true;
    this.steps.push(snap);
    this.frozen = true;
    this.violation = v;
    this.frozenAtStep = step;
  }

  // ---- 事件分派 ----

  private apply(ev: ScenarioEvent, step: number): string {
    switch (ev.kind) {
      case 'read_miss':
        return this.applyReadMiss(ev, step);
      case 'write_upgrade':
        return this.applyWriteUpgrade(ev, step);
      case 'message':
        return this.applyMessage(ev, step);
      case 'timeout_retry':
        return this.applyRetry(ev, step);
      case 'ack':
        return this.applyAck(ev, step);
      default:
        this.fail({
          code: 'BAD_EVENT',
          message: `未知事件类型 ${(ev as { kind: string }).kind}`,
          line: ev.line ?? null,
          cores: ev.core != null ? [ev.core] : [],
          mids: [],
          gen: null,
        });
    }
  }

  private requireCore(core: number | undefined, ev: ScenarioEvent): number {
    if (core == null || !Number.isInteger(core)) {
      this.fail({
        code: 'BAD_EVENT',
        message: `事件 ${ev.id} 缺少合法 core 字段`,
        line: ev.line ?? null,
        cores: [],
        mids: [],
        gen: null,
      });
    }
    if (core < 0 || core >= this.scenario.coreCount) {
      this.fail({
        code: 'BAD_EVENT',
        message: `事件 ${ev.id} 的 core=${core} 超出核心数 ${this.scenario.coreCount}`,
        line: ev.line ?? null,
        cores: [core],
        mids: [],
        gen: null,
      });
    }
    return core;
  }

  private requireLine(ev: ScenarioEvent): number {
    const line = ev.line;
    if (line == null || !Number.isInteger(line)) {
      this.fail({
        code: 'BAD_EVENT',
        message: `事件 ${ev.id} 缺少合法 line 字段`,
        line: null,
        cores: ev.core != null ? [ev.core] : [],
        mids: ev.mid ? [ev.mid] : [],
        gen: ev.gen ?? null,
      });
    }
    if (line < 0 || line >= this.scenario.lineCount) {
      this.fail({
        code: 'BAD_EVENT',
        message: `事件 ${ev.id} 的 line=${line} 超出缓存线数 ${this.scenario.lineCount}`,
        line,
        cores: ev.core != null ? [ev.core] : [],
        mids: ev.mid ? [ev.mid] : [],
        gen: ev.gen ?? null,
      });
    }
    return line;
  }

  /** 解析 "@AUTO:<事件id>:<类型>:<键>" 符号引用为确定性消息号 */
  private resolveMid(ref: string, ev: ScenarioEvent): string {
    const m = /^@AUTO:([^:]+):(data|fetch|inv|invack):(-?\d+)$/.exec(ref);
    if (!m) return ref;
    const [, srcId, t, keyRaw] = m;
    const srcStep = this.eventIndex.get(srcId);
    if (srcStep === undefined) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `事件 ${ev.id} 引用的消息来源事件 ${srcId} 不存在`,
        line: ev.line ?? null,
        cores: ev.to != null ? [ev.to] : [],
        mids: [ref],
        gen: ev.gen ?? null,
      });
    }
    if (srcStep > (this.steps.length ? this.steps[this.steps.length - 1].index : -1)) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `事件 ${ev.id} 引用了尚未发生的事件 ${srcId} 所签发的消息`,
        line: ev.line ?? null,
        cores: ev.to != null ? [ev.to] : [],
        mids: [ref],
        gen: ev.gen ?? null,
      });
    }
    return `e${srcStep + 1}-${AUTO_TYPE_MAP[t]}-${keyRaw}`;
  }

  private emit(
    mid: string,
    type: MessageRecord['type'],
    line: number,
    from: number | null,
    to: number,
    step: number,
    gen?: number,
  ) {
    this.registry.set(mid, {
      mid,
      type,
      line,
      from,
      to,
      gen,
      sentAt: step,
      deliveredAt: null,
      retries: 0,
    });
  }

  private applyReadMiss(ev: ScenarioEvent, step: number): string {
    const c = this.requireCore(ev.core, ev);
    const L = this.requireLine(ev);
    if (this.caches[c][L] !== 'I') {
      this.fail({
        code: 'BAD_EVENT',
        message: `核 ${c} 在线 ${L} 已持有 ${this.caches[c][L]} 副本，read_miss 属于重复/非法请求`,
        line: L,
        cores: [c],
        mids: [],
        gen: null,
      });
    }
    const d = this.dir[L];
    if (d.pendingWrite) {
      this.fail({
        code: 'BAD_EVENT',
        message: `线 ${L} 存在进行中的写失效世代 ${d.pendingWrite.gen}，读缺失必须等待其闭合`,
        line: L,
        cores: [c, d.pendingWrite.requester],
        mids: [],
        gen: d.pendingWrite.gen,
      });
    }
    if (d.pendingRead) {
      this.fail({
        code: 'BAD_EVENT',
        message: `线 ${L} 已有未闭合读缺失（请求核 ${d.pendingRead.requester}），不得重复发起`,
        line: L,
        cores: [c, d.pendingRead.requester],
        mids: [],
        gen: null,
      });
    }
    if (d.owner !== null) {
      const O = d.owner;
      if (this.caches[O][L] !== 'M') {
        this.fail({
          code: 'DIR_COPY_MISMATCH',
          message: `目录记录线 ${L} 的独占拥有者为核 ${O}，但其副本并非 M`,
          line: L,
          cores: [O, c],
          mids: [],
          gen: null,
        });
      }
      const fetchMid = nextMid(step, 'fetch', O);
      this.emit(fetchMid, 'fetch', L, null, O, step);
      d.pendingRead = {
        requester: c,
        needFrom: O,
        fetchMid,
        dataMid: null,
      };
      return `核 ${c} 对线 ${L} 读缺失：目录向独占拥有者核 ${O} 下发 fetch（消息 ${fetchMid}），等待拥有者下放数据`;
    }
    const dataMid = nextMid(step, 'data', c);
    this.emit(dataMid, 'data', L, null, c, step);
    d.pendingRead = { requester: c, needFrom: null, fetchMid: null, dataMid };
    return `核 ${c} 对线 ${L} 读缺失：无独占拥有者，目录/内存提供数据（消息 ${dataMid} 在途）`;
  }

  private applyWriteUpgrade(ev: ScenarioEvent, step: number): string {
    const c = this.requireCore(ev.core, ev);
    const L = this.requireLine(ev);
    const d = this.dir[L];
    if (this.caches[c][L] === 'I') {
      this.fail({
        code: 'BAD_EVENT',
        message: `核 ${c} 在线 ${L} 无有效副本，write_upgrade 前必须先 read_miss`,
        line: L,
        cores: [c],
        mids: [],
        gen: null,
      });
    }
    if (this.caches[c][L] === 'M' && d.owner === c) {
      this.fail({
        code: 'BAD_EVENT',
        message: `核 ${c} 已是线 ${L} 的独占拥有者，write_upgrade 重复`,
        line: L,
        cores: [c],
        mids: [],
        gen: d.gen,
      });
    }
    if (d.pendingWrite) {
      this.fail({
        code: 'BAD_EVENT',
        message: `线 ${L} 已有进行中的写失效世代 ${d.pendingWrite.gen}，新 write_upgrade 必须等待`,
        line: L,
        cores: [c, d.pendingWrite.requester],
        mids: [],
        gen: d.pendingWrite.gen,
      });
    }
    if (d.pendingRead) {
      this.fail({
        code: 'BAD_EVENT',
        message: `线 ${L} 存在未闭合读缺失（请求核 ${d.pendingRead.requester}），暂不接受 write_upgrade`,
        line: L,
        cores: [c, d.pendingRead.requester],
        mids: [],
        gen: null,
      });
    }
    d.gen += 1;
    const gen = d.gen;
    const waitSet = this.allCores().filter(
      (x) => x !== c && this.caches[x][L] !== 'I',
    );
    const invMids: Record<number, string> = {};
    for (const x of waitSet) {
      const mid = nextMid(step, 'inv', x);
      invMids[x] = mid;
      this.emit(mid, 'invalidation', L, null, x, step, gen);
    }
    d.pendingWrite = {
      gen,
      requester: c,
      waitSet,
      invDelivered: [],
      acked: [],
      invMids,
      ackMids: {},
    };
    if (waitSet.length === 0) {
      this.closeWrite(L, `核 ${c} 对线 ${L} 的 write_upgrade：等待集合为空，世代 ${gen} 直接闭合，核 ${c} 获得独占权限`);
      return `核 ${c} 对线 ${L} 发起 write_upgrade，目录签发世代 ${gen}；等待集合为空，直接闭合，核 ${c} 成为独占拥有者`;
    }
    return `核 ${c} 对线 ${L} 发起 write_upgrade：目录签发绑定世代 ${gen} 的失效请求，等待集合 {${waitSet.join(
      ', ',
    )}}（消息 ${waitSet.map((x) => invMids[x]).join('、')}）`;
  }

  private closeWrite(L: number, _description: string) {
    const d = this.dir[L];
    const pw = d.pendingWrite!;
    this.caches[pw.requester][L] = 'M';
    d.owner = pw.requester;
    d.sharers = new Set<number>([pw.requester]);
    d.memoryValid = false;
    d.closedGens.push(pw.gen);
    d.pendingWrite = null;
  }

  private applyMessage(ev: ScenarioEvent, step: number): string {
    let mid = ev.mid;
    if (!mid) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `事件 ${ev.id} 是 message 投递但缺少 mid`,
        line: ev.line ?? null,
        cores: ev.core != null ? [ev.core] : [],
        mids: [],
        gen: ev.gen ?? null,
      });
    }
    mid = this.resolveMid(mid, ev);
    const rec = this.registry.get(mid);
    if (!rec) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `在途登记中不存在消息 ${mid}，无法投递`,
        line: ev.line ?? null,
        cores: ev.to != null ? [ev.to] : [],
        mids: [mid],
        gen: ev.gen ?? null,
      });
    }
    if (ev.to != null && ev.to !== rec.to) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `消息 ${mid} 的接收方声明为核 ${ev.to}，登记接收方为核 ${rec.to}`,
        line: rec.line,
        cores: [ev.to, rec.to],
        mids: [mid],
        gen: rec.gen ?? null,
      });
    }
    if (ev.from !== undefined && ev.from !== rec.from && rec.type !== 'data') {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `消息 ${mid} 的发送方声明为 ${ev.from === null ? '目录' : `核 ${ev.from}`}，登记发送方为 ${
          rec.from === null ? '目录' : `核 ${rec.from}`
        }`,
        line: rec.line,
        cores: [rec.to],
        mids: [mid],
        gen: rec.gen ?? null,
      });
    }
    if (ev.gen != null && ev.gen !== rec.gen) {
      this.fail({
        code: 'UNKNOWN_MESSAGE',
        message: `消息 ${mid} 的世代声明为 ${ev.gen}，登记世代为 ${rec.gen}`,
        line: rec.line,
        cores: [rec.to],
        mids: [mid],
        gen: ev.gen,
      });
    }
    if (rec.deliveredAt !== null) {
      // 重复投递：只能回放既有动作，绝不产生新状态
      return `重复投递消息 ${mid}（${rec.type}，线 ${rec.line}）：仅回放既有送达动作，状态与世代不变（幂等）`;
    }
    rec.deliveredAt = step;
    switch (rec.type) {
      case 'fetch':
        return this.deliverFetch(rec, step);
      case 'data':
        return this.deliverData(rec, ev.from);
      case 'invalidation':
        return this.deliverInvalidation(rec, step);
      case 'invack':
        return this.handleAck(rec.from ?? -1, rec.line, rec.gen ?? null, rec.mid, true);
      default:
        this.fail({
          code: 'UNKNOWN_MESSAGE',
          message: `消息 ${mid} 类型 ${(rec as { type: string }).type} 不受支持`,
          line: rec.line,
          cores: [rec.to],
          mids: [mid],
          gen: rec.gen ?? null,
        });
    }
  }

  private deliverFetch(rec: MessageRecord, step: number): string {
    const L = rec.line;
    const d = this.dir[L];
    const pr = d.pendingRead;
    if (!pr || pr.fetchMid !== rec.mid) {
      this.fail({
        code: 'BAD_EVENT',
        message: `fetch 消息 ${rec.mid} 到达，但线 ${L} 没有等待它的读缺失`,
        line: L,
        cores: [rec.to],
        mids: [rec.mid],
        gen: null,
      });
    }
    const O = pr.needFrom;
    if (O === null || rec.to !== O) {
      this.fail({
        code: 'BAD_EVENT',
        message: `fetch 消息 ${rec.mid} 应送达独占拥有者核 ${O}，实际送达核 ${rec.to}`,
        line: L,
        cores: [rec.to, O ?? -1].filter((x) => x >= 0),
        mids: [rec.mid],
        gen: null,
      });
    }
    if (this.caches[O][L] !== 'M') {
      this.fail({
        code: 'DIR_COPY_MISMATCH',
        message: `fetch 到达时核 ${O} 在线 ${L} 已不是 M 独占拥有者`,
        line: L,
        cores: [O, pr.requester],
        mids: [rec.mid],
        gen: null,
      });
    }
    // 拥有者在总线上下放数据并降级为共享，内存同时被写回
    this.caches[O][L] = 'S';
    d.owner = null;
    d.memoryValid = true;
    d.sharers.add(O);
    const dataMid = nextMid(step, 'data', pr.requester);
    this.emit(dataMid, 'data', L, O, pr.requester, step);
    pr.dataMid = dataMid;
    return `fetch 送达核 ${O}：拥有者对线 ${L} 由 M 降级为 S 并下放数据（消息 ${dataMid} 发往核 ${pr.requester}），目录撤销其独占身份`;
  }

  private deliverData(rec: MessageRecord, claimedFrom?: number | null): string {
    const L = rec.line;
    const d = this.dir[L];
    const pr = d.pendingRead;
    if (!pr || pr.dataMid !== rec.mid) {
      this.fail({
        code: 'BAD_EVENT',
        message: `data 消息 ${rec.mid} 到达，但线 ${L} 没有等待该数据的读缺失`,
        line: L,
        cores: [rec.to],
        mids: [rec.mid],
        gen: null,
      });
    }
    const c = pr.requester;
    if (rec.to !== c) {
      this.fail({
        code: 'BAD_EVENT',
        message: `data 消息 ${rec.mid} 应送达请求核 ${c}，实际接收方为核 ${rec.to}`,
        line: L,
        cores: [c, rec.to],
        mids: [rec.mid],
        gen: null,
      });
    }
    // 投递事件可声明数据源；与登记来源不符即数据源篡改（拥有者数据缺失）
    const actualFrom = claimedFrom !== undefined ? claimedFrom : rec.from;
    if (pr.needFrom !== null) {
      if (actualFrom !== pr.needFrom) {
        this.fail({
          code: 'MISSING_OWNER_DATA',
          message: `线 ${L} 的读缺失要求独占拥有者核 ${pr.needFrom} 下放数据，但 data ${rec.mid} 实际来自 ${
            actualFrom === null ? '内存/目录' : `核 ${actualFrom}`
          }；拥有者数据缺失，读缺失不得闭合`,
          line: L,
          cores: [pr.needFrom, c],
          mids: [rec.mid],
          gen: null,
        });
      }
      if (this.caches[pr.needFrom][L] !== 'S') {
        this.fail({
          code: 'DIR_COPY_MISMATCH',
          message: `拥有者核 ${pr.needFrom} 在线 ${L} 应已随 fetch 降级为 S，但副本状态为 ${this.caches[pr.needFrom][L]}`,
          line: L,
          cores: [pr.needFrom, c],
          mids: [rec.mid],
          gen: null,
        });
      }
    } else if (actualFrom !== null) {
      this.fail({
        code: 'MISSING_OWNER_DATA',
        message: `线 ${L} 无独占拥有者，数据应由内存/目录提供，但 data ${rec.mid} 来自核 ${actualFrom}`,
        line: L,
        cores: [actualFrom, c],
        mids: [rec.mid],
        gen: null,
      });
    }
    if (this.caches[c][L] !== 'I') {
      this.fail({
        code: 'DIR_COPY_MISMATCH',
        message: `数据到达时请求核 ${c} 在线 ${L} 的副本不是 I（${this.caches[c][L]}）`,
        line: L,
        cores: [c],
        mids: [rec.mid],
        gen: null,
      });
    }
    this.caches[c][L] = 'S';
    d.sharers.add(c);
    if (pr.needFrom !== null) d.sharers.add(pr.needFrom);
    const fromDesc = actualFrom === null ? '内存/目录' : `拥有者核 ${actualFrom}`;
    d.pendingRead = null;
    return `data 由${fromDesc}送达核 ${c}：线 ${L} 读缺失闭合，核 ${c} 副本置 S，目录共享集合更新为 {${[
      ...d.sharers,
    ].join(', ')}}`;
  }

  private deliverInvalidation(rec: MessageRecord, step: number): string {
    const L = rec.line;
    const d = this.dir[L];
    const pw = d.pendingWrite;
    if (!pw || rec.gen !== pw.gen) {
      this.fail({
        code: 'STALE_INVALIDATION',
        message: `失效请求 ${rec.mid} 绑定世代 ${rec.gen}，但线 ${L} 当前等待集合属于世代 ${
          pw ? pw.gen : '(无进行中写入)'
        }；旧世代失效不得影响新一轮写入`,
        line: L,
        cores: [rec.to],
        mids: [rec.mid],
        gen: rec.gen ?? null,
      });
    }
    const x = rec.to;
    if (!pw.waitSet.includes(x)) {
      this.fail({
        code: 'STALE_INVALIDATION',
        message: `核 ${x} 不在线 ${L} 世代 ${pw.gen} 的等待集合 {${pw.waitSet.join(', ')}} 中`,
        line: L,
        cores: [x, pw.requester],
        mids: [rec.mid],
        gen: pw.gen,
      });
    }
    if (pw.invDelivered.includes(x)) {
      // 同一失效记录只会投递一次；走到这里说明登记异常
      this.fail({
        code: 'STALE_INVALIDATION',
        message: `线 ${L} 世代 ${pw.gen} 对核 ${x} 的失效已送达，不得重复计入`,
        line: L,
        cores: [x],
        mids: [rec.mid],
        gen: pw.gen,
      });
    }
    if (this.caches[x][L] === 'I') {
      this.fail({
        code: 'DIR_COPY_MISMATCH',
        message: `失效送达核 ${x} 时其线 ${L} 副本已是 I，目录等待集合与副本不一致`,
        line: L,
        cores: [x, pw.requester],
        mids: [rec.mid],
        gen: pw.gen,
      });
    }
    this.caches[x][L] = 'I';
    d.sharers.delete(x);
    pw.invDelivered.push(x);
    const ackMid = nextMid(step, 'invack', x);
    pw.ackMids[x] = ackMid;
    this.emit(ackMid, 'invack', L, x, -1, step, pw.gen);
    return `世代 ${pw.gen} 失效请求送达核 ${x}：其线 ${L} 副本置 I，核 ${x} 回送同世代确认 ${ackMid}`;
  }

  private applyAck(ev: ScenarioEvent, _step: number): string {
    const c = this.requireCore(ev.core, ev);
    const L = this.requireLine(ev);
    const gen = ev.gen ?? null;
    // 显式 ack 若对应一封在途 invack，则视同为该消息送达
    if (ev.mid) {
      const rec = this.registry.get(ev.mid);
      if (!rec) {
        this.fail({
          code: 'UNKNOWN_MESSAGE',
          message: `ack 事件 ${ev.id} 引用的消息 ${ev.mid} 不存在`,
          line: L,
          cores: [c],
          mids: [ev.mid],
          gen,
        });
      }
      if (rec.type !== 'invack' || rec.from !== c || rec.line !== L || (gen != null && rec.gen !== gen)) {
        this.fail({
          code: 'LATE_ACK',
          message: `ack 事件 ${ev.id} 与消息 ${ev.mid} 的登记（类型/来源/线/世代）不符`,
          line: L,
          cores: [c],
          mids: [ev.mid],
          gen,
        });
      }
      if (rec.deliveredAt === null) rec.deliveredAt = _step;
      return this.handleAck(c, L, rec.gen ?? gen, ev.mid, false);
    }
    return this.handleAck(c, L, gen, null, false);
  }

  /**
   * 确认推进规则：
   * 只有“当前等待集合中、世代严格相等、且失效已先行送达、此前未计”的确认才能推进写入。
   */
  private handleAck(
    c: number,
    L: number,
    gen: number | null,
    mid: string | null,
    fromMessage: boolean,
  ): string {
    const d = this.dir[L];
    const pw = d.pendingWrite;
    const mids = mid ? [mid] : [];
    if (!pw) {
      this.fail({
        code: 'LATE_ACK',
        message: `线 ${L} 没有进行中的写失效等待集合：来自核 ${c} 的世代 ${gen} 确认为迟到/多余确认，予以拒绝，不释放任何独占权限`,
        line: L,
        cores: [c],
        mids,
        gen,
      });
    }
    if (gen !== pw.gen) {
      this.fail({
        code: 'LATE_ACK',
        message: `线 ${L} 当前等待集合属于世代 ${pw.gen}，来自核 ${c} 的确认世代为 ${gen}；非同世代确认拒绝推进写入`,
        line: L,
        cores: [c, pw.requester],
        mids,
        gen,
      });
    }
    if (!pw.waitSet.includes(c)) {
      this.fail({
        code: 'LATE_ACK',
        message: `核 ${c} 不在线 ${L} 世代 ${pw.gen} 的等待集合 {${pw.waitSet.join(', ')}} 中，其确认不能推进写入`,
        line: L,
        cores: [c, pw.requester],
        mids,
        gen: pw.gen,
      });
    }
    if (!pw.invDelivered.includes(c)) {
      this.fail({
        code: 'STALE_INVALIDATION',
        message: `核 ${c} 的世代 ${pw.gen} 确认先于失效请求送达（失效尚未到达该核），确认无效`,
        line: L,
        cores: [c, pw.requester],
        mids,
        gen: pw.gen,
      });
    }
    if (pw.acked.includes(c)) {
      return `来自核 ${c} 的世代 ${pw.gen} 确认为重复确认：仅回放既有动作，不再次推进（幂等）`;
    }
    pw.acked.push(c);
    if (pw.acked.length === pw.waitSet.length) {
      this.closeWrite(L, '');
      return `来自核 ${c} 的同世代确认到齐（${pw.acked.length}/${pw.waitSet.length}）：世代 ${pw.gen} 写闭合，核 ${pw.requester} 成为线 ${L} 的唯一独占拥有者`;
    }
    return `接受核 ${c} 的世代 ${pw.gen} 确认（${pw.acked.length}/${pw.waitSet.length}），写入继续等待其余同世代确认${
      fromMessage && mid ? `（消息 ${mid}）` : ''
    }`;
  }

  private applyRetry(ev: ScenarioEvent, _step: number): string {
    let mid = ev.mid;
    if (!mid) {
      this.fail({
        code: 'BAD_RETRY',
        message: `事件 ${ev.id} 是 timeout_retry 但缺少 mid`,
        line: ev.line ?? null,
        cores: ev.core != null ? [ev.core] : [],
        mids: [],
        gen: ev.gen ?? null,
      });
    }
    mid = this.resolveMid(mid, ev);
    const rec = this.registry.get(mid);
    if (!rec) {
      this.fail({
        code: 'BAD_RETRY',
        message: `超时重传引用的消息 ${mid} 从未在目录登记，重传不能凭空创建动作`,
        line: ev.line ?? null,
        cores: ev.core != null ? [ev.core] : [],
        mids: [mid],
        gen: ev.gen ?? null,
      });
    }
    if (rec.deliveredAt !== null) {
      this.fail({
        code: 'BAD_RETRY',
        message: `消息 ${mid}（${rec.type}，线 ${rec.line}）已送达，对其发起超时重传属于重复动作；重传只能针对在途丢失消息`,
        line: rec.line,
        cores: [rec.to >= 0 ? rec.to : 0],
        mids: [mid],
        gen: rec.gen ?? null,
      });
    }
    rec.retries += 1;
    return `总线短暂丢包后超时重传消息 ${mid}（${rec.type}，线 ${rec.line}${
      rec.gen != null ? `，世代 ${rec.gen}` : ''
    }）：仅重放既有发送，世代/字段不变，不产生新状态（第 ${rec.retries} 次重传）`;
  }

  // ---- 快照 ----

  private allCores(): number[] {
    return Array.from({ length: this.scenario.coreCount }, (_, i) => i);
  }

  private snapshot(
    index: number,
    ev: ScenarioEvent,
    description: string,
    violation: Violation | null,
  ): StepSnapshot {
    const directory: DirLineSnapshot[] = this.dir.map((d) => ({
      owner: d.owner,
      sharers: [...d.sharers].sort((a, b) => a - b),
      memoryValid: d.memoryValid,
      gen: d.gen,
      closedGens: [...d.closedGens],
      pendingRead: d.pendingRead
        ? {
            requester: d.pendingRead.requester,
            needFrom: d.pendingRead.needFrom,
            fetchMid: d.pendingRead.fetchMid,
            dataMid: d.pendingRead.dataMid,
          }
        : null,
      pendingWrite: d.pendingWrite
        ? {
            gen: d.pendingWrite.gen,
            requester: d.pendingWrite.requester,
            waitSet: [...d.pendingWrite.waitSet],
            invDelivered: [...d.pendingWrite.invDelivered],
            acked: [...d.pendingWrite.acked],
            invMids: { ...d.pendingWrite.invMids },
            ackMids: { ...d.pendingWrite.ackMids },
          }
        : null,
    }));
    const caches = this.caches.map((row) => [...row]);
    const inFlight: MessageRecord[] = [];
    for (const rec of this.registry.values()) {
      if (rec.deliveredAt === null) {
        inFlight.push({ ...rec });
      }
    }
    inFlight.sort((a, b) => a.sentAt - b.sentAt || a.mid.localeCompare(b.mid));
    return {
      index,
      eventId: ev.id,
      kind: ev.kind,
      description,
      directory,
      caches,
      inFlight,
      nextGen: this.dir.map((d) => d.gen + 1),
      frozen: false,
      violation,
    };
  }
}

interface ViolationSignal {
  v: Violation;
}

export function validateLimits(scenario: Scenario): Violation | null {
  if (!Number.isInteger(scenario.coreCount) || scenario.coreCount < 1 || scenario.coreCount > LIMITS.maxCores) {
    return {
      code: 'LIMIT_EXCEEDED',
      message: `核心数 ${scenario.coreCount} 超出允许范围（1..${LIMITS.maxCores}）`,
      line: null,
      cores: [],
      mids: [],
      gen: null,
    };
  }
  if (!Number.isInteger(scenario.lineCount) || scenario.lineCount < 1 || scenario.lineCount > LIMITS.maxLines) {
    return {
      code: 'LIMIT_EXCEEDED',
      message: `缓存线数 ${scenario.lineCount} 超出允许范围（1..${LIMITS.maxLines}）`,
      line: null,
      cores: [],
      mids: [],
      gen: null,
    };
  }
  if (!Array.isArray(scenario.events) || scenario.events.length === 0) {
    return {
      code: 'LIMIT_EXCEEDED',
      message: '事件序列为空',
      line: null,
      cores: [],
      mids: [],
      gen: null,
    };
  }
  if (scenario.events.length > LIMITS.maxEvents) {
    return {
      code: 'LIMIT_EXCEEDED',
      message: `事件数 ${scenario.events.length} 超过上限 ${LIMITS.maxEvents}`,
      line: null,
      cores: [],
      mids: [],
      gen: null,
    };
  }
  return null;
}
