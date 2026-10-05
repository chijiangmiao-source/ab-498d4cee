/**
 * 目录缓存一致性重放引擎 —— 类型定义
 *
 * 协议模型（每缓存线独立维护世代号）：
 *  - 核缓存副本状态： I（无效）/ S（共享）/ M（独占修改，即独占者）
 *  - read_miss：若存在 M 拥有者，目录先投递 fetch 要求拥有者下放数据并降级为 S，
 *    数据必须来自该拥有者；无拥有者时由内存/目录提供数据。
 *  - write_upgrade：目录签发绑定递增世代号的失效请求（invalidation），
 *    进入等待集合；只有等待集合中、同世代、且失效已先行送达的确认才能推进写入，
 *    全部到齐后写闭合，请求核成为唯一 M 拥有者。
 *  - timeout_retry / 重复投递：只能重放既有动作（幂等），不得产生新状态、新世代。
 */

export type CacheState = 'I' | 'S' | 'M';

export type EventKind =
  | 'read_miss'
  | 'write_upgrade'
  | 'message'
  | 'timeout_retry'
  | 'ack';

export type MessageType = 'fetch' | 'data' | 'invalidation' | 'invack';

export interface ScenarioEvent {
  id: string;
  kind: EventKind;
  core?: number;
  line?: number;
  /** message / timeout_retry 使用 */
  mid?: string;
  /** 投递事件可选的声明字段，引擎会与在途记录逐条核对 */
  to?: number;
  from?: number | null;
  gen?: number;
}

export interface Scenario {
  name: string;
  coreCount: number;
  lineCount: number;
  events: ScenarioEvent[];
}

export interface MessageRecord {
  mid: string;
  type: MessageType;
  line: number;
  from: number | null; // null = 目录/内存
  to: number;
  gen?: number;
  sentAt: number;
  deliveredAt: number | null;
  retries: number;
}

export interface PendingRead {
  requester: number;
  /** 当前 M 拥有者；null 表示由内存提供数据 */
  needFrom: number | null;
  fetchMid: string | null;
  dataMid: string | null;
}

export interface PendingWrite {
  gen: number;
  requester: number;
  waitSet: number[];
  invDelivered: number[];
  acked: number[];
  invMids: Record<number, string>;
  ackMids: Record<number, string>;
}

export interface DirLineSnapshot {
  owner: number | null;
  sharers: number[];
  memoryValid: boolean;
  gen: number;
  closedGens: number[];
  pendingRead: PendingRead | null;
  pendingWrite: PendingWrite | null;
}

export interface Violation {
  code:
    | 'LATE_ACK'
    | 'MISSING_OWNER_DATA'
    | 'DUPLICATE_OWNER'
    | 'DIR_COPY_MISMATCH'
    | 'STALE_INVALIDATION'
    | 'UNKNOWN_MESSAGE'
    | 'BAD_RETRY'
    | 'BAD_EVENT'
    | 'LIMIT_EXCEEDED';
  message: string;
  line: number | null;
  cores: number[];
  mids: string[];
  gen: number | null;
}

export interface StepSnapshot {
  index: number;
  eventId: string;
  kind: EventKind;
  description: string;
  directory: DirLineSnapshot[];
  caches: CacheState[][];
  inFlight: MessageRecord[];
  nextGen: number[];
  frozen: boolean;
  violation: Violation | null;
}

export interface RunHooks {
  /** 每执行完一步后回调（快照已生成但未追加进结果前也可观察） */
  onStep?: (index: number, total: number) => void;
  /** 返回 true 则引擎在当前步边界中止，结果带 cancelled 标记 */
  isCancelled?: () => boolean;
  /** 每步之间让出事件循环（Web Worker 中用于响应取消消息） */
  yieldBetweenSteps?: boolean;
}

export interface ReplayResult {
  scenario: string;
  coreCount: number;
  lineCount: number;
  steps: StepSnapshot[];
  frozen: boolean;
  violation: Violation | null;
  frozenAtStep: number | null;
  finalSnapshot: StepSnapshot | null;
  cancelled?: boolean;
}

export const LIMITS = {
  maxCores: 4,
  maxLines: 8,
  maxEvents: 48,
} as const;
