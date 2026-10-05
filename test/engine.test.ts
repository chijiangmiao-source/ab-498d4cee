import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { ReplayEngine } from '../src/engine/engine';
import { parseScenario } from '../src/engine/parser';
import { checkDirCacheInvariant } from '../src/engine/checker';
import { Scenario, StepSnapshot } from '../src/engine/types';

const here = dirname(fileURLToPath(import.meta.url));
const rawSamples = JSON.parse(
  readFileSync(resolve(here, '../src/samples.json'), 'utf8'),
) as Record<string, unknown>;
const samples: Record<string, Scenario> = {};
for (const [k, v] of Object.entries(rawSamples)) {
  const { scenario, errors } = parseScenario(JSON.stringify(v));
  if (!scenario) throw new Error(`样例 ${k} 解析失败：${errors.join('；')}`);
  samples[k] = scenario;
}

function run(s: Scenario) {
  return new ReplayEngine(s).runSync();
}
function runText(text: string) {
  const { scenario, errors } = parseScenario(text);
  expect(errors).toEqual([]);
  return run(scenario!);
}

describe('合法：丢包重传后同世代确认闭合', () => {
  const r = run(samples['legal-retry']);

  it('全程无违约', () => {
    expect(r.frozen).toBe(false);
    expect(r.violation).toBeNull();
  });

  it('重传只重放既有发送，世代不变', () => {
    const retry = r.steps.find((s) => s.eventId === 'e6')!;
    expect(retry.description).toContain('重传');
    const inv = retry.inFlight.find((m) => m.type === 'invalidation')!;
    expect(inv.gen).toBe(1);
    expect(inv.sentAt).toBe(4); // 仍在 e5 签发
  });

  it('同世代确认到齐后写才闭合，独占唯一', () => {
    const last = r.finalSnapshot!;
    expect(last.directory[0].owner).toBe(0);
    expect(last.caches.map((row) => row[0])).toEqual(['M', 'I', 'I']);
    expect(last.directory[0].closedGens).toEqual([1]);
    expect(last.directory[0].pendingWrite).toBeNull();
  });

  it('失效世代确认前请求核不持有独占权限', () => {
    const e7 = r.steps.find((s) => s.eventId === 'e7')!;
    expect(e7.caches[0][0]).not.toBe('M');
    expect(e7.directory[0].owner).toBeNull();
  });
});

describe('违约：迟到的旧世代确认不得释放新一轮独占权限', () => {
  const r = run(samples['late-ack']);

  it('在首次违约步冻结', () => {
    expect(r.frozen).toBe(true);
    expect(r.frozenAtStep).toBe(11); // e12（0-based 11）
    expect(r.steps).toHaveLength(12);
  });

  it('判定为 LATE_ACK 并说明线/核/世代/消息上下文', () => {
    const v = r.violation!;
    expect(v.code).toBe('LATE_ACK');
    expect(v.line).toBe(0);
    expect(v.cores).toContain(1);
    expect(v.gen).toBe(1);
    expect(v.message).toContain('世代 2');
  });

  it('新一轮写入未被旧确认推进：请求核仍非独占，等待集合保留', () => {
    const s = r.finalSnapshot!;
    expect(s.directory[0].pendingWrite?.gen).toBe(2);
    expect(s.directory[0].owner).toBeNull();
    expect(s.caches[0][0]).toBe('S');
  });
});

describe('违约：确认先于失效请求送达', () => {
  const r = run(samples['early-ack']);
  it('冻结且确认无效', () => {
    expect(r.frozen).toBe(true);
    expect(r.violation?.code).toBe('STALE_INVALIDATION');
    expect(r.violation?.cores).toContain(1);
    expect(r.finalSnapshot!.caches[1][0]).not.toBe('I');
    expect(r.finalSnapshot!.directory[0].owner).toBeNull();
  });
});

describe('违约：缺失拥有者数据', () => {
  const r = run(samples['missing-owner-data']);
  it('内存数据不得顶替拥有者下放', () => {
    expect(r.frozen).toBe(true);
    expect(r.violation?.code).toBe('MISSING_OWNER_DATA');
    expect(r.violation?.cores).toEqual(expect.arrayContaining([0, 1]));
    expect(r.violation?.message).toContain('拥有者');
    expect(r.finalSnapshot!.caches[1][0]).toBe('I'); // 读缺失未闭合
    expect(r.finalSnapshot!.directory[0].pendingRead).not.toBeNull();
  });
});

describe('合法：重复投递/重复确认幂等', () => {
  const r = run(samples['idempotent-replay']);
  it('不产生新状态且正常闭合', () => {
    expect(r.frozen).toBe(false);
    const dupInv = r.steps.find((s) => s.eventId === 'e7')!;
    expect(dupInv.description).toContain('重复投递');
    const dupAck = r.steps.find((s) => s.eventId === 'e9')!;
    expect(dupAck.description).toContain('重复投递');
    expect(r.finalSnapshot!.directory[0].owner).toBe(0);
    expect(r.finalSnapshot!.caches[0][0]).toBe('M');
  });
});

describe('拥有者正常下放路径（fetch→data）', () => {
  const text = JSON.stringify({
    name: 'owner-supply',
    cores: 2,
    lines: 1,
    events: [
      { id: 'a', type: 'read_miss', core: 0, line: 0 },
      { id: 'b', type: 'message', mid: '@AUTO:a:data:0' },
      { id: 'c', type: 'write_upgrade', core: 0, line: 0 },
      { id: 'd', type: 'read_miss', core: 1, line: 0 },
      { id: 'e', type: 'message', mid: '@AUTO:d:fetch:0' },
      { id: 'f', type: 'message', mid: '@AUTO:e:data:1' },
    ],
  });
  const r = runText(text);
  it('拥有者降级为 S 并提供数据，双副本共享', () => {
    expect(r.frozen).toBe(false);
    const s = r.finalSnapshot!;
    expect(s.caches.map((row) => row[0])).toEqual(['S', 'S']);
    expect(s.directory[0].sharers).toEqual([0, 1]);
    expect(s.directory[0].owner).toBeNull();
    expect(s.directory[0].pendingRead).toBeNull();
  });
});

describe('复核器独立复核损坏快照', () => {
  const base = (mutate: (dir: StepSnapshot['directory'], caches: StepSnapshot['caches']) => void) => {
    const dir = [
      {
        owner: null,
        sharers: [] as number[],
        memoryValid: true,
        gen: 0,
        closedGens: [],
        pendingRead: null,
        pendingWrite: null,
      },
    ];
    const caches = [
      ['I', 'I'],
      ['I', 'I'],
    ] as StepSnapshot['caches'];
    mutate(dir, caches);
    return checkDirCacheInvariant(dir, caches, 2, 1, { step: 1, eventId: 'x' });
  };

  it('重复独占者 → DUPLICATE_OWNER', () => {
    const v = base((d, c) => {
      c[0][0] = 'M';
      c[1][0] = 'M';
    });
    expect(v?.code).toBe('DUPLICATE_OWNER');
    expect(v?.cores).toEqual([0, 1]);
  });

  it('目录 owner 与副本不符 → DIR_COPY_MISMATCH', () => {
    const v = base((d, c) => {
      d[0].owner = 0;
      d[0].memoryValid = false;
      c[0][0] = 'S';
    });
    expect(v?.code).toBe('DIR_COPY_MISMATCH');
  });

  it('持有 M 但目录无记录 → DIR_COPY_MISMATCH', () => {
    const v = base((_d, c) => {
      c[0][0] = 'M';
    });
    expect(v?.code).toBe('DIR_COPY_MISMATCH');
  });

  it('共享集合与 S 副本不符 → DIR_COPY_MISMATCH', () => {
    const v = base((_d, c) => {
      c[0][0] = 'S';
    });
    expect(v?.code).toBe('DIR_COPY_MISMATCH');
  });

  it('一致快照无违约', () => {
    const v = base((d, c) => {
      d[0].owner = 0;
      d[0].sharers = [0];
      d[0].memoryValid = false;
      c[0][0] = 'M';
    });
    expect(v).toBeNull();
  });
});

describe('导入解析与上限', () => {
  it('宽容字段名', () => {
    const { scenario, errors } = parseScenario(
      JSON.stringify({ name: 't', cores: 4, lines: 8, events: [{ type: 'read_miss', core: 0, line: 0 }] }),
    );
    expect(errors).toEqual([]);
    expect(scenario?.coreCount).toBe(4);
  });

  it('核心数超 4 拒绝', () => {
    const { scenario, errors } = parseScenario(
      JSON.stringify({ cores: 5, lines: 1, events: [] }),
    );
    expect(scenario).toBeNull();
    expect(errors.join()).toMatch(/核心数/);
  });

  it('事件数超 48 在引擎层拒绝', () => {
    const events = Array.from({ length: 49 }, (_, i) => ({
      id: `x${i}`,
      type: 'read_miss',
      core: 0,
      line: 0,
    }));
    const r = run({ name: 'big', coreCount: 1, lineCount: 1, events });
    expect(r.frozen).toBe(true);
    expect(r.violation?.code).toBe('LIMIT_EXCEEDED');
  });
});
