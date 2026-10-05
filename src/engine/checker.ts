import { CacheState, DirLineSnapshot, Violation } from './types';

/**
 * 目录—副本一致性复核器（纯函数，独立于引擎状态机）。
 * 每步结束后由引擎调用；也可对任意快照（含手工构造的损坏快照）复核：
 *  - 重复独占者（同一线多于一个 M）
 *  - 目录 owner 与副本 M 不符
 *  - 共享集合与 S 副本不符
 *  - 内存有效位与独占状态不符
 */
export function checkDirCacheInvariant(
  directory: DirLineSnapshot[],
  caches: CacheState[][],
  coreCount: number,
  lineCount: number,
  context: { step: number; eventId: string; mid?: string },
): Violation | null {
  const cores = Array.from({ length: coreCount }, (_, i) => i);
  for (let L = 0; L < lineCount; L++) {
    const d = directory[L];
    const mCores = cores.filter((x) => caches[x][L] === 'M');
    if (mCores.length > 1) {
      return {
        code: 'DUPLICATE_OWNER',
        message: `步 ${context.step}（事件 ${context.eventId}）后线 ${L} 出现多个独占拥有者：核 ${mCores.join('、')}`,
        line: L,
        cores: mCores,
        mids: context.mid ? [context.mid] : [],
        gen: d.gen,
      };
    }
    if (d.owner !== null) {
      if (caches[d.owner][L] !== 'M') {
        return {
          code: 'DIR_COPY_MISMATCH',
          message: `步 ${context.step}（事件 ${context.eventId}）后目录声明线 ${L} 独占者为核 ${d.owner}，但其副本为 ${caches[d.owner][L]}`,
          line: L,
          cores: [d.owner],
          mids: context.mid ? [context.mid] : [],
          gen: d.gen,
        };
      }
    } else if (mCores.length === 1) {
      return {
        code: 'DIR_COPY_MISMATCH',
        message: `步 ${context.step}（事件 ${context.eventId}）后核 ${mCores[0]} 在线 ${L} 持有 M，但目录无独占记录`,
        line: L,
        cores: mCores,
        mids: context.mid ? [context.mid] : [],
        gen: d.gen,
      };
    }
    for (const x of cores) {
      const inSharers = d.sharers.includes(x);
      const state = caches[x][L];
      if (state === 'S' && !inSharers) {
        return {
          code: 'DIR_COPY_MISMATCH',
          message: `步 ${context.step}（事件 ${context.eventId}）后核 ${x} 在线 ${L} 持有 S 副本，但目录共享集合中没有该核`,
          line: L,
          cores: [x],
          mids: context.mid ? [context.mid] : [],
          gen: d.gen,
        };
      }
      if (inSharers && state === 'I') {
        return {
          code: 'DIR_COPY_MISMATCH',
          message: `步 ${context.step}（事件 ${context.eventId}）后目录共享集合含核 ${x}，但其线 ${L} 副本为 I`,
          line: L,
          cores: [x],
          mids: context.mid ? [context.mid] : [],
          gen: d.gen,
        };
      }
    }
    const expectMemoryValid = d.owner === null;
    if (d.memoryValid !== expectMemoryValid) {
      return {
        code: 'DIR_COPY_MISMATCH',
        message: `步 ${context.step}（事件 ${context.eventId}）后线 ${L} 内存有效位=${d.memoryValid} 与独占状态不一致（owner=${d.owner}）`,
        line: L,
        cores: d.owner != null ? [d.owner] : [],
        mids: context.mid ? [context.mid] : [],
        gen: d.gen,
      };
    }
  }
  return null;
}
