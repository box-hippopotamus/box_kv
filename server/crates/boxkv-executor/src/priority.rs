use std::collections::VecDeque;
use std::time::Instant;

use crate::task::Task;

/// 任务优先级（三级）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// 用户请求
    Critical = 3,
    /// 低优先级后台任务
    High = 2,
    /// Flush / WAL / 重型后台任务
    Medium = 1,
}

impl Priority {
    /// 从整数转换
    pub fn from_u8(val: u8) -> Self {
        match val {
            3 => Self::Critical,
            2 => Self::High,
            1 => Self::Medium,
            _ => Self::Medium,
        }
    }

    /// 转为整数
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// 数组下标（0=Critical, 1=High, 2=Medium）
    #[inline]
    pub const fn idx(self) -> usize {
        match self {
            Priority::Critical => 0,
            Priority::High => 1,
            Priority::Medium => 2,
        }
    }

    /// 从数组下标转换
    #[inline]
    pub const fn from_idx(idx: usize) -> Self {
        match idx {
            0 => Priority::Critical,
            1 => Priority::High,
            _ => Priority::Medium,
        }
    }
}

struct Level {
    q: VecDeque<Task>,
    deficit: u64,
    quantum: u64,
}

impl Level {
    fn new(quantum: u64) -> Self {
        Self {
            q: VecDeque::new(),
            deficit: 0,
            quantum,
        }
    }
}

/// 全局 DRR（Deficit Round Robin）队列：纯数据结构
pub struct PriorityQueue {
    levels: [Level; 3],
    cursor: usize, // 下一次从哪个队列开始扫描
}

impl PriorityQueue {
    /// 默认权重：Critical:High:Medium = 8:4:1
    pub fn new() -> Self {
        Self::with_quanta(8, 4, 1)
    }

    pub fn with_quanta(critical_quantum: u64, high_quantum: u64, medium_quantum: u64) -> Self {
        Self {
            levels: [
                Level::new(critical_quantum),
                Level::new(high_quantum),
                Level::new(medium_quantum),
            ],
            cursor: 0,
        }
    }

    /// 入队（FIFO），由上层设置 metadata.priority/cost。
    pub fn push(&mut self, mut task: Task) {
        task.metadata.enqueued_at = Some(Instant::now());
        let idx = task.metadata.priority.idx();
        self.levels[idx].q.push_back(task);
    }

    pub fn is_empty(&self) -> bool {
        self.levels.iter().all(|l| l.q.is_empty())
    }

    pub fn len(&self) -> usize {
        self.levels.iter().map(|l| l.q.len()).sum()
    }

    /// 各优先级队列长度（Critical, High, Medium）
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.levels[Priority::Critical.idx()].q.len(),
            self.levels[Priority::High.idx()].q.len(),
            self.levels[Priority::Medium.idx()].q.len(),
        )
    }

    /// 全局 DRR 出队
    pub fn pop_drr_with_medium_cap<FCan, FAcquire, P>(
        &mut self,
        can_pick_medium: FCan,
        mut acquire_medium_permit: FAcquire,
    ) -> Option<(Task, Option<P>)>
    where
        FCan: Fn() -> bool,
        FAcquire: FnMut() -> Option<P>,
    {
        if self.is_empty() {
            return None;
        }

        if let Some(res) = self.scan_once(&can_pick_medium, &mut acquire_medium_permit) {
            return Some(res);
        }

        let mut min_refill_needed: Option<u64> = None;

        for i in 0..3 {
            let prio = Priority::from_idx(i);
            if prio == Priority::Medium && !can_pick_medium() {
                continue;
            }

            let lvl = &self.levels[i];
            let Some(front) = lvl.q.front() else { continue };
            let cost = front.metadata.cost;

            if lvl.quantum == 0 {
                continue;
            }

            if lvl.deficit >= cost {
                min_refill_needed = Some(0);
                break;
            }

            let needed = (cost - lvl.deficit).div_ceil(lvl.quantum);
            min_refill_needed = Some(match min_refill_needed {
                Some(cur) => cur.min(needed),
                None => needed,
            });
        }

        let refill_needed = min_refill_needed?;

        let medium_idx = Priority::Medium.idx();
        let medium_deficit_before_refill = self.levels[medium_idx].deficit;

        if refill_needed > 0 {
            for i in 0..3 {
                let prio = Priority::from_idx(i);
                if prio == Priority::Medium && !can_pick_medium() {
                    continue;
                }
                if self.levels[i].q.is_empty() {
                    continue;
                }
                let add = refill_needed.saturating_mul(self.levels[i].quantum);
                self.levels[i].deficit = self.levels[i].deficit.saturating_add(add);
            }
        }

        let result = self.scan_once(&can_pick_medium, &mut acquire_medium_permit);

        if result.is_none()
            && !self.levels[medium_idx].q.is_empty()
            && can_pick_medium()
            && self.levels[medium_idx].deficit > medium_deficit_before_refill
        {
            self.levels[medium_idx].deficit = medium_deficit_before_refill;
        }

        result
    }

    fn scan_once<FCan, FAcquire, P>(
        &mut self,
        can_pick_medium: &FCan,
        acquire_medium_permit: &mut FAcquire,
    ) -> Option<(Task, Option<P>)>
    where
        FCan: Fn() -> bool,
        FAcquire: FnMut() -> Option<P>,
    {
        // 从 cursor 开始轮询三个队列
        for _ in 0..3 {
            let i = self.cursor;
            let prio = Priority::from_idx(i);

            // 队列为空：跳过
            if self.levels[i].q.is_empty() {
                self.cursor = (self.cursor + 1) % 3;
                continue;
            }

            // Medium cap 不可用：跳过且不改变任何状态
            if prio == Priority::Medium && !can_pick_medium() {
                self.cursor = (self.cursor + 1) % 3;
                continue;
            }

            // 检查队首任务
            let Some(front) = self.levels[i].q.front() else {
                self.cursor = (self.cursor + 1) % 3;
                continue;
            };
            let cost = front.metadata.cost;

            // 当前 deficit 不足：跳过（不增加 deficit）
            if self.levels[i].deficit < cost {
                self.cursor = (self.cursor + 1) % 3;
                continue;
            }

            // deficit 足够：尝试出队
            // Medium 需要先获取 permit
            let medium_permit = if prio == Priority::Medium {
                match acquire_medium_permit() {
                    Some(p) => Some(p),
                    None => {
                        // permit 获取失败：跳过且不改变任何状态
                        self.cursor = (self.cursor + 1) % 3;
                        continue;
                    }
                }
            } else {
                None
            };

            // 成功：出队并扣除 deficit
            self.levels[i].deficit = self.levels[i].deficit.saturating_sub(cost);
            let task = self.levels[i].q.pop_front().expect("front checked");

            // 推进 cursor 到下一个队列
            self.cursor = (i + 1) % 3;

            return Some((task, medium_permit));
        }

        None
    }
}

impl Default for PriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn mk_task(priority: Priority, cost: u64, name: &str) -> Task {
        let mut t = Task::new(name.to_string(), priority, |_cancel| {});
        t.metadata.cost = cost.max(1);
        t
    }

    #[test]
    fn push_sets_enqueued_at_and_lengths() {
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        assert!(pq.is_empty());
        assert_eq!(pq.len(), 0);

        pq.push(mk_task(Priority::Critical, 1, "c1"));
        pq.push(mk_task(Priority::High, 1, "h1"));
        pq.push(mk_task(Priority::Medium, 1, "m1"));

        assert!(!pq.is_empty());
        assert_eq!(pq.len(), 3);

        let (c, h, m) = pq.stats();
        assert_eq!((c, h, m), (1, 1, 1));

        assert!(
            pq.levels[Priority::Critical.idx()]
                .q
                .front()
                .unwrap()
                .metadata
                .enqueued_at
                .is_some()
        );
        assert!(
            pq.levels[Priority::High.idx()]
                .q
                .front()
                .unwrap()
                .metadata
                .enqueued_at
                .is_some()
        );
        assert!(
            pq.levels[Priority::Medium.idx()]
                .q
                .front()
                .unwrap()
                .metadata
                .enqueued_at
                .is_some()
        );
    }

    #[test]
    fn drr_respects_quantum_ratio_statistically() {
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        for i in 0..1000 {
            pq.push(mk_task(Priority::Critical, 1, &format!("c{i}")));
            pq.push(mk_task(Priority::High, 1, &format!("h{i}")));
            pq.push(mk_task(Priority::Medium, 1, &format!("m{i}")));
        }

        let mut cnt_c = 0usize;
        let mut cnt_h = 0usize;
        let mut cnt_m = 0usize;

        for _ in 0..1300 {
            let (task, _permit) = pq
                .pop_drr_with_medium_cap(|| true, || Some(()))
                .expect("should pop");
            match task.metadata.priority {
                Priority::Critical => cnt_c += 1,
                Priority::High => cnt_h += 1,
                Priority::Medium => cnt_m += 1,
            }
        }

        // 8:4:1 => 13 份
        let target_c = 800isize;
        let target_h = 400isize;
        let target_m = 100isize;

        let tol_c = (target_c as f64 * 0.10) as isize;
        let tol_h = (target_h as f64 * 0.10) as isize;
        let tol_m = (target_m as f64 * 0.15) as isize;

        let dc = cnt_c as isize - target_c;
        let dh = cnt_h as isize - target_h;
        let dm = cnt_m as isize - target_m;

        assert!(
            dc.abs() <= tol_c,
            "critical ratio off: got {cnt_c}, expected ~{target_c} (+/-{tol_c})"
        );
        assert!(
            dh.abs() <= tol_h,
            "high ratio off: got {cnt_h}, expected ~{target_h} (+/-{tol_h})"
        );
        assert!(
            dm.abs() <= tol_m,
            "medium ratio off: got {cnt_m}, expected ~{target_m} (+/-{tol_m})"
        );
    }

    #[test]
    fn large_cost_task_eventually_runs_due_to_refill() {
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        pq.push(mk_task(Priority::High, 50, "big_high"));
        for i in 0..20 {
            pq.push(mk_task(Priority::Critical, 1, &format!("c{i}")));
        }

        let mut saw_big = false;
        for _ in 0..2000 {
            let (task, _permit) = pq
                .pop_drr_with_medium_cap(|| true, || Some(()))
                .expect("queue not empty => should pop");
            if task.metadata.name == "big_high" {
                saw_big = true;
                break;
            }
        }

        assert!(saw_big, "large cost task should eventually be scheduled");
    }

    #[test]
    fn medium_cap_blocks_without_advancing_medium_deficit() {
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        pq.push(mk_task(Priority::Medium, 2, "m_cost2"));

        assert_eq!(pq.levels[Priority::Medium.idx()].deficit, 0);

        // cap 关闭：完全不触碰 Medium deficit
        for _ in 0..10 {
            let r = pq.pop_drr_with_medium_cap(|| false, || Some(()));
            assert!(r.is_none());
            assert_eq!(
                pq.levels[Priority::Medium.idx()].deficit,
                0,
                "medium deficit must not advance when cap closed"
            );
        }

        // cap 打开：cost=2, quantum=1，累计后能 pop
        let mut got = false;
        for _ in 0..10 {
            let r = pq.pop_drr_with_medium_cap(|| true, || Some(()));
            if let Some((task, _p)) = r {
                assert_eq!(task.metadata.priority, Priority::Medium);
                assert_eq!(task.metadata.name, "m_cost2");
                got = true;
                break;
            }
        }
        assert!(got);
    }

    #[test]
    fn medium_acquire_failure_is_atomic_no_pop_no_deficit_advance() {
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        pq.push(mk_task(Priority::Medium, 1, "m1"));

        let before_def = pq.levels[Priority::Medium.idx()].deficit;
        assert_eq!(before_def, 0);

        // can_pick_medium=true，但 acquire 失败
        let r = pq.pop_drr_with_medium_cap(|| true, || None::<()>);
        assert!(r.is_none());

        // deficit 不变，任务仍在
        assert_eq!(pq.levels[Priority::Medium.idx()].deficit, before_def);
        assert_eq!(pq.levels[Priority::Medium.idx()].q.len(), 1);
    }

    #[test]
    fn medium_permit_consumption_is_counted() {
        // 用一个“可用 permit 计数器”模拟 cap，并验证：
        // - 每 pop 一个 medium，必须消耗一次 permit
        let mut pq = PriorityQueue::with_quanta(8, 4, 1);

        for i in 0..10 {
            pq.push(mk_task(Priority::Medium, 1, &format!("m{i}")));
        }

        let permits = Arc::new(AtomicUsize::new(3));
        let acquire_calls = Arc::new(AtomicUsize::new(0));

        let can_pick = {
            let permits = Arc::clone(&permits);
            move || permits.load(Ordering::Relaxed) > 0
        };

        let mut acquire = {
            let permits = Arc::clone(&permits);
            let calls = Arc::clone(&acquire_calls);
            move || {
                calls.fetch_add(1, Ordering::Relaxed);
                let cur = permits.load(Ordering::Relaxed);
                if cur == 0 {
                    return None;
                }
                permits.store(cur - 1, Ordering::Relaxed);
                Some(())
            }
        };

        let mut popped = 0usize;
        for _ in 0..10 {
            let r = pq.pop_drr_with_medium_cap(&can_pick, &mut acquire);
            if r.is_some() {
                popped += 1;
            } else {
                break;
            }
        }

        assert_eq!(popped, 3, "should pop exactly as many as permits");
        assert_eq!(permits.load(Ordering::Relaxed), 0);
        assert!(
            acquire_calls.load(Ordering::Relaxed) >= 3,
            "acquire should be called for each successful medium pop"
        );
        assert_eq!(pq.levels[Priority::Medium.idx()].q.len(), 7);
    }

    #[test]
    fn cursor_rotation_does_not_stick() {
        let mut pq = PriorityQueue::with_quanta(1, 1, 1);

        pq.push(mk_task(Priority::Critical, 1, "c"));
        pq.push(mk_task(Priority::High, 1, "h"));

        let first = pq.pop_drr_with_medium_cap(|| true, || Some(())).unwrap().0;
        let second = pq.pop_drr_with_medium_cap(|| true, || Some(())).unwrap().0;

        assert_ne!(first.metadata.priority, second.metadata.priority);
        assert!(pq.pop_drr_with_medium_cap(|| true, || Some(())).is_none());
    }
}
