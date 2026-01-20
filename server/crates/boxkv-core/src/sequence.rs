use std::sync::atomic::{AtomicU64, Ordering};

pub struct SequenceGenerator {
    counter: AtomicU64,
}

impl SequenceGenerator {
    // 创建新的序列号生成器
    pub fn new(initial: u64) -> Self {
        Self {
            counter: AtomicU64::new(initial),
        }
    }

    // 获取下一个序列号
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    // 获取当前序列号（不递增）
    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }

    // 设置序列号（用于恢复）
    pub fn set(&self, seq: u64) {
        self.counter.store(seq, Ordering::SeqCst);
    }

    /// 批量分配序列号
    /// - 为批量操作分配连续的序列号范围
    /// - 返回起始序列号，调用者可使用 [start, start+count) 范围
    pub fn next_batch(&self, count: u64) -> u64 {
        self.counter.fetch_add(count, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_sequence_basic() {
        let seq_gen = SequenceGenerator::new(0);

        assert_eq!(seq_gen.current(), 0);
        assert_eq!(seq_gen.next(), 0);
        assert_eq!(seq_gen.current(), 1);
        assert_eq!(seq_gen.next(), 1);
        assert_eq!(seq_gen.current(), 2);
    }

    #[test]
    fn test_sequence_set() {
        let seq_gen = SequenceGenerator::new(0);
        seq_gen.set(100);

        assert_eq!(seq_gen.current(), 100);
        assert_eq!(seq_gen.next(), 100);
        assert_eq!(seq_gen.current(), 101);
    }

    #[test]
    fn test_concurrent_access() {
        let seq_gen = Arc::new(SequenceGenerator::new(0));
        let mut handles = vec![];

        // 10个线程并发获取序列号
        for _ in 0..10 {
            let seq_gen = Arc::clone(&seq_gen);
            let handle = thread::spawn(move || {
                let mut results = vec![];
                for _ in 0..100 {
                    results.push(seq_gen.next());
                }
                results
            });
            handles.push(handle);
        }

        // 收集所有结果
        let mut all_seqs = vec![];
        for handle in handles {
            all_seqs.extend(handle.join().unwrap());
        }

        // 验证：1000个不重复的序列号
        all_seqs.sort();
        assert_eq!(all_seqs.len(), 1000);
        for (i, &seq) in all_seqs.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }
}
