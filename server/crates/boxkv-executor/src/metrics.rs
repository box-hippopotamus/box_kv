//! EWMA 统计器
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// EWMA 统计器
pub struct EwmaEstimator {
    /// 当前 EWMA 值（放大 1000 倍存储，避免浮点数）
    value: Arc<AtomicU64>,

    /// 权重因子（0-1000）
    alpha: u64,
}

impl EwmaEstimator {
    /// 创建新的 EWMA 估算器
    pub fn new(initial: u64, alpha: f64) -> Self {
        let alpha_scaled = (alpha * 1000.0) as u64;
        Self {
            value: Arc::new(AtomicU64::new(initial * 1000)), // 放大 1000 倍
            alpha: alpha_scaled.clamp(1, 1000),
        }
    }

    /// 使用默认参数创建（α = 0.2）
    pub fn default_with_initial(initial: u64) -> Self {
        Self::new(initial, 0.2)
    }

    /// 更新 EWMA 值
    pub fn update(&self, new_value: u64) {
        let old = self.value.load(Ordering::Relaxed);
        let new_scaled = new_value * 1000;

        // EWMA = α * new + (1 - α) * old
        let updated = (self.alpha * new_scaled + (1000 - self.alpha) * old) / 1000;

        self.value.store(updated, Ordering::Relaxed);
    }

    /// 获取当前估算值（字节数）
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed) / 1000
    }

    /// 克隆统计器（共享底层数据）
    pub fn clone_shared(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            alpha: self.alpha,
        }
    }
}

impl Default for EwmaEstimator {
    fn default() -> Self {
        Self::new(4096, 0.2) // 默认 4KB，α = 0.2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_basic() {
        let ewma = EwmaEstimator::new(100, 0.2);
        assert_eq!(ewma.get(), 100);

        // 更新为 200
        ewma.update(200);
        // EWMA = 0.2 * 200 + 0.8 * 100 = 40 + 80 = 120
        assert_eq!(ewma.get(), 120);

        // 更新为 200
        ewma.update(200);
        // EWMA = 0.2 * 200 + 0.8 * 120 = 40 + 96 = 136
        assert_eq!(ewma.get(), 136);
    }

    #[test]
    fn test_ewma_smoothing() {
        let ewma = EwmaEstimator::new(1000, 0.2);

        // 模拟波动数据：1000, 2000, 1500, 1800, 1200
        let values = vec![2000, 1500, 1800, 1200];
        for &v in &values {
            ewma.update(v);
        }

        // EWMA 应该平滑波动，介于最小和最大值之间
        let result = ewma.get();
        assert!(result > 1200 && result < 2000);
    }

    #[test]
    fn test_ewma_shared_clone() {
        let ewma1 = EwmaEstimator::new(100, 0.2);
        let ewma2 = ewma1.clone_shared();

        ewma1.update(200);
        assert_eq!(ewma2.get(), 120); // 共享底层数据
    }

    #[test]
    fn test_ewma_concurrent() {
        use std::thread;

        let ewma = EwmaEstimator::new(1000, 0.2);
        let mut handles = vec![];

        for _ in 0..10 {
            let e = ewma.clone_shared();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    e.update(1500);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // 应该收敛到 1500 附近
        let result = ewma.get();
        assert!(result > 1400 && result < 1600);
    }
}
