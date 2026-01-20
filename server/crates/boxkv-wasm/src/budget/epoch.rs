//! Epoch-based 超时管理

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use wasmtime::{Engine, Store};

use crate::context::CallContext;
use crate::error::{Result, WasmError};

/// Epoch Ticker 全局管理器（Engine 级别）
pub struct EpochTicker {
    /// Wasmtime 引擎
    engine: Arc<Engine>,

    /// 当前 epoch（与 engine 同步）
    current_epoch: Arc<AtomicU64>,

    /// Tick 间隔（毫秒）
    tick_ms: u64,

    /// 后台线程句柄
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    /// 关闭标志
    shutdown: Arc<AtomicBool>,

    /// 统计信息
    stats: Arc<Mutex<TickerStats>>,
}

/// Ticker 统计
#[derive(Debug, Clone, Default)]
pub struct TickerStats {
    /// 总 tick 次数
    pub total_ticks: u64,

    /// 平均抖动（毫秒）
    pub avg_jitter_ms: f64,

    /// 最大抖动（毫秒）
    pub max_jitter_ms: u64,
}

impl EpochTicker {
    /// 创建并启动 Epoch Ticker
    pub fn new(engine: Arc<Engine>, tick_ms: u64) -> Result<Self> {
        let current_epoch = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(Mutex::new(TickerStats::default()));

        let ticker = Self {
            engine: engine.clone(),
            current_epoch: current_epoch.clone(),
            tick_ms,
            thread_handle: Mutex::new(None),
            shutdown: shutdown.clone(),
            stats: stats.clone(),
        };

        // 启动后台线程
        ticker.start_background_thread()?;

        Ok(ticker)
    }

    /// 启动后台 ticker 线程
    fn start_background_thread(&self) -> Result<()> {
        let engine = self.engine.clone();
        let current_epoch = self.current_epoch.clone();
        let tick_ms = self.tick_ms;
        let shutdown = self.shutdown.clone();
        let stats = self.stats.clone();

        let handle = std::thread::Builder::new()
            .name("wasm-epoch-ticker".to_string())
            .spawn(move || {
                let tick_duration = Duration::from_millis(tick_ms);
                let mut last_tick = Instant::now();

                while !shutdown.load(Ordering::Relaxed) {
                    // 精确等待
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_tick);

                    if elapsed < tick_duration {
                        std::thread::sleep(tick_duration - elapsed);
                    }

                    // Tick
                    let tick_start = Instant::now();
                    engine.increment_epoch();
                    let new_epoch = current_epoch.fetch_add(1, Ordering::SeqCst) + 1;

                    // 计算抖动
                    let actual_interval = tick_start.duration_since(last_tick);
                    let jitter_ms = if actual_interval > tick_duration {
                        (actual_interval - tick_duration).as_millis() as u64
                    } else {
                        (tick_duration - actual_interval).as_millis() as u64
                    };

                    // 更新统计
                    let mut s = stats.lock();
                    s.total_ticks += 1;
                    s.max_jitter_ms = s.max_jitter_ms.max(jitter_ms);

                    // 滑动平均抖动
                    let alpha = 0.1;
                    s.avg_jitter_ms = alpha * (jitter_ms as f64) + (1.0 - alpha) * s.avg_jitter_ms;

                    last_tick = tick_start;

                    // 每 1000 次输出日志
                    if new_epoch % 1000 == 0 {
                        tracing::debug!(
                            "Epoch ticker: epoch={}, avg_jitter={:.2}ms, max_jitter={}ms",
                            new_epoch,
                            s.avg_jitter_ms,
                            s.max_jitter_ms
                        );
                    }
                }

                tracing::info!("Epoch ticker thread stopped");
            })
            .map_err(|e| {
                WasmError::InternalError(format!("Failed to spawn epoch ticker thread: {}", e))
            })?;

        *self.thread_handle.lock() = Some(handle);

        tracing::info!("Epoch ticker started: tick_ms={}", tick_ms);
        Ok(())
    }

    /// 获取当前 epoch
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::SeqCst)
    }

    /// 计算截止 epoch
    pub fn calculate_deadline(&self, timeout_ms: u64) -> u64 {
        let current = self.current_epoch();
        let ticks = (timeout_ms + self.tick_ms - 1) / self.tick_ms; // ceil
        current + ticks
    }

    /// 为 Store 设置 epoch 截止时间
    pub fn set_deadline(&self, store: &mut Store<CallContext>, timeout_ms: u64) {
        let deadline = self.calculate_deadline(timeout_ms);
        store.set_epoch_deadline(deadline);

        tracing::trace!(
            "Set epoch deadline: current={}, timeout_ms={}, deadline={}",
            self.current_epoch(),
            timeout_ms,
            deadline
        );
    }

    /// 获取统计信息
    pub fn stats(&self) -> TickerStats {
        self.stats.lock().clone()
    }

    /// 优雅关闭
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.thread_handle.lock().take() {
            let _ = handle.join();
        }

        tracing::info!("Epoch ticker shutdown complete");
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Epoch 截止时间管理器（每次执行）
pub struct EpochDeadline {
    /// 当前设置的截止 epoch
    deadline: u64,

    /// 开始时的 epoch
    start_epoch: u64,
}

impl EpochDeadline {
    /// 创建新的 deadline 管理器
    pub fn new(ticker: &EpochTicker, timeout_ms: u64) -> Self {
        let start_epoch = ticker.current_epoch();
        let deadline = ticker.calculate_deadline(timeout_ms);

        Self {
            deadline,
            start_epoch,
        }
    }

    /// 应用到 Store
    pub fn apply(&self, store: &mut Store<CallContext>) {
        store.set_epoch_deadline(self.deadline);
    }

    /// 检查是否超时（用于统计）
    pub fn is_timeout(&self, ticker: &EpochTicker) -> bool {
        ticker.current_epoch() >= self.deadline
    }

    /// 获取已消耗的 epoch 数
    pub fn consumed_epochs(&self, ticker: &EpochTicker) -> u64 {
        ticker.current_epoch().saturating_sub(self.start_epoch)
    }

    /// 获取截止 epoch
    pub fn deadline(&self) -> u64 {
        self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::Config;

    #[test]
    fn test_epoch_ticker_basic() {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Arc::new(Engine::new(&config).unwrap());

        let ticker = EpochTicker::new(engine.clone(), 10).unwrap();

        let epoch1 = ticker.current_epoch();
        std::thread::sleep(Duration::from_millis(25));
        let epoch2 = ticker.current_epoch();

        // 应该至少 tick 了 2 次
        assert!(epoch2 >= epoch1 + 2, "epoch2={}, epoch1={}", epoch2, epoch1);

        ticker.shutdown();
    }

    #[test]
    fn test_deadline_calculation() {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Arc::new(Engine::new(&config).unwrap());

        let ticker = EpochTicker::new(engine, 10).unwrap();

        // 15ms timeout -> 2 ticks (ceil(15/10))
        let deadline1 = ticker.calculate_deadline(15);
        let current = ticker.current_epoch();
        assert_eq!(deadline1, current + 2);

        // 10ms timeout -> 1 tick
        let deadline2 = ticker.calculate_deadline(10);
        assert_eq!(deadline2, current + 1);

        // 21ms timeout -> 3 ticks (ceil(21/10))
        let deadline3 = ticker.calculate_deadline(21);
        assert_eq!(deadline3, current + 3);

        ticker.shutdown();
    }

    #[test]
    fn test_ticker_stats() {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Arc::new(Engine::new(&config).unwrap());

        let ticker = EpochTicker::new(engine, 10).unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let stats = ticker.stats();
        assert!(stats.total_ticks >= 4, "ticks={}", stats.total_ticks);

        ticker.shutdown();
    }
}
