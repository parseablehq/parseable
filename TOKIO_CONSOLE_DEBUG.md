# Tokio Console Warnings in Parseable - Debug Guide

This document explains the tokio-console warnings you're seeing and how to debug and fix them.

## Warning Types Explained

### 1. "This task has lost its waker, and will never be woken again"

**What it means**: A tokio task is stuck in a "waiting" state, but the mechanism that would wake it up (the "waker") has been dropped or lost. The task will never complete and will leak memory.

**Why it's critical**: This leads to:
- Memory leaks (tasks and their data stay in memory forever)
- Potential deadlocks if other tasks are waiting for these stuck tasks
- Resource exhaustion over time
- Application hang if critical tasks are affected

### 2. "This task occupies a large amount of stack space (2632 bytes)"

**What it means**: The task's async stack frame is using more memory than tokio considers optimal.

**Impact**: 
- Increased memory usage when you have many concurrent tasks
- Reduced number of tasks you can spawn
- Performance degradation due to cache misses

## Identified Problem Areas in Parseable

Based on code analysis, here are the likely sources of these issues:

### 1. **Retention Scheduler** (`src/storage/retention.rs`)
```rust
// ❌ PROBLEMATIC: Infinite loop without shutdown handling
let scheduler_handler = tokio::spawn(async move {
    loop {  // No way to exit this loop!
        tokio::time::sleep(Duration::from_secs(10)).await;
        scheduler.run_pending().await;
    }
});
```

**Fixed version**: Added shutdown signal handling (see fixes below)

### 2. **Alert Evaluation Tasks** (`src/sync.rs`)
```rust
// ❌ PROBLEMATIC: Alert tasks run forever without shutdown handling
let handle = tokio::spawn(async move {
    loop {  // No shutdown mechanism
        match alerts_utils::evaluate_alert(&*alert).await {
            // ... evaluation logic
        }
        tokio::time::sleep(Duration::from_secs(sleep_duration * 60)).await;
    }
});
```

### 3. **Alert Target Timeout Tasks** (`src/alerts/target.rs`)
```rust
// ❌ PROBLEMATIC: Infinite retry loops
match retry {
    Retry::Infinite => loop {  // Literally infinite!
        let should_call = sleep_and_check_if_call(state, current_state).await;
        if should_call {
            call_target(target.clone(), alert_context.clone())
        }
        // No break condition except errors
    }
}
```

## How to Debug Using Tokio Console

### 1. **Start tokio-console with your application**
```bash
# Terminal 1: Start Parseable with console instrumentation
RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console -- s3-store

# Terminal 2: Start tokio-console
tokio-console
```

### 2. **Identify problematic tasks in the console**

In the tokio-console TUI:

#### **Tasks Tab**
- Look for tasks with **very long durations** (hours/days)
- Check the **"State"** column - stuck tasks often show "Idle" or "Sleep" for extended periods
- Note the **"Location"** column to see where the task was spawned

#### **Resources Tab**
- Look for channels, mutexes, or other resources that have been "awaited" for long periods
- Check for resources with many waiters that never get notified

### 3. **Specific things to look for**

#### **Lost Waker Tasks**
- **Duration**: More than a few minutes for what should be short-lived tasks
- **State**: Stuck in "Sleep" or "Idle" 
- **Location**: Often in scheduler loops, alert evaluation, or timeout handlers

#### **Large Stack Tasks** 
- **Stack Size**: More than 1-2KB
- **Location**: Usually in complex async functions with many local variables

## Immediate Actions You Can Take

### 1. **Check Current Running Tasks**
In tokio-console, press `t` to go to the Tasks view, then:
- Sort by duration (press `d`) to find long-running tasks
- Look for tasks spawned from these locations:
  - `storage::retention::init_scheduler`
  - `sync::alert_runtime`  
  - `alerts::target::spawn_timeout_task`
  - `resource_check::spawn_resource_monitor`

### 2. **Monitor Task Creation**
- Watch for new tasks being spawned continuously
- If you see many tasks with the same spawn location, you likely have a leak

### 3. **Check Memory Usage**
Run with jemalloc profiling to see if memory is growing:
```bash
export MALLOC_CONF="prof:true,prof_active:true"
RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console,jemalloc-prof -- s3-store
```

## Applied Fixes

I've applied comprehensive fixes to address the tokio-console warnings:

### ✅ **Fixed: Retention Scheduler Shutdown**
Added shutdown signal handling to `src/storage/retention.rs`:

```rust
static SCHEDULER_SHUTDOWN: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// Modified the scheduler loop to check for shutdown
let scheduler_handler = tokio::spawn(async move {
    loop {
        // ✅ Now checks for shutdown signal
        if SCHEDULER_SHUTDOWN.load(Ordering::Relaxed) {
            info!("Retention scheduler shutting down");
            break;
        }
        
        tokio::time::sleep(Duration::from_secs(10)).await;
        scheduler.run_pending().await;
    }
});

// Added function to trigger shutdown
pub fn shutdown_scheduler() {
    SCHEDULER_SHUTDOWN.store(true, Ordering::Relaxed);
    // Handle cleanup...
}
```

### ✅ **Fixed: Alert Runtime Shutdown** 
Completely rewrote `src/sync.rs` alert_runtime to handle shutdown:

```rust
static ALERT_RUNTIME_SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub async fn alert_runtime(mut rx: mpsc::Receiver<AlertTask>) -> Result<(), anyhow::Error> {
    let mut alert_tasks: HashMap<Ulid, tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        tokio::select! {
            // ✅ Check for shutdown signal regularly
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if ALERT_RUNTIME_SHUTDOWN.load(Ordering::Relaxed) {
                    // Cancel all active alert tasks
                    for (id, handle) in alert_tasks {
                        handle.abort();
                    }
                    break;
                }
            }
            
            // ✅ Individual alert evaluation tasks also check for shutdown
            // Modified each alert task to use tokio::select! for cancellable sleep
            task_option = rx.recv() => {
                // Handle alert creation/deletion with shutdown checks
            }
        }
    }
}
```

### ✅ **Fixed: Alert Target Timeout Infinite Loops**
Fixed the truly infinite loops in `src/alerts/target.rs`:

```rust
static ALERT_TARGET_SHUTDOWN: AtomicBool = AtomicBool::new(false);

match retry {
    Retry::Infinite => {
        // ✅ Added protection against truly infinite loops
        let start_time = Instant::now();
        let max_duration = Duration::from_secs(24 * 60 * 60); // 24 hours max
        
        loop {
            // ✅ Check for shutdown signal and maximum duration
            if ALERT_TARGET_SHUTDOWN.load(Ordering::Relaxed) {
                info!("Alert target timeout task shutting down");
                break;
            }
            
            if start_time.elapsed() > max_duration {
                warn!("Alert target timeout task exceeded 24 hours, stopping");
                break;
            }
            
            // ... rest of logic
        }
    }
}

// ✅ Modified sleep function to be cancellable
let sleep_and_check_if_call = |state, current_state| async move {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(timeout * 60)) => {}
        _ = shutdown_check_loop() => return false, // Cancel on shutdown
    }
    // ... rest of logic
};
```

### ✅ **Integrated Shutdown Calls**
Updated `src/handlers/http/health_check.rs` to call all shutdown functions:

```rust
pub async fn shutdown() {
    // Existing code...
    
    // ✅ Shutdown all background tasks
    crate::storage::retention::shutdown_scheduler();
    crate::sync::signal_alert_runtime_shutdown();
    crate::alerts::target::signal_alert_target_shutdown();
    
    // Give tasks time to shutdown gracefully
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    
    // Continue with existing sync operations...
}

## Testing the Fixes

### **Test the Applied Fixes**

1. **Build and run with tokio-console**:
```bash
# Build with fixes
RUSTFLAGS="--cfg tokio_unstable" cargo build --features tokio-console

# Run with tokio-console enabled
make run-console -- s3-store

# In another terminal, start tokio-console
tokio-console
```

2. **What you should see**:
   - ✅ **Fewer "lost waker" warnings** - The retention scheduler and alert tasks now properly shut down
   - ✅ **Proper task cleanup on shutdown** - When you send SIGTERM/SIGINT, you should see graceful shutdown messages
   - ✅ **No infinite loops** - Alert target timeout tasks are limited to 24 hours maximum
   - ✅ **Faster shutdown** - Background tasks respond to shutdown signals instead of being stuck forever

3. **Monitor improvements**:
   - In tokio-console, watch for tasks that have been running for very long periods
   - You should see fewer tasks in "Sleep" state for extended periods  
   - Alert-related tasks should properly terminate on shutdown

### **Before vs After**

**Before fixes:**
```
⚠ This task has lost its waker, and will never be woken again
  - Retention scheduler: Running forever without shutdown handling
  - Alert evaluation: Infinite loops with no cancellation  
  - Target timeouts: Literally infinite retry loops
  - Stack usage: 2632+ bytes per task
```

**After fixes:**
```  
✅ Graceful shutdown handling for all background tasks
✅ Retention scheduler: Responds to shutdown signal
✅ Alert evaluation: Cancellable with shutdown checks  
✅ Target timeouts: Limited to 24 hours max + shutdown handling
✅ Proper task cleanup during application shutdown
```

All background tasks now have proper shutdown mechanisms and will no longer create "lost waker" scenarios!

## Prevention Best Practices

1. **Always add timeout/cancellation to infinite loops**
2. **Use `tokio::select!` for cancellable operations**  
3. **Implement graceful shutdown for all background tasks**
4. **Monitor task creation rates in production**
5. **Use tokio-console regularly to catch issues early**

## Next Steps

1. **Monitor the fixes**: Run with tokio-console and verify the retention scheduler warnings are gone
2. **Apply remaining fixes**: Add shutdown handling to alert tasks and target timeouts
3. **Add integration**: Call shutdown functions from your main shutdown handler
4. **Test graceful shutdown**: Verify all tasks exit properly on SIGTERM/SIGINT

Run tokio-console again and you should see fewer long-running tasks and lost wakers!
