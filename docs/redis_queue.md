## Queue Structure in Redis

The priority queue system uses multiple Redis lists to organize tasks by priority level. Here's how it works:

```
Redis Storage Structure:
┌─────────────────────────────────────────────────────────┐
│                     Redis Instance                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  tasks:priority:100 (CRITICAL)  [T1] [T2] [T3] →        │
│                                                         │
│  tasks:priority:75  (HIGH)      [T4] [T5] →             │
│                                                         │
│  tasks:priority:50  (NORMAL)    [T6] [T7] [T8] [T9] →   │
│                                                         │
│  tasks:priority:25  (LOW)       [T10] →                 │
│                                                         │
│  tasks:priority:0   (IDLE)      [T11] [T12] →           │
│                                                         │
│  tasks:queue_stats (Hash)                               │
│    - last_enqueue:100 = 1234567890                      │
│    - last_dequeue:100 = 1234567880                      │
│    - last_enqueue:75  = 1234567885                      │
│    - ...                                                │
└─────────────────────────────────────────────────────────┘

→ indicates LPUSH direction (newest tasks)
Tasks are dequeued from the opposite end (BRPOP)
```

## Task Flow Through the Queue

```
Enqueue Process:
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Client    │────►│ Redis        │────►│ Redis List      │
│ enqueue()   │     │ TaskQueue    │     │ (by priority)   │
└─────────────┘     └──────────────┘     └─────────────────┘
                           │
                           ▼
                    Determine queue key
                    based on priority
                           │
                           ▼
                    LPUSH to specific
                    priority queue

Dequeue Process:
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Worker    │◄────│ Redis        │◄────│ Selected Queue  │
│ dequeue()   │     │ TaskQueue    │     │ (BRPOP)         │
└─────────────┘     └──────────────┘     └─────────────────┘
                           ▲
                           │
                    Weighted random
                    selection algorithm
```

## Priority Selection Algorithm

The dequeue operation uses a weighted random selection algorithm to choose which priority queue to pull from:

```
Priority Selection Process:

Step 1: Check Non-Empty Queues
┌─────────────────────────────────────┐
│ Priority │ Queue Size │ Base Weight │
├──────────┼────────────┼─────────────┤
│   100    │     3      │    1.0      │
│    75    │     2      │    0.7      │
│    50    │     5      │    0.4      │
│    25    │     0      │    0.2      │ ← Skipped (empty)
│     0    │     2      │    0.1      │
└─────────────────────────────────────┘

Step 2: Calculate Effective Weights (with anti-starvation)
┌─────────────────────────────────────────────────────┐
│ Priority │ Wait Time │ Boost Factor │ Final Weight  │
├──────────┼───────────┼──────────────┼───────────────┤
│   100    │    5s     │     1.0      │     1.0       │
│    75    │   10s     │     1.0      │     0.7       │
│    50    │   45s     │     1.5      │     0.6       │ ← Boosted!
│     0    │   90s     │     2.0      │     0.2       │ ← Max boost
└─────────────────────────────────────────────────────┘

Step 3: Weighted Random Selection
┌─────────────────────────────────────────┐
│         Probability Distribution        │
├─────────────────────────────────────────┤
│ P(100) = 1.0/2.5 = 40%   ████████       │
│ P(75)  = 0.7/2.5 = 28%   ██████         │
│ P(50)  = 0.6/2.5 = 24%   █████          │
│ P(0)   = 0.2/2.5 = 8%    ██             │
└─────────────────────────────────────────┘
```

## Anti-Starvation Mechanism

The anti-starvation mechanism prevents low-priority tasks from waiting indefinitely:
```
Starvation Prevention Timeline:

Time →
0s        30s       60s       90s      120s
│─────────│─────────│─────────│─────────│
│         │         │         │         │
│ Normal  │ Boost   │ 1.5x    │ 2x      │
│ Weight  │ Starts  │ Boost   │ Boost   │
│         │         │         │ (max)   │

Example: IDLE priority (base weight 0.1)
- 0-30s:   weight = 0.1
- 30-60s:  weight = 0.1 × 1.5 = 0.15
- 60-90s:  weight = 0.1 × 1.75 = 0.175
- 90s+:    weight = 0.1 × 2.0 = 0.2 (capped)
```

## Complete Operation Example

```
Scenario: Multiple tasks with different priorities

Initial State:
┌───────────────────────────────────────────────┐
│ CRITICAL: [Task A] [Task B]                   │
│ HIGH:     [Task C]                            │
│ NORMAL:   [Task D] [Task E] [Task F]          │
│ LOW:      (empty)                             │
│ IDLE:     [Task G]                            │
└───────────────────────────────────────────────┘

Worker Dequeue Sequence (example):
1. Select CRITICAL (40% chance) → Dequeue Task B
2. Select CRITICAL (40% chance) → Dequeue Task A
3. Select NORMAL (24% chance) → Dequeue Task F
4. Select HIGH (28% chance) → Dequeue Task C
5. Select NORMAL (24% chance) → Dequeue Task E
6. Select IDLE (8% chance) → Dequeue Task G
7. Select NORMAL (24% chance) → Dequeue Task D

Note: Actual sequence varies due to random selection
```
