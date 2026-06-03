# Step Connection Patterns

## Sequential Execution

Steps execute one after the other using `depends_on`.

```
[Step A] ──► [Step B] ──► [Step C]
```

```json
[
  { "id": "a", "name": "Step A", "type": "Task", ... },
  { "id": "b", "name": "Step B", "type": "Task", "depends_on": ["a"], ... },
  { "id": "c", "name": "Step C", "type": "Task", "depends_on": ["b"], ... }
]
```

---

## Parallel Execution (Independent Steps)

Steps with no shared `depends_on` are eligible to run concurrently.

```
          ┌─► [Step B] ─┐
[Step A] ─┤              ├─► [Step D]
          └─► [Step C] ─┘
```

```json
[
  { "id": "a", "name": "Step A", ... },
  { "id": "b", "name": "Step B", "depends_on": ["a"], ... },
  { "id": "c", "name": "Step C", "depends_on": ["a"], ... },
  { "id": "d", "name": "Step D", "depends_on": ["b", "c"], ... }
]
```

---

## Conditional Branching (Decision Step)

A Decision step routes to one of two paths based on a runtime condition.

```
[Step A] ──► [Decision] ──true──►  [Step B]
                        └──false──► [Step C]
```

```json
[
  { "id": "a", "name": "Step A", "type": "Task", ... },
  {
    "id": "d", "name": "Check condition", "type": "Decision",
    "depends_on": ["a"],
    "config": {
      "condition": "{{result.approved}} == true",
      "branches": { "true": "b", "false": "c" }
    }
  },
  { "id": "b", "name": "Approved path", "type": "Task", ... },
  { "id": "c", "name": "Rejected path", "type": "Task", ... }
]
```

---

## Structured Parallelism (Parallel Step)

A Parallel step runs N isolated branch sequences concurrently.

```
           ┌─► [Branch 1: Task1a → Task1b] ─┐
[Step A] ──┤                                 ├─► [Step C]
           └─► [Branch 2: Task2a]           ─┘
```

```json
[
  { "id": "a", "name": "Step A", "type": "Task", ... },
  {
    "id": "p", "name": "Parallel notifications", "type": "Parallel",
    "depends_on": ["a"],
    "config": {
      "completion_policy": "all",
      "branches": [
        { "steps": [{ "id": "t1a", ... }, { "id": "t1b", ... }] },
        { "steps": [{ "id": "t2a", ... }] }
      ]
    }
  },
  { "id": "c", "name": "Step C", "type": "Task", "depends_on": ["p"], ... }
]
```

---

## Timeout and Error Routing

Steps can route to compensating steps on failure instead of failing the entire workflow.

```
[Step A] ──► [Step B] ──failure──► [Compensate B] ──► [Step C]
                      └──success──────────────────────► [Step C]
```

```json
[
  { "id": "a", ... },
  { "id": "b", ..., "on_failure": "compensate_b" },
  { "id": "compensate_b", "name": "Compensate Step B", "type": "Task", ... },
  { "id": "c", "depends_on": ["b", "compensate_b"], ... }
]
```

---

## Sentinels

| Sentinel | Used in | Meaning |
|----------|---------|---------|
| `__end__` | `on_success`, branch targets | Terminate workflow successfully |
| `__fail__` | `on_failure` | Terminate workflow with failure |
| `__retry__` | `on_failure` | Re-execute the same step |
