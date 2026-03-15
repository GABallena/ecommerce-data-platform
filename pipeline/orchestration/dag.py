\
\
\
\
\
\

import enum
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

logger = logging.getLogger("orchestration")

class TaskStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"

@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_seconds: float = 0.0
    attempts: int = 0
    error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status == TaskStatus.SUCCESS

@dataclass
class Task:
\

    task_id: str
    callable: Callable[..., None]
    kwargs: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    max_retries: int = 3
    retry_delay: float = 2.0
    retry_backoff: float = 2.0
    result: TaskResult | None = None

class DAGValidationError(Exception):
\

    pass

class DAG:
\
\
\
\
\
\
\
\

    def __init__(self, dag_id: str = "pipeline"):
        self.dag_id = dag_id
        self._tasks: dict[str, Task] = {}

    def add_task(self, task: Task) -> "DAG":
        if task.task_id in self._tasks:
            raise DAGValidationError(f"Duplicate task_id: {task.task_id}")
        self._tasks[task.task_id] = task
        return self

    @property
    def task_ids(self) -> list[str]:
        return list(self._tasks.keys())

    def validate(self) -> None:
        \
        all_ids = set(self._tasks.keys())

        for task in self._tasks.values():
            missing = set(task.dependencies) - all_ids
            if missing:
                raise DAGValidationError(
                    f"Task '{task.task_id}' depends on unknown tasks: {missing}"
                )

        in_degree: dict[str, int] = {tid: 0 for tid in all_ids}
        adj: dict[str, list[str]] = defaultdict(list)
        for task in self._tasks.values():
            for dep in task.dependencies:
                adj[dep].append(task.task_id)
                in_degree[task.task_id] += 1

        queue = deque(tid for tid, d in in_degree.items() if d == 0)
        visited = 0
        while queue:
            node = queue.popleft()
            visited += 1
            for child in adj[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if visited != len(all_ids):
            raise DAGValidationError(
                "DAG contains a cycle — cannot determine execution order"
            )

    def execution_order(self) -> list[str]:
        \
        self.validate()
        in_degree: dict[str, int] = {tid: 0 for tid in self._tasks}
        adj: dict[str, list[str]] = defaultdict(list)
        for task in self._tasks.values():
            for dep in task.dependencies:
                adj[dep].append(task.task_id)
                in_degree[task.task_id] += 1

        queue = deque(sorted(tid for tid, d in in_degree.items() if d == 0))
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for child in sorted(adj[node]):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
        return order

    def run(self) -> dict[str, TaskResult]:
        \
        order = self.execution_order()
        results: dict[str, TaskResult] = {}
        failed_tasks: set[str] = set()

        logger.info("=" * 60)
        logger.info("DAG RUN  dag_id=%s  tasks=%d", self.dag_id, len(order))
        logger.info("Execution order: %s", " → ".join(order))
        logger.info("=" * 60)

        for task_id in order:
            task = self._tasks[task_id]

            upstream_failed = failed_tasks & set(task.dependencies)
            if upstream_failed:
                result = TaskResult(
                    task_id=task_id,
                    status=TaskStatus.UPSTREAM_FAILED,
                    error=f"Skipped — upstream tasks failed: {upstream_failed}",
                )
                results[task_id] = result
                task.result = result
                failed_tasks.add(task_id)
                logger.warning(
                    "⊘ SKIP  %-30s  (upstream failed: %s)", task_id, upstream_failed
                )
                continue

            result = self._execute_with_retry(task)
            results[task_id] = result
            task.result = result

            if not result.succeeded:
                failed_tasks.add(task_id)

        success = sum(1 for r in results.values() if r.succeeded)
        failed = len(results) - success
        logger.info("=" * 60)
        logger.info(
            "DAG COMPLETE  success=%d  failed=%d  total=%d",
            success,
            failed,
            len(results),
        )
        for tid in order:
            r = results[tid]
            icon = (
                "✓"
                if r.succeeded
                else ("⊘" if r.status == TaskStatus.UPSTREAM_FAILED else "✗")
            )
            logger.info(
                "  %s  %-30s  %s  %.3fs  attempts=%d",
                icon,
                tid,
                r.status.value,
                r.duration_seconds,
                r.attempts,
            )
        logger.info("=" * 60)

        return results

    def _execute_with_retry(self, task: Task) -> TaskResult:
        \
        result = TaskResult(task_id=task.task_id, status=TaskStatus.RUNNING)
        result.start_time = datetime.now(timezone.utc)
        delay = task.retry_delay

        for attempt in range(1, task.max_retries + 1):
            result.attempts = attempt
            try:
                logger.info(
                    "▶ RUN   %-30s  attempt %d/%d",
                    task.task_id,
                    attempt,
                    task.max_retries,
                )
                task.callable(**task.kwargs)
                result.status = TaskStatus.SUCCESS
                result.end_time = datetime.now(timezone.utc)
                result.duration_seconds = (
                    result.end_time - result.start_time
                ).total_seconds()
                logger.info(
                    "✓ DONE  %-30s  %.3fs", task.task_id, result.duration_seconds
                )
                return result
            except Exception as exc:
                err_msg = f"{type(exc).__name__}: {exc}"
                result.error = err_msg
                logger.error(
                    "✗ FAIL  %-30s  attempt %d/%d  %s",
                    task.task_id,
                    attempt,
                    task.max_retries,
                    err_msg,
                )
                if attempt < task.max_retries:
                    logger.info(
                        "  ↻ retrying in %.1fs (backoff=%.1f×)",
                        delay,
                        task.retry_backoff,
                    )
                    time.sleep(delay)
                    delay *= task.retry_backoff

        result.status = TaskStatus.FAILED
        result.end_time = datetime.now(timezone.utc)
        result.duration_seconds = (result.end_time - result.start_time).total_seconds()
        logger.error(
            "✗ EXHAUSTED  %-30s  after %d attempts: %s",
            task.task_id,
            task.max_retries,
            result.error,
        )
        return result
