import multiprocessing as mp
import os
import signal
import time
import psutil
import psycopg
from psycopg.types.json import Jsonb
from .config import RUNTIME, STOP_FILE, TIMEOUT_SECONDS, WORKER_ID, WORKER_SLOT
from .database import wait_for_database
from .optimizer import optimize


def claim_next_job(connection, worker_id):
    return connection.execute(
        """
        UPDATE jobs AS job SET status='running', progress=1, stage='Воркер отримав задачу',
            worker_id=%s, started_at=now(), heartbeat_at=now(), attempts=attempts+1
        FROM (
            SELECT id FROM jobs WHERE status='queued' AND cancel_requested=false
            ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
        ) AS next WHERE job.id=next.id RETURNING job.*
    """,
        (worker_id,),
    ).fetchone()


def compute(input_data, channel):
    try:
        result = optimize(
            input_data,
            on_progress=lambda progress, stage: channel.send(('progress', progress, stage)),
        )
        channel.send(('result', result))
    except BaseException as error:
        channel.send(('error', str(error)))
    finally:
        channel.close()


def stop_compute(process):
    if process.is_alive():
        process.terminate()
    process.join(3)
    if process.is_alive():
        process.kill()
        process.join(3)


class Worker:

    def __init__(self):
        if not 1 <= WORKER_SLOT <= 4:
            raise ValueError('WORKER_SLOT має бути від 1 до 4.')
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.drain_file = RUNTIME / f'drain-{WORKER_ID}.request'
        self.stopping = False
        self.connection = wait_for_database()
        self.parent_metrics = psutil.Process()
        self.parent_metrics.cpu_percent()
        self.active_id, self.child_metrics = (None, None)
        self.enabled = WORKER_SLOT <= 2
        self.last_heartbeat = 0.0

    def request_stop(self, *_):
        self.stopping = True

    def heartbeat(self, force=False):
        if not force and time.monotonic() - self.last_heartbeat < 2:
            return
        cpu, memory = (self.parent_metrics.cpu_percent(), self.parent_metrics.memory_info().rss)
        if self.child_metrics:
            try:
                cpu += self.child_metrics.cpu_percent()
                memory += self.child_metrics.memory_info().rss
            except psutil.Error:
                pass
        self.connection.execute(
            """
            INSERT INTO nodes(id,kind,active_job_id,last_seen,cpu_percent,memory_mb)
            VALUES(%s,%s,%s,now(),%s,%s)
            ON CONFLICT(id) DO UPDATE SET kind=EXCLUDED.kind,active_job_id=EXCLUDED.active_job_id,
                last_seen=now(),cpu_percent=EXCLUDED.cpu_percent,memory_mb=EXCLUDED.memory_mb
        """,
            (
                WORKER_ID,
                'worker' if self.enabled or self.active_id else 'standby',
                self.active_id,
                round(cpu, 1),
                round(memory / 1048576),
            ),
        )
        self.last_heartbeat = time.monotonic()

    def execute_job(self, job):
        self.active_id = job['id']
        context = mp.get_context('spawn')
        reader, writer = context.Pipe(duplex=False)
        process = context.Process(
            target=compute, args=(job['input'], writer), name=f'compute-{WORKER_ID}'
        )
        process.start()
        writer.close()
        self.child_metrics = psutil.Process(process.pid)
        self.child_metrics.cpu_percent()
        began, last_sync = (time.monotonic(), 0.0)
        progress, stage, result = (2, 'Підготовка Python-оптимізатора', None)

        def has_message():
            try:
                return reader.poll()
            except (EOFError, OSError):
                return False

        try:
            self.heartbeat(True)
            print(f"[{WORKER_ID}] Python: початок {job['id']}", flush=True)
            while True:
                while has_message():
                    try:
                        message = reader.recv()
                    except (EOFError, OSError):
                        break
                    if message[0] == 'progress':
                        progress, stage = (min(99, message[1]), message[2])
                    elif message[0] == 'result':
                        result = message[1]
                        break
                    elif message[0] == 'error':
                        raise RuntimeError(message[1])
                now = time.monotonic()
                if now - began >= TIMEOUT_SECONDS:
                    raise TimeoutError(
                        f'Перевищено ліміт виконання {TIMEOUT_SECONDS} с. Python-процес зупинено.'
                    )
                if result is not None:
                    break
                if now - last_sync >= 0.35:
                    row = self.connection.execute(
                        """
                        UPDATE jobs SET progress=%s,stage=%s,heartbeat_at=now()
                        WHERE id=%s AND worker_id=%s AND status='running' AND attempts=%s
                        RETURNING cancel_requested
                    """,
                        (progress, stage, job['id'], WORKER_ID, job['attempts']),
                    ).fetchone()
                    if not row or row['cancel_requested']:
                        raise RuntimeError('TASK_CANCELLED')
                    last_sync = now
                self.heartbeat()
                if not process.is_alive():
                    if has_message():
                        continue
                    raise RuntimeError(
                        f'Обчислювальний процес завершився без результату ({process.exitcode}).'
                    )
                time.sleep(0.08)
            elapsed = round((time.monotonic() - began) * 1000)
            self.connection.execute(
                """
                UPDATE jobs SET status='completed',progress=100,stage='Оптимізований розклад готовий',
                    result=%s,execution_ms=%s,completed_at=now(),heartbeat_at=now()
                WHERE id=%s AND worker_id=%s AND status='running' AND cancel_requested=false AND attempts=%s
            """,
                (Jsonb(result), elapsed, job['id'], WORKER_ID, job['attempts']),
            )
            print(f"[{WORKER_ID}] Завершено {job['id']} за {elapsed} мс", flush=True)
        except Exception as error:
            stop_compute(process)
            cancelled = str(error) == 'TASK_CANCELLED'
            if cancelled:
                status = 'cancelled'
            else:
                status = 'failed'
            if cancelled:
                stage = 'Скасовано користувачем'
            elif isinstance(error, TimeoutError):
                stage = 'Перевищено ліміт часу'
            else:
                stage = 'Помилка обчислення'
            self.connection.execute(
                """
                UPDATE jobs SET status=CASE WHEN cancel_requested THEN 'cancelled' ELSE %s END,
                    stage=CASE WHEN cancel_requested THEN 'Скасовано користувачем' ELSE %s END,
                    error=CASE WHEN cancel_requested OR %s='cancelled' THEN NULL ELSE %s END,
                    execution_ms=%s,completed_at=now(),heartbeat_at=now()
                WHERE id=%s AND worker_id=%s AND status='running' AND attempts=%s
            """,
                (
                    status,
                    stage,
                    status,
                    str(error),
                    round((time.monotonic() - began) * 1000),
                    job['id'],
                    WORKER_ID,
                    job['attempts'],
                ),
            )
            print(f"[{WORKER_ID}] {stage}: {job['id']}", flush=True)
        finally:
            stop_compute(process)
            reader.close()
            process.close()
            self.active_id, self.child_metrics = (None, None)
            self.heartbeat(True)

    def run(self):
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        print(
            f'[{WORKER_ID}] Python-воркер готовий, слот {WORKER_SLOT}, PostgreSQL SKIP LOCKED.',
            flush=True,
        )
        try:
            while not self.stopping and (not STOP_FILE.exists()) and (not self.drain_file.exists()):
                try:
                    setting = self.connection.execute(
                        "SELECT value FROM settings WHERE key='desired_workers'"
                    ).fetchone()
                    self.enabled = WORKER_SLOT <= max(
                        2, min(4, int(setting['value'] if setting else 2))
                    )
                    self.heartbeat()
                    if self.enabled:
                        job = claim_next_job(self.connection, WORKER_ID)
                    else:
                        job = None
                    if job:
                        self.execute_job(job)
                    else:
                        time.sleep(0.35)
                except (psycopg.Error, OSError) as error:
                    print(f'[{WORKER_ID}] БД: {error}', flush=True)
                    self.connection.close()
                    self.connection = wait_for_database()
                    time.sleep(0.5)
        finally:
            try:
                self.connection.execute('DELETE FROM nodes WHERE id=%s', (WORKER_ID,))
            finally:
                self.connection.close()


def run_worker():
    Worker().run()


if __name__ == '__main__':
    mp.freeze_support()
    run_worker()
