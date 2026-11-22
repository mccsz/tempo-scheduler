import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from .config import ROOT, RUNTIME, STOP_FILE, TIMEOUT_SECONDS
from .database import wait_for_database


def normalize_desired(value):
    try:
        parsed = float(value)
        if parsed.is_integer():
            return max(2, min(4, int(parsed)))
        else:
            return 2
    except (TypeError, ValueError, OverflowError):
        return 2


class WorkerSupervisor:

    def __init__(self):
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.connection = wait_for_database()
        self.children = {}
        self.stopping = False

    def request_stop(self, *_):
        self.stopping = True

    def save_processes(self):
        snapshot = {
            'supervisorPid': os.getpid(),
            'updatedAt': datetime.now(timezone.utc).isoformat(),
            'children': [
                {
                    'pid': worker_state['process'].pid,
                    'workerId': f'WORKER_{slot}',
                    'slot': slot,
                    'startedAt': worker_state['startedAt'],
                }
                for slot, worker_state in self.children.items()
            ],
        }
        temporary = RUNTIME / 'supervisor-children.json.tmp'
        temporary.write_text(json.dumps(snapshot, indent=2), encoding='utf-8')
        temporary.replace(RUNTIME / 'supervisor-children.json')

    def drain_worker(self, slot, worker_state):
        if not worker_state.get('draining'):
            (RUNTIME / f'drain-WORKER_{slot}.request').write_text('drain', encoding='utf-8')
            worker_state['draining'] = time.monotonic()
            print(f'[supervisor] WORKER_{slot}: очікування завершення роботи.', flush=True)

    def run(self):
        if not self.connection.execute(
            'SELECT pg_try_advisory_lock(740002) AS acquired'
        ).fetchone()['acquired']:
            self.connection.close()
            print('[supervisor] Інший супервізор уже працює.', flush=True)
            return
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        print('[supervisor] Резервні Python-процеси WORKER_3/4 за попитом.', flush=True)
        self.save_processes()
        try:
            while not self.stopping and (not STOP_FILE.exists()):
                for slot, worker_state in list(self.children.items()):
                    if worker_state['process'].poll() is not None:
                        del self.children[slot]
                        (RUNTIME / f'drain-WORKER_{slot}.request').unlink(missing_ok=True)
                        self.save_processes()
                setting = self.connection.execute(
                    "SELECT value FROM settings WHERE key='desired_workers'"
                ).fetchone()
                desired_workers = normalize_desired(setting['value'] if setting else 2)
                for slot in (3, 4):
                    worker_state = self.children.get(slot)
                    if slot <= desired_workers and worker_state is None:
                        worker_id = f'WORKER_{slot}'
                        existing_worker = self.connection.execute(
                            "SELECT id FROM nodes WHERE id=%s AND last_seen>now()-interval '12 seconds'",
                            (worker_id,),
                        ).fetchone()
                        if existing_worker:
                            continue
                        (RUNTIME / f'drain-{worker_id}.request').unlink(missing_ok=True)
                        environment = dict(
                            os.environ,
                            WORKER_ID=worker_id,
                            WORKER_SLOT=str(slot),
                            PYTHONUNBUFFERED='1',
                            PYTHONUTF8='1',
                        )
                        process = subprocess.Popen(
                            [sys.executable, '-m', 'backend.python.worker'],
                            cwd=ROOT,
                            env=environment,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
                        )
                        self.children[slot] = {
                            'process': process,
                            'startedAt': datetime.now(timezone.utc).isoformat(),
                        }
                        print(
                            f'[supervisor] Запущено {worker_id}, Python PID {process.pid}.',
                            flush=True,
                        )
                        self.save_processes()
                    elif slot > desired_workers and worker_state:
                        self.drain_worker(slot, worker_state)
                time.sleep(0.5)
        finally:
            for slot, worker_state in self.children.items():
                self.drain_worker(slot, worker_state)
            deadline = time.monotonic() + TIMEOUT_SECONDS + 20
            while self.children and time.monotonic() < deadline:
                for slot, worker_state in list(self.children.items()):
                    if worker_state['process'].poll() is not None:
                        del self.children[slot]
                        (RUNTIME / f'drain-WORKER_{slot}.request').unlink(missing_ok=True)
                if self.children:
                    time.sleep(0.2)
            for worker_state in self.children.values():
                worker_state['process'].terminate()
                worker_state['process'].wait(5)
            self.children.clear()
            self.save_processes()
            self.connection.close()


def main():
    WorkerSupervisor().run()


if __name__ == '__main__':
    main()
