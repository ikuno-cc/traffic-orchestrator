import os
import threading
import time
from typing import Dict

from app.supabase_sync import claim_next_queued_request, fetch_services_from_supabase, is_supabase_enabled
from workers.tasks import process_dispatch_request

POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "1.0"))
REFRESH_SECONDS = float(os.getenv("WORKER_CONFIG_REFRESH_SECONDS", "5.0"))


class ServiceWorker(threading.Thread):
    def __init__(self, service_id: str, slot: int):
        super().__init__(daemon=True)
        self.service_id = service_id
        self.slot = slot
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.is_set():
            record = claim_next_queued_request(self.service_id)
            if not record:
                time.sleep(POLL_SECONDS)
                continue
            process_dispatch_request(record)


def run():
    if not is_supabase_enabled():
        raise RuntimeError("Supabase is not configured")

    workers: Dict[str, ServiceWorker] = {}

    while True:
        services = fetch_services_from_supabase()
        desired: Dict[str, int] = {}
        for s in services:
            if not bool(s.get("enabled", True)):
                continue
            desired[str(s["id"])] = max(1, int(s.get("worker_count") or 1))

        current_keys = set(workers.keys())
        desired_keys = set()
        for service_id, count in desired.items():
            for i in range(count):
                desired_keys.add(f"{service_id}:{i}")

        for key in sorted(current_keys - desired_keys):
            workers[key].stop()
            del workers[key]

        for key in sorted(desired_keys - current_keys):
            service_id, slot = key.split(":", 1)
            w = ServiceWorker(service_id, int(slot))
            w.start()
            workers[key] = w

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    run()
