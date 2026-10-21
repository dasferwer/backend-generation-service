"""Проверка фактических дочерних процессов внутри Linux-контейнера API."""

from pathlib import Path

workers = []
for path in Path("/proc").glob("[0-9]*/cmdline"):
    try:
        args = path.read_bytes().split(b"\0")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
    if b"--multiprocessing-fork" in args and any(b"spawn_main" in arg for arg in args):
        workers.append(int(path.parent.name))
print("Процессы API:", sorted(workers))
if len(workers) != 4:
    raise SystemExit(f"Ожидалось четыре процесса, найдено {len(workers)}")
