#!/usr/bin/env python3
"""
Главный скрипт для cron — генерирует сообщения и озвучивает их.

Логика:
  1. Считает сколько необработанных сообщений в messages/
  2. Если меньше LOW_WATERMARK — запускает generate.py (пополняет запас)
  3. Считает сколько wav файлов в очереди для RPi
  4. Если меньше LOW_QUEUE — запускает tts.py (озвучивает порцию)

Cron (каждые 30 минут):
  */30 * * * * /usr/bin/python3 /home/fff/ghost/run.py >> /home/fff/ghost/cron.log 2>&1

Переменные окружения (опционально):
  GHOST_OUTPUT=/path/to/rpi/queue  — если RPi монтирован через sshfs
"""

import subprocess
import logging
import logging.handlers
import sys
from pathlib import Path
import csv

# ─── Настройки ────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).parent
LOG_FILE    = BASE_DIR / "run.log"
PYTHON      = sys.executable

GENERATE_SCRIPT = BASE_DIR / "generate.py"
TTS_SCRIPT      = BASE_DIR / "tts.py"

MESSAGES_DIR = BASE_DIR / "messages"
OUTPUT_DIR   = BASE_DIR / "queue"       # переопределяется через env GHOST_OUTPUT

# Порог: если необработанных текстов меньше — генерируем ещё
LOW_WATERMARK_TEXT = 50
GENERATE_COUNT     = 100   # сколько генерировать за раз

# Порог: если wav в очереди меньше — озвучиваем ещё
LOW_WATERMARK_WAV  = 10
TTS_LIMIT          = 20    # сколько озвучивать за раз

# ─── Логирование ──────────────────────────────────────────────────────────────

def setup_logger():
    logger = logging.getLogger("run")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=1 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

log = setup_logger()

# ─── Счётчики ─────────────────────────────────────────────────────────────────

def count_pending_text() -> int:
    """Считает строки с audio_done=0 во всех CSV."""
    count = 0
    for f in MESSAGES_DIR.glob("messages_*.csv"):
        with open(f, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if row.get("audio_done", "0") == "0":
                    count += 1
    return count


def count_queue_wav(output_dir: Path) -> int:
    """Считает wav файлы в очереди для RPi."""
    if not output_dir.exists():
        return 0
    return len(list(output_dir.glob("*.wav")))


def run_script(script: Path, extra_args: list = None):
    """Запускает скрипт и логирует результат."""
    cmd = [PYTHON, str(script)] + (extra_args or [])
    log.info(f"Запуск: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(f"  {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(f"  {line}")
    if result.returncode != 0:
        log.error(f"Скрипт завершился с кодом {result.returncode}")
    return result.returncode == 0

# ─── Основная логика ──────────────────────────────────────────────────────────

def main():
    import os
    output_dir = Path(os.environ.get("GHOST_OUTPUT", str(OUTPUT_DIR)))
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("─── Ghost Phone pipeline ───")

    # 1. Проверяем запас текстов
    pending_text = count_pending_text()
    log.info(f"Необработанных текстов: {pending_text}")

    if pending_text < LOW_WATERMARK_TEXT:
        log.info(f"Мало текстов (< {LOW_WATERMARK_TEXT}), генерируем {GENERATE_COUNT}...")
        run_script(GENERATE_SCRIPT, ["--count", str(GENERATE_COUNT)])
    else:
        log.info("Запас текстов достаточен, пропускаем генерацию")

    # 2. Проверяем очередь wav для RPi
    queue_wav = count_queue_wav(output_dir)
    log.info(f"WAV в очереди для RPi: {queue_wav} (путь: {output_dir})")

    if queue_wav < LOW_WATERMARK_WAV:
        log.info(f"Мало wav (< {LOW_WATERMARK_WAV}), озвучиваем {TTS_LIMIT}...")
        run_script(TTS_SCRIPT, [
            "--output", str(output_dir),
            "--limit",  str(TTS_LIMIT),
        ])
    else:
        log.info("Очередь wav достаточна, пропускаем TTS")

    log.info("─── Готово ───")


if __name__ == "__main__":
    main()
