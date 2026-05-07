#!/usr/bin/env python3
"""
Генератор атмосферных сообщений для Ghost Phone.

Использование:
  python3 generate.py
  python3 generate.py --model qwen2.5:9b
  python3 generate.py --model gemma4:e4b --count 50
  python3 generate.py --model random --count 200 --voice

Файлы:
  prompts.csv              — промпты (id, category, prompt_text, voice_hint)
  messages/messages_NNN.csv — выходные файлы по 100 строк каждый
"""

import csv
import random
import argparse
import logging
import logging.handlers
import time
import re
from datetime import datetime
from pathlib import Path

try:
    import ollama
except ImportError:
    print("Установи ollama: pip install ollama")
    exit(1)

# ─── Настройки ────────────────────────────────────────────────────────────────

BASE_DIR      = Path(__file__).parent
PROMPTS_FILE  = BASE_DIR / "prompts.csv"
MESSAGES_DIR  = BASE_DIR / "messages"
LOG_FILE      = BASE_DIR / "generate.log"

MESSAGES_PER_FILE = 100

AVAILABLE_MODELS = [
    "qwen3.5:9b",
    "mistral-nemo",
    "gemma4:e4b",
]

SYSTEM_PROMPT = """Ты — атмосферный генератор сообщений для арт-проекта в стиле
советского постапокалипсиса, Зоны отчуждения и числовых радиостанций (УВБ-76).

СТИЛЬ:
- Короткие, рубленые фразы. Официозный, холодный, отстранённый тон.
- 2-5 предложений. Никаких эмоций, метафор, эпитетов.
- Уместны: позывные, числа, коды, обрывочные приказы, повторы.
- Пиши на русском. Никаких предисловий, кавычек, пояснений, ремарок в скобках.

ФОРМАТ ВЫВОДА:
- Чистый текст, одной строкой или с переносами — не важно.
- Никаких HTML/XML-тегов, никаких <speak>, <break>, <prosody>.
- Никаких markdown-символов (* _ # ` ~).
- Только то, что должен произнести диктор.
"""

# ─── Логирование ──────────────────────────────────────────────────────────────

def setup_logger():
    logger = logging.getLogger("generate")
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

# ─── CSV helpers ──────────────────────────────────────────────────────────────

def load_prompts():
    """Загружает промпты из CSV."""
    if not PROMPTS_FILE.exists():
        log.error(f"Файл промптов не найден: {PROMPTS_FILE}")
        exit(1)
    prompts = []
    with open(PROMPTS_FILE, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            prompts.append(row)
    log.info(f"Загружено промптов: {len(prompts)}")
    return prompts


def get_next_output_file():
    """Возвращает путь к следующему выходному файлу и стартовый ID."""
    MESSAGES_DIR.mkdir(exist_ok=True)
    existing = sorted(MESSAGES_DIR.glob("messages_*.csv"))

    if not existing:
        return MESSAGES_DIR / "messages_001.csv", 1

    last_file = existing[-1]
    # Считаем строки в последнем файле (не считая заголовок)
    with open(last_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if len(rows) < MESSAGES_PER_FILE:
        # Последний файл не заполнен — дописываем в него
        last_id = int(rows[-1]["id"]) if rows else 0
        return last_file, last_id + 1

    # Последний файл полный — создаём следующий
    num = int(last_file.stem.split("_")[1]) + 1
    new_file = MESSAGES_DIR / f"messages_{num:03d}.csv"
    return new_file, int(rows[-1]["id"]) + 1


def append_to_csv(filepath: Path, row: dict, write_header: bool):
    """Дописывает строку в CSV файл."""
    with open(filepath, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "text", "model", "category", "prompt_id",
                        "audio_done", "created_at"]
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# ─── Генерация ────────────────────────────────────────────────────────────────

def generate_message(model: str, prompt_text: str) -> str | None:
    """Генерирует сообщение через Ollama."""
    try:
        response = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt_text},
            ],
            think=False,
            options={"temperature": 0.9, "top_p": 0.95},
        )
        text = response["message"]["content"].strip()
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        return text if text else None
    except Exception as e:
        log.error(f"Ошибка генерации ({model}): {e}")
        return None

def pick_model(model_arg: str) -> str:
    """Выбирает модель по аргументу."""
    if model_arg == "random":
        return random.choice(AVAILABLE_MODELS)
    return model_arg

# ─── Основная логика ──────────────────────────────────────────────────────────

def build_shuffled_queue(prompts: list, count: int) -> list:
    """
    Строит перемешанную очередь промптов длиной count.
    Сначала равномерно миксуем все промпты (каждый используется примерно одинаково),
    потом финально перемешиваем весь список.
    """
    # Повторяем промпты столько раз, чтобы покрыть count
    repeats = (count // len(prompts)) + 1
    pool = prompts * repeats
    random.shuffle(pool)
    queue = pool[:count]
    random.shuffle(queue)  # второе перемешивание
    return queue


def run(model_arg: str, count: int):
    log.info(f"Старт генерации: model={model_arg}, count={count}")

    prompts = load_prompts()
    queue   = build_shuffled_queue(prompts, count)

    output_file, start_id = get_next_output_file()
    current_id    = start_id
    file_row_count = 0

    # Если файл уже существует — не пишем заголовок
    write_header = not output_file.exists()

    # Считаем сколько строк уже в текущем файле
    if output_file.exists():
        with open(output_file, encoding="utf-8") as f:
            file_row_count = sum(1 for _ in csv.DictReader(f))

    success = 0
    fail    = 0
    
    model = pick_model(model_arg)
    
    for i, prompt in enumerate(queue):    

        log.info(f"[{i+1}/{count}] id={current_id} model={model} "
                 f"category={prompt['category']}")

        text = generate_message(model, prompt["prompt_text"])
        
        if not text:
            fail += 1
            continue

        text = text.replace('\n', ' ').replace('\r', '').strip()
        text = re.sub(r"<[^>]+>", " ", text)          # любые теги если проскочили
        text = re.sub(r"[*_`~#]", "", text)           # markdown-символы
        text = re.sub(r"\s+", " ", text).strip()
        
        row = {
            "id":         current_id,
            "text":       text,
            "model":      model,
            "category":   prompt["category"],
            "prompt_id":  prompt["id"],
            "audio_done": "0",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        append_to_csv(output_file, row, write_header)
        write_header   = False
        file_row_count += 1
        current_id     += 1
        success        += 1

        log.info(f"  ✓ {text[:80]}{'...' if len(text) > 80 else ''}")

        # Переключаемся на новый файл если текущий заполнен
        if file_row_count >= MESSAGES_PER_FILE:
            log.info(f"Файл заполнен: {output_file.name}")
            output_file, current_id = get_next_output_file()
            write_header   = not output_file.exists()
            file_row_count = 0

        # Небольшая пауза чтобы не перегревать GPU на длинных сериях
        time.sleep(0.5)

    log.info(f"Готово. Успешно: {success}, ошибок: {fail}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Генератор атмосферных сообщений для Ghost Phone"
    )
    parser.add_argument(
        "--model",
        default="random",
        help=f"Модель Ollama или 'random'. Доступные: {', '.join(AVAILABLE_MODELS)} (default: random)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Количество сообщений для генерации (default: 100)"
    )
    args = parser.parse_args()

    if args.model not in AVAILABLE_MODELS + ["random"]:
        log.warning(f"Неизвестная модель '{args.model}', будет использована как есть")

    run(args.model, args.count)


if __name__ == "__main__":
    main()
