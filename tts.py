#!/usr/bin/env python3
"""
TTS обработчик для Ghost Phone.

Берёт необработанные строки из messages/messages_NNN.csv,
генерирует речь через Silero TTS v5, накладывает фон (UVB или sox помехи),
кладёт готовый wav в output_dir, помечает audio_done=1 в CSV.

Использование:
  python3 tts.py
  python3 tts.py --output /home/pi/queue
  python3 tts.py --limit 10
"""

import csv
import random
import re
import argparse
import logging
import logging.handlers
import subprocess
import tempfile
import time
import html
import shutil
from pathlib import Path
from datetime import datetime
from xml.etree import ElementTree

import torch
import torchaudio

# ─── Настройки ────────────────────────────────────────────────────────────────

BASE_DIR     = Path(__file__).parent
MESSAGES_DIR = BASE_DIR / "messages"
UVB_DIR      = BASE_DIR / "sounds" / "uvb"   # wav файлы с реальным UVB эфиром
LOG_FILE     = BASE_DIR / "tts.log"
PICKUP_DIR   = BASE_DIR / "sounds" / "pickup"
DEFAULT_OUTPUT = BASE_DIR / "queue"
PIPER_MODELS_DIR = BASE_DIR / "piper_models"
DIALTONE_DIR = BASE_DIR / "sounds" / "dialtone"

DIALTONE_PROBABILITY = 0.85

UVB_ONLY_PROBABILITY = 0.08         # 8% запусков цикла вместо голоса — чистый эфир
UVB_ONLY_MIN_SEC     = 15
UVB_ONLY_MAX_SEC     = 45

TTS_ENGINES = [
    "silero",
    "rhvoice",
    "piper"
]

TTS_WEIGHTS = [
    0.55,
    0.15,
    0.30
]

PIPER_VOICES = [
    "ru_RU-ruslan-medium",
    "ru_RU-dmitri-medium",
    "ru_RU-irina-medium",
    "ru_RU-denis-medium"
]

RHVOICE_VOICES = [
    "anna",
    "aleksandr",
    "elena",
    "irina"
]

SILERO_SPEAKERS = [
    "aidar",    # мужской, нейтральный
    "baya",     # женский, спокойный
    "kseniya",  # женский
    "xenia",    # женский, чёткий
    "eugene",   # мужской, чуть грубее
]

# distant_pa варианты доминируют (60%), broken_signal только в 10%
FX_WEIGHTS = {
    "distant_pa":      0.40,
    "distant_pa_wide": 0.20,
    "close_radio":     0.20,
    "tape_recorder":   0.10,
    "broken_signal":   0.10,
}

# ─── Логирование ──────────────────────────────────────────────────────────────

def setup_logger():
    logger = logging.getLogger("tts")
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

# ─── Фильтры ffmpeg ────────────────────────────────────────────────────────────

def _fx_chain_distant_pa() -> str:
    """Громкоговоритель издалека — эталонный эффект."""
    return (
        "aformat=channel_layouts=mono,"
        "highpass=f=200,"
        "lowpass=f=2000,"
        "acompressor=threshold=-20dB:ratio=5:attack=10:release=200,"
        "aecho=0.85:0.9:55|110|230|480:0.45|0.35|0.25|0.15,"
        "aecho=0.75:0.88:20|45:0.3|0.2,"
        "volume=-4dB,"
        "aformat=channel_layouts=stereo,"
        "extrastereo=m=1.5"
    )

def _fx_chain_distant_pa_wide() -> str:
    """distant_pa с ещё большим пространством — для открытых сцен."""
    return (
        "aformat=channel_layouts=mono,"
        "highpass=f=180,"
        "lowpass=f=1800,"
        "acompressor=threshold=-22dB:ratio=4:attack=15:release=280,"
        "aecho=0.88:0.92:80|180|350|700|1200:0.5|0.4|0.3|0.2|0.12,"
        "aecho=0.75:0.88:25|55:0.3|0.2,"
        "volume=-6dB,"
        "aformat=channel_layouts=stereo,"
        "extrastereo=m=1.8"
    )

def _fx_chain_close_radio() -> str:
    """Близкое радио — как рация в руке. Без реверба, чётко."""
    return (
        "aformat=channel_layouts=mono,"
        "highpass=f=350,"
        "lowpass=f=2800,"
        "acompressor=threshold=-18dB:ratio=7:attack=5:release=80:makeup=3,"
        "asoftclip=type=tanh:threshold=0.85,"        # замена aeval-сатурации
        "treble=g=1.5:f=2200,"
        "volume=0dB,"
        "alimiter=limit=0.92,"
        "vibrato=f=2.5:d=0.08,"                      # очень лёгкий дрейф частоты
        "aformat=channel_layouts=stereo"
    )

def _fx_chain_tape_recorder() -> str:
    """Магнитофонная запись — без агрессии."""
    return (
        "aformat=channel_layouts=mono,"
        "highpass=f=140,"
        "lowpass=f=4200,"
        "vibrato=f=0.6:d=0.04,"                      # wow
        "vibrato=f=5.5:d=0.02,"                      # flutter
        "acompressor=threshold=-18dB:ratio=3:attack=20:release=400,"
        "treble=g=-3:f=5000,"
        "volume=1dB,"
        "aformat=channel_layouts=stereo"
    )

def _fx_chain_broken_signal() -> str:
    """Распадающийся сигнал — СИЛЬНО смягчён."""
    return (
        "aformat=channel_layouts=mono,"
        "highpass=f=400,"
        "lowpass=f=2600,"
        "acompressor=threshold=-20dB:ratio=6:attack=3:release=60:makeup=3,"
        "acrusher=bits=10:samples=1:mode=log:aa=1,"  # было bits=7, samples=2 — агрессивно
        "tremolo=f=4:d=0.15,"                         # было f=11:d=0.55 — дёргало как током
        "aphaser=in_gain=0.55:out_gain=0.78:delay=3:decay=0.35:speed=0.4,"
        "volume=2dB,"
        "alimiter=limit=0.92,"
        "aformat=channel_layouts=stereo"
    )

FX_BUILDERS = {
    "distant_pa":      _fx_chain_distant_pa,
    "distant_pa_wide": _fx_chain_distant_pa_wide,
    "close_radio":     _fx_chain_close_radio,
    "tape_recorder":   _fx_chain_tape_recorder,
    "broken_signal":   _fx_chain_broken_signal,
}

# ─── TTS движки ───────────────────────────────────────────────────────────────

def synthesize_rhvoice(text_plain: str, voice: str, out_path: Path) -> bool:
    """Синтез через RHVoice. Делает именно тот механический советский звук."""
    try:
        result = subprocess.run(
            ["RHVoice-test", "-p", voice, "-o", str(out_path)],
            input=text_plain, capture_output=True, text=True, timeout=60,
        )
        return result.returncode == 0 and out_path.exists()
    except Exception as e:
        log.error(f"RHVoice error: {e}")
        return False


def synthesize_piper(text_plain: str, voice: str, out_path: Path) -> bool:
    """Синтез через piper."""
    model = PIPER_MODELS_DIR / f"{voice}.onnx"
    if not model.exists():
        log.error(f"Piper модель не найдена: {model}")
        return False
    try:
        # piper читает stdin, пишет raw wav в stdout
        result = subprocess.run(
            ["piper", "--model", str(model), "--output_file", str(out_path)],
            input=text_plain, capture_output=True, text=True, timeout=60,
        )
        return result.returncode == 0 and out_path.exists()
    except Exception as e:
        log.error(f"Piper error: {e}")
        return False


def pick_tts_engine(category: str) -> str:
    return random.choices(TTS_ENGINES, weights=TTS_WEIGHTS, k=1)[0]


def synthesize_dispatch(engine, silero_model, text_plain, out_path) -> bool:
    if engine == "silero":
        v = random.choice(SILERO_SPEAKERS)
        log.info(f"silero synthesize by {v}")
        return synthesize_silero(silero_model, text_plain, v, out_path), engine
    if engine == "rhvoice":
        v = random.choice(RHVOICE_VOICES)
        log.info(f"rhvoice synthesize by {v}")
        return synthesize_rhvoice(text_plain, v, out_path), engine
    if engine == "piper":
        v = random.choice(PIPER_VOICES)
        log.info(f"piper synthesize by {v}")
        return synthesize_piper(text_plain, v, out_path), engine
    return False, False


def load_silero_model():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"Устройство: {device}")
    log.info("Загружаем Silero TTS v5...")
    model, _ = torch.hub.load(
        repo_or_dir="snakers4/silero-models",
        model="silero_tts",
        language="ru",
        speaker="v5_ru",
    )
    model.to(device)
    log.info(f"Модель загружена. Доступные спикеры: {model.speakers}")
    return model


def synthesize_silero(model, text: str, speaker: str, out_path: Path) -> bool:
    """Синтезирует речь через Silero, сохраняет в out_path (моно 48000Hz)."""
    try:
        audio = model.apply_tts(
            text=text,
            speaker=speaker,
            sample_rate=48000,
            put_accent=True,
            put_yo=True,
        )
        torchaudio.save(str(out_path), audio.unsqueeze(0), 48000)
        return True
    except Exception as e:
        log.error(f"Ошибка Silero: {e}")
        return False

# ─── Фон: sox помехи ─────────────────────────────────────────

def get_sox_background(duration_sec: float, out_path: Path) -> bool:
    import shutil
    sox_bin = shutil.which("sox")
    if not sox_bin:
        log.info("sox не найден, используем ffmpeg шум")
        return _get_ffmpeg_noise(duration_sec, out_path)

    # Оставили только радио-подобные фоны, никаких «белошумных» пресетов
    noise_type = random.choices(
        ["radio_crackle", "shortwave_hiss", "distant_static"],
        weights=[0.55, 0.25, 0.20], k=1
    )[0]

    log.info(f"Sox фон: {noise_type} ({duration_sec:.1f}s)")
    mono_path = Path(tempfile.mkstemp(suffix=".wav")[1])

    try:
        if noise_type == "radio_crackle":
            # Основной пресет — узкая АМ-полоса, слышно «эфир»
            cmd = [
                sox_bin, "-n", "-r", "48000", "-c", "1", str(mono_path),
                "synth", str(duration_sec), "pinknoise", "vol", "0.045",
                "bandpass", "1200", "700h",
                "tremolo", "0.3", "15",            # медленное дыхание эфира
            ]
        elif noise_type == "shortwave_hiss":
            # Коротковолновый шум, очень тихий, высокочастотный
            cmd = [
                sox_bin, "-n", "-r", "48000", "-c", "1", str(mono_path),
                "synth", str(duration_sec), "pinknoise", "vol", "0.025",
                "bandpass", "2000", "1500h",
                "tremolo", "0.7", "25",
            ]
        else:  # distant_static
            # «Дальний» приглушённый фон — для distant_pa связки
            cmd = [
                sox_bin, "-n", "-r", "48000", "-c", "1", str(mono_path),
                "synth", str(duration_sec), "brownnoise", "vol", "0.06",
                "lowpass", "800",
                "tremolo", "0.2", "8",
            ]

        r = subprocess.run(cmd, capture_output=True, text=False)
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", errors="replace")[-400:]
            log.warning(f"sox ошибка: {err}")
            mono_path.unlink(missing_ok=True)
            return _get_ffmpeg_noise(duration_sec, out_path)

        ok, err = run_ffmpeg([
            "-i", str(mono_path),
            "-ac", "2", "-ar", "48000", str(out_path)
        ])
        return ok
    finally:
        mono_path.unlink(missing_ok=True)


def _get_ffmpeg_noise(duration_sec: float, out_path: Path) -> bool:
    """Fallback: генерация шума через ffmpeg без sox."""
    noise_type = random.choice(["white", "brown", "pink"])
    log.info(f"ffmpeg шум: {noise_type}noise")
    ok, err = run_ffmpeg([
        "-f", "lavfi",
        "-t", str(duration_sec),
        "-i", f"anoisesrc=color={noise_type}:amplitude=0.04",
        "-ar", "48000", "-ac", "2",
        str(out_path),
    ])
    if not ok:
        log.error(f"get_ffmpeg_noise raw error: {err}")
        return False, ""
    return True

# ─── Сборка финального файла ──────────────────────────────────────────────────
#
# Структура:
#   [0.5-1.0s тишина] [0.5s фон без голоса] [голос поверх фона] [1.0-2.0s хвост фона]

def get_audio_duration(wav_path: Path) -> float:
    result = subprocess.run([
        "ffprobe", "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "csv=p=0",
        str(wav_path),
    ], capture_output=True, text=True)
    try:
        return float(result.stdout.strip())
    except Exception:
        return 3.0


def pick_voice_fx() -> tuple[str, str]:
    names   = list(FX_WEIGHTS.keys())
    weights = [FX_WEIGHTS[n] for n in names]
    chosen  = random.choices(names, weights=weights, k=1)[0]
    return chosen, FX_BUILDERS[chosen]()


def process_voice_radio(voice_wav: Path, out_path: Path, category: str) -> bool:
    """Обрабатывает голос. Возвращает (ok)."""
    fx_name, af = pick_voice_fx()
    log.info(f"  voice_fx={fx_name}")
    ok, err = run_ffmpeg([
        "-i", str(voice_wav),
        "-af", af,
        "-ar", "48000", "-ac", "2",
        str(out_path),
    ])
    if not ok:
        log.error(f"voice FX error: {err}")
        return False, 
        
    return True


def build_final_wav(voice_wav: Path, out_path: Path, category: str) -> bool:
    """Собирает финальный файл: Поднятие трубки → тишина → фон → голос поверх фона → хвост."""
    voice_dur    = get_audio_duration(voice_wav)
    silence_dur = round(random.uniform(0.15, 0.4), 2)
    pre_bg_dur   = 0.5
    post_bg_dur  = round(random.uniform(1.0, 2.0), 2)
    total_bg_dur = pre_bg_dur + voice_dur + post_bg_dur

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        bg_path      = tmpdir / "bg.wav"
        silence_path = tmpdir / "silence.wav"

        # 1. Фон
        bg_ok = get_sox_background(total_bg_dur, bg_path)

        if not bg_ok:
            log.error("bg error")
            return False

        # 1.5 Поднятие трубки
        pickup_files = list(PICKUP_DIR.glob("*.wav"))
        use_pickup = pickup_files and random.random() < 0.85  # 85% записей с трубкой

        if use_pickup:
            pickup_src = random.choice(pickup_files)
            pickup_path = tmpdir / "pickup.wav" 
            # нормализуем pickup к 48000/stereo
            ok, err = run_ffmpeg([
                "-i", str(pickup_src),
                "-ar", "48000", "-ac", "2",
                str(pickup_path)
            ])
            if not ok:
                log.error(f"pickup convert error: {err}")
                pickup_dur = 0.0
            else:
                pickup_dur = get_audio_duration(pickup_path)
        else:
            pickup_dur = 0.0

        # 2. Тишина — используем -t вместо d= и ПРОВЕРЯЕМ returncode
        ok, err = run_ffmpeg([
            "-f", "lavfi",
            "-t", str(silence_dur),
            "-i", "anullsrc=r=48000:cl=stereo",
            "-ar", "48000", "-ac", "2",
            str(silence_path),
        ])

        if not ok:
            log.error(f"ffmpeg silence error: {err}")
            return False

        voice_fx = tmpdir / "voice_fx.wav"
        if not process_voice_radio(voice_wav, voice_fx, category):
            return False

        dialtone_files = list(DIALTONE_DIR.glob("*.wav")) if DIALTONE_DIR.exists() else []
        use_dialtone = dialtone_files and random.random() < DIALTONE_PROBABILITY

        # 2.5 Гудки
        if use_dialtone:
            dt_src  = random.choice(dialtone_files)
            dt_path = tmpdir / "dialtone.wav"
            # Нормализуем к нужному формату
            ok, _ = run_ffmpeg([
                "-i", str(dt_src),
                "-ar", "48000", "-ac", "2", str(dt_path)
            ])
            if not ok:
                use_dialtone = False
            else:
                log.info(f"  dialtone: {dt_src.name}")
        
        # 3. Микс: тишина+фон как база, уже обработанный голос поверх
        if use_pickup and use_dialtone:
            voice_onset = pickup_dur + silence_dur + pre_bg_dur
            onset_ms = int(voice_onset * 1000)
            filter_complex = (
                f"[0][1]concat=n=2:v=0:a=1[preroll];"
                f"[2]volume=0.45[bg];"
                f"[preroll][bg]concat=n=2:v=0:a=1[base];"
                # Голос + split на две копии: одна для сайдчейна, одна для микса
                f"[3]adelay={onset_ms}|{onset_ms},volume=0.9,"
                f"asplit=2[voice_sc][voice_mix];"
                f"[base][voice_sc]sidechaincompress="
                f"threshold=0.05:ratio=4:attack=80:release=400:makeup=1[ducked];"
                f"[ducked][voice_mix]amix=inputs=2:normalize=0:duration=first[mixed];"
                # Короткая пауза 0.2с тишины и гудок
                f"[4]adelay=200|200,volume=0.8[dt];"
                f"[mixed][dt]concat=n=2:v=0:a=1[out]"
            )
            inputs = ["-i", str(pickup_path), "-i", str(silence_path),
                      "-i", str(bg_path), "-i", str(voice_fx),
                      "-i", str(dt_path)]
        else:
            voice_onset = silence_dur + pre_bg_dur
            onset_ms = int(voice_onset * 1000)
            filter_complex = (
                f"[1]volume=0.45[bg];"
                f"[0][bg]concat=n=2:v=0:a=1[base];"
                # Голос = вход [2] (voice_fx), split для сайдчейна и микса
                f"[2]adelay={onset_ms}|{onset_ms},volume=0.9,"
                f"asplit=2[voice_sc][voice_mix];"
                f"[base][voice_sc]sidechaincompress="
                f"threshold=0.05:ratio=4:attack=80:release=400:makeup=1[ducked];"
                f"[ducked][voice_mix]amix=inputs=2:normalize=0:duration=first[out]"
            )
            # ❗ Был баг: отсутствовал voice_fx, хотя фильтр ссылался на [2]
            inputs = ["-i", str(silence_path), "-i", str(bg_path),
                      "-i", str(voice_fx)]
        
        ok, err = run_ffmpeg(
            inputs + [
                "-i",str(voice_fx),
                "-filter_complex",filter_complex,
                "-map","[out]",
                "-ar","48000",
                "-ac","2", str(out_path)
            ]
        )
        
        if not ok:
            log.error(f"ffmpeg mix ошибка {err}")
            return False

    return True

# ─── UVB-only генерация ──────────────────────────────────────────────────────

def build_uvb_only(out_path: Path) -> tuple[bool, str]:
    """Собирает файл из чистого UVB с FX, без голоса."""
    if not UVB_DIR.exists():
        return False, ""
    files = list(UVB_DIR.glob("*.wav"))
    if not files:
        return False, ""

    src         = random.choice(files)
    dur         = round(random.uniform(UVB_ONLY_MIN_SEC, UVB_ONLY_MAX_SEC), 1)
    silence_dur = round(random.uniform(0.4, 0.9), 2)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir    = Path(tmpdir)
        raw_uvb   = tmpdir / "uvb_raw.wav"
        proc_uvb  = tmpdir / "uvb_fx.wav"
        silence   = tmpdir / "silence.wav"

        # 1. Сырой UVB нужной длины (зацикливаем)
        ok, err = run_ffmpeg([
            "-stream_loop", "-1",
            "-i", str(src),
            "-t", str(dur),
            "-ar", "48000", "-ac", "2",
            str(raw_uvb),
        ])
        if not ok:
            log.error(f"uvb-only raw error: {err}")
            return False, ""

        # 2. Применяем рандомный FX поверх UVB (как к голосу)
        fx_name, af = pick_voice_fx()
        ok, err = run_ffmpeg([
            "-i", str(raw_uvb),
            "-af", af,
            "-ar", "48000", "-ac", "2",
            str(proc_uvb),
        ])
        if not ok:
            log.error(f"uvb-only fx error: {err}")
            return False, ""

        # 3. Тишина (как пауза после поднятия трубки)
        ok, err = run_ffmpeg([
            "-f", "lavfi", "-t", str(silence_dur),
            "-i", "anullsrc=r=48000:cl=stereo",
            "-ar", "48000", "-ac", "2",
            str(silence),
        ])
        if not ok:
            log.error(f"uvb-only silence error: {err}")
            return False, ""

        # 4. Опционально — звук поднятия трубки в начале
        pickup_files = list(PICKUP_DIR.glob("*.wav")) if PICKUP_DIR.exists() else []
        inputs = []
        if pickup_files and random.random() < 0.85:
            pickup = random.choice(pickup_files)
            inputs += ["-i", str(pickup)]
        inputs += ["-i", str(silence), "-i", str(proc_uvb)]

        n_inputs = len(inputs) // 2
        concat_in = "".join(f"[{i}]" for i in range(n_inputs))
        fc = f"{concat_in}concat=n={n_inputs}:v=0:a=1[out]"

        ok, err = run_ffmpeg([
            *inputs,
            "-filter_complex", fc,
            "-map", "[out]",
            "-ar", "48000", "-ac", "2",
            str(out_path),
        ])
        if not ok:
            log.error(f"uvb-only mix error: {err}")
            return False, ""

    return True, fx_name


# ─── run_ffmpeg ─────────────────────────────────────────────────────

def run_ffmpeg(args: list, timeout: int = 60) -> tuple[bool, str]:
    """Запускает ffmpeg, возвращает (ok, stderr_tail)."""
    try:
        r = subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", *args],
            capture_output=True, text=False, timeout=timeout,
        )
        if r.returncode != 0:
            return False, r.stderr.decode("utf-8", errors="replace")[-800:]
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)

# ─── CSV helpers ──────────────────────────────────────────────────────────────

def get_pending_rows():
    pending = []
    for csv_file in sorted(MESSAGES_DIR.glob("messages_*.csv")):
        with open(csv_file, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        for i, row in enumerate(rows):
            if row.get("audio_done", "0") == "0":
                pending.append((csv_file, i, row))
    return pending


def mark_done(csv_file: Path, row_index: int, meta: dict):
    with open(csv_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    all_fields = list(rows[0].keys()) if rows else []
    for key in ["audio_done", "tts_speaker", "bg_type"]:
        if key not in all_fields:
            all_fields.append(key)

    rows[row_index].update({
        "audio_done":  "1",
        "tts_speaker": meta.get("speaker", ""),
        "bg_type":     meta.get("bg_type", ""),
    })

    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

# ─── Основная логика ──────────────────────────────────────────────────────────

def run(output_dir: Path, limit: int):
    output_dir.mkdir(parents=True, exist_ok=True)

    pending = get_pending_rows()
    if not pending:
        log.info("Нет необработанных сообщений")
        return
    if limit:
        pending = pending[:limit]

    log.info(f"К обработке: {len(pending)} сообщений → {output_dir}")

    success = 0
    fail    = 0
    model = load_silero_model()

    for csv_file, row_index, row in pending:
        if random.random() < UVB_ONLY_PROBABILITY:
            ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
            out = output_dir / f"{ts}_uvb_only.wav"
            ok, fx = build_uvb_only(out)
            if ok:
                log.info(f"  ✓ UVB-only → {out.name} fx={fx}")
                success += 1
                time.sleep(0.2)
                continue                    # ← сообщение НЕ тратим
            else:
                log.warning("  UVB-only failed, fallback to voice")
    
        msg_id   = row["id"]
        raw_text = row["text"]
        category = row["category"]

        log.info(f"[id={msg_id}]"
                 f"text={raw_text[:50]}{'...' if len(raw_text) > 50 else ''}")

        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"{ts}_{msg_id}.wav"
        out_path = output_dir / out_name

        engine = pick_tts_engine(row["category"])
        log.info(f"[id={msg_id}] engine={engine} ...")

        with tempfile.TemporaryDirectory() as tmpdir:
            voice_path = Path(tmpdir) / "voice.wav"
            
            ok, speaker = synthesize_dispatch(engine, model, raw_text, voice_path)
            if not ok:
                log.error(f"TTS failed id={msg_id}")
                fail += 1
                continue

            ok = build_final_wav(voice_path, out_path, category)

        if ok:
            mark_done(csv_file, row_index, {"speaker": speaker, "tts_engine": engine})
            log.info(f"✓ → {out_name}")
            success += 1
        else:
            log.error(f"mix failed id={msg_id}")
            fail += 1

        time.sleep(0.2)

    log.info(f"Готово. Успешно: {success}, ошибок: {fail}")

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TTS обработчик Ghost Phone (Silero v5)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit",  type=int,  default=0,
                        help="Максимум сообщений за запуск (0 = все)")
    args = parser.parse_args()
    run(args.output, args.limit)


if __name__ == "__main__":
    main()
