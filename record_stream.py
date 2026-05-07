#!/usr/bin/env python3
"""
record_stream.py — вырезает фрагменты из онлайн-стримов советского радио
и УВБ-76 webSDR. Кладёт готовые wav в ту же очередь что и tts.py.
"""
import subprocess, random, argparse, time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUT_DIR  = BASE_DIR / "queue"

STREAMS = [
    # (url, category, weight, процессинг)
    ("https://stream.cassiopeia-station.ru:1045/stream", "soviet_radio", 0.5, "radio"),
    ("https://evcast.mediacp.eu:2075/stream", "soviet_radio", 0.5, "radio"),
    ("https://staroeradio.ru/radio/ices128", "soviet_radio", 0.5, "radio"),
    ("https://staroeradio.ru/radio/music128", "soviet_radio", 0.5, "radio")
]

FX_PRESETS = {
    # Обычное узкополосное радио (как раньше)
    "radio": (
        "highpass=f=250,lowpass=f=3200,"
        "acompressor=threshold=-20dB:ratio=6,"
        "volume=-2dB,"
        "aformat=channel_layouts=stereo"
    ),
    # «Радио в пустом городе» — как в интро Fallout 3
    "distant_city": (
        # 1. Сужаем спектр — дальний источник теряет ВЧ и НЧ
        "aformat=channel_layouts=mono,"
        "highpass=f=180,"
        "lowpass=f=1800,"
        # 2. Лёгкая компрессия чтобы «приблизить» динамику
        "acompressor=threshold=-22dB:ratio=4:attack=20:release=250,"
        # 3. Тихо — как будто источник далеко
        "volume=-9dB,"
        # 4. Длинная пустая реверберация (большое пространство)
        "aecho=0.8:0.88:60|120|250|500|900:0.5|0.4|0.3|0.2|0.15,"
        # 5. Ещё один слой коротких отражений — стены
        "aecho=0.7:0.9:25|55:0.35|0.25,"
        # 6. Крошечный фейзер — «воздух движется»
        "aphaser=in_gain=0.5:out_gain=0.74:delay=3:decay=0.3:speed=0.2,"
        # 7. В стерео с небольшим сдвигом — ощущение пространства
        "aformat=channel_layouts=stereo,"
        "extrastereo=m=1.8"
    ),
    # УВБ-76 как раньше
    "uvb": (
        "highpass=f=350,lowpass=f=2600,"
        "acompressor=threshold=-18dB:ratio=8,"
        "volume=0dB,"
        "aformat=channel_layouts=stereo"
    ),
}

def grab(url: str, duration: int, fx_key: str, out_path: Path) -> bool:
    cmd = [
        "ffmpeg", "-y",
        "-loglevel", "error",           # меньше мусора в stderr
        "-reconnect", "1",              # переподключение при разрывах
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-rw_timeout", "10000000",      # 10 сек таймаут на чтение (µs)
        "-i", url,
        "-t", str(duration),
        "-af", FX_PRESETS[fx_key],
        "-ar", "48000", "-ac", "2",
        str(out_path),
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=False,                 # ← ключ: забираем байты
            timeout=duration + 30,
        )
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", errors="replace")[-800:]
            print(f"  ffmpeg rc={r.returncode}: {err}")
            return False
        return out_path.exists() and out_path.stat().st_size > 1024
    except subprocess.TimeoutExpired:
        print("  timeout")
        return False

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--min-sec", type=int, default=20)
    p.add_argument("--max-sec", type=int, default=60)
    args = p.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    for i in range(args.count):
        url, cat, w, fx = random.choices(STREAMS, weights=[s[2] for s in STREAMS])[0]
        dur = random.randint(args.min_sec, args.max_sec)
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = OUT_DIR / f"{ts}_stream_{cat}_{i:03d}.wav"
        print(f"[{i+1}/{args.count}] {cat} {dur}s → {out.name}")
        if not grab(url, dur, fx, out):
            print("  ✗ fail")
        time.sleep(2)  # не бомбим сервер

if __name__ == "__main__":
    main()
