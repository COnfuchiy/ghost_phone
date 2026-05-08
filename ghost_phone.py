#!/usr/bin/env python3
"""
Ghost Phone Daemon
Звонит в рандомные моменты, ждёт ответа, проигрывает сообщение.

Кнопки:
  KEY_PLAYPAUSE  — ответить / положить трубку
  KEY_VOLUMEUP   — включить звонки
  KEY_VOLUMEDOWN — отключить звонки

Папки:
  ~/queue/   — .wav файлы сообщений (кладёт Ubuntu по SCP)
  ~/sounds/  — ring.wav (рингтон)
"""

import os
import time
import random
import subprocess
import threading
from pathlib import Path
from evdev import InputDevice, ecodes

# ─── Настройки ────────────────────────────────────────────────────────────────

QUEUE_DIR    = Path.home() / "queue"
SOUNDS_DIR   = Path.home() / "sounds"
RING_DIR     = SOUNDS_DIR / "ring"
AUDIO_DEVICE = "hw:0,0"
EVENT_DEVICE = "/dev/input/event0"

# Интервал между звонками (в секундах)
MIN_INTERVAL = 1#30 * 60   # 30 минут
MAX_INTERVAL = 1#120 * 60  # 2 часа

# ─── Состояния ────────────────────────────────────────────────────────────────
#
#  IDLE      — тихо, ждём следующего звонка по таймеру
#  RINGING   — играет рингтон (1-2 сек)
#  WAITING   — рингтон закончился, ждём KEY_PLAYPAUSE
#  PLAYING   — играет сообщение
#
# Таймер следующего звонка запускается ТОЛЬКО после того как:
#   - сообщение прослушано до конца
#   - или пользователь положил трубку во время воспроизведения
# Таким образом сообщения не пропускаются.

class GhostPhone:

    def __init__(self):
        self.state           = "IDLE"
        self.enabled         = True      # KEY_VOLUMEDOWN/UP
        self.current_message = None      # Path текущего файла
        self.play_process    = None      # subprocess aplay
        self.next_ring_time  = None
        self._lock           = threading.Lock()

        QUEUE_DIR.mkdir(exist_ok=True)
        SOUNDS_DIR.mkdir(exist_ok=True)

   # ── Громкость ─────────────────────────────────────────────────────────────
 
    def _set_volume_max(self):
        try:
            subprocess.run(
                ["amixer", "-c","0", "sset", "PCM", "11520"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
            )
        except Exception as e:
            print(f"[ghost] не удалось установить громкость:: {e}")
 

    # ── Аудио ─────────────────────────────────────────────────────────────────

    def _play(self, filepath: Path):
        """Запустить aplay, вернуть процесс."""
        return subprocess.Popen(
            ["aplay", "-D", AUDIO_DEVICE, str(filepath)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _stop(self):
        """Остановить текущее воспроизведение."""
        if self.play_process and self.play_process.poll() is None:
            self.play_process.terminate()
            self.play_process.wait()
            self.play_process = None

    # ── Очередь ───────────────────────────────────────────────────────────────

    def _next_message(self):
        """Взять первый файл из очереди (по имени = хронологически)."""
        files = sorted(QUEUE_DIR.glob("*.wav"))
        random.shuffle(files)
        return files[0] if files else None

    def _queue_size(self):
        return len(list(QUEUE_DIR.glob("*.wav")))

    # ── Таймер ────────────────────────────────────────────────────────────────

    def _schedule_next(self):
        delay = random.randint(MIN_INTERVAL, MAX_INTERVAL)
        self.next_ring_time = time.time() + delay
        m, s = divmod(delay, 60)
        print(f"[ghost] следующий звонок через {m} мин {s} сек  "
              f"(очередь: {self._queue_size()} файлов)")

    # ── Звонок ────────────────────────────────────────────────────────────────

    def _do_ring(self):
        msg = self._next_message()
        if not msg:
            print("[ghost] очередь пуста — пропускаем, планируем следующий")
            self._schedule_next()
            return

        self.current_message = msg

        with self._lock:
            self.state = "RINGING"

        print(f"[ghost] звоним... ({msg.name})")

        ringtones = list(RING_DIR.glob("*.wav")) if RING_DIR.exists() else []
        if not ringtones:
            print(f"[ghost] WARNING: нет файлов в {RING_DIR}, пропускаем рингтон")
        else:
            ringtone = random.choice(ringtones)
            print(f"[ghost] рингтон: {ringtone.name}")
            self.play_process = self._play(ringtone)
            self.play_process.wait()
            self.play_process = None

        # Рингтон закончился — ждём ответа
        with self._lock:
            if self.state == "RINGING":          # не прервали извне
                self.state = "WAITING"
                print("[ghost] ждём ответа (KEY_PLAYPAUSE)...")

    # ── Ответ / сброс ─────────────────────────────────────────────────────────

    def _answer(self):
        with self._lock:
            state = self.state

        if state in ("RINGING", "WAITING"):
            self._stop()
            with self._lock:
                self.state = "PLAYING"
            print(f"[ghost] воспроизводим: {self.current_message.name}")
            t = threading.Thread(target=self._play_message, daemon=True)
            t.start()

        elif state == "PLAYING":
            # Второе нажатие — положить трубку
            self._hangup(delete=True)

    def _play_message(self):
        self.play_process = self._play(self.current_message)
        self.play_process.wait()
        self.play_process = None

        with self._lock:
            if self.state != "PLAYING":   # прервали кнопкой — уже обработано
                return
            self.state = "IDLE"

        print("[ghost] сообщение прослушано")
        self.current_message.unlink(missing_ok=True)
        self.current_message = None
        self._schedule_next()

    def _hangup(self, delete=True):
        self._stop()
        with self._lock:
            self.state = "IDLE"
        if delete and self.current_message:
            self.current_message.unlink(missing_ok=True)
        self.current_message = None
        print("[ghost] трубка положена")
        self._schedule_next()

    # ── Основные потоки ───────────────────────────────────────────────────────

    def _timer_loop(self):
        self._schedule_next()
        while True:
            time.sleep(5)
            with self._lock:
                state   = self.state
                enabled = self.enabled

            if not enabled or state != "IDLE":
                continue
            if time.time() >= self.next_ring_time:
                t = threading.Thread(target=self._do_ring, daemon=True)
                t.start()

    def _input_loop(self):
        try:
            dev = InputDevice(EVENT_DEVICE)
            dev.grab()   # чтобы события не уходили в систему (не меняли громкость ОС)
            print(f"[ghost] слушаем кнопки: {dev.name}")
            for event in dev.read_loop():
                if event.type == ecodes.EV_KEY and event.value == 1:
                    code = event.code
                    if code == ecodes.KEY_PLAYPAUSE:
                        print("[ghost] KEY_PLAYPAUSE")
                        self._answer()
                    elif code == ecodes.KEY_VOLUMEDOWN:
                        self.enabled = False
                        self._stop()
                        with self._lock:
                            self.state = "IDLE"
                        self.current_message = None
                        print("[ghost] звонки ОТКЛЮЧЕНЫ (KEY_VOLUMEDOWN)")
                    elif code == ecodes.KEY_VOLUMEUP:
                        self.enabled = True
                        self._schedule_next()
                        print("[ghost] звонки ВКЛЮЧЕНЫ (KEY_VOLUMEUP)")
        except PermissionError:
            print(f"[ghost] нет доступа к {EVENT_DEVICE}. "
                  "Добавь пользователя в группу input:\n"
                  "  sudo usermod -aG input $USER\n"
                  "  и перелогинься / перезапусти демон")
        except Exception as e:
            print(f"[ghost] ошибка ввода: {e}")

    def run(self):
        print("[ghost] Ghost Phone запущен")
        print(f"[ghost] очередь: {QUEUE_DIR}")
        print(f"[ghost] рингтоны: {RING_DIR}")
        
        self._set_volume_max()

        t_input = threading.Thread(target=self._input_loop, daemon=True)
        t_timer = threading.Thread(target=self._timer_loop, daemon=True)
        t_input.start()
        t_timer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[ghost] остановлен")


if __name__ == "__main__":
    GhostPhone().run()
