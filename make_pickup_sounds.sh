#!/bin/bash
# make_pickup_sounds.sh — генерит 10 вариантов звука поднятия трубки в sounds/pickup/
mkdir -p sounds/pickup

for i in $(seq 1 10); do
  # компоненты
  T=$(mktemp -d)

  # 1) короткий механический клик (поднятие рычага)
  sox -n -r 48000 -c 1 "$T/click.wav" \
      synth 0.015 pinknoise vol 0.7 \
      fade h 0.001 0.015 0.003

  # 2) шорох кабеля / движение трубки (0.2-0.35 сек)
  DUR=$(awk -v min=0.2 -v max=0.35 'BEGIN{srand(); print min+rand()*(max-min)}')
  sox -n -r 48000 -c 1 "$T/rustle.wav" \
      synth $DUR pinknoise vol 0.25 \
      bandpass 1800 1200h \
      tremolo 35 80 \
      fade t 0.02 $DUR 0.08

  # 3) гудок/фон линии (глухой, чуть слышный)
  sox -n -r 48000 -c 1 "$T/line.wav" \
      synth 0.4 sine 50 vol 0.04 \
      fade t 0.05 0.4 0.1

  # 4) лёгкий статический фон сквозь всю запись
  sox -n -r 48000 -c 1 "$T/hiss.wav" \
      synth 0.6 pinknoise vol 0.05 \
      highpass 3000

  # собираем: [пауза 50мс][клик][шорох][пауза 100мс][гудок линии]
  sox "$T/click.wav" "$T/rustle.wav" "$T/line.wav" "$T/seq.wav"
  sox -m "$T/seq.wav" "$T/hiss.wav" "sounds/pickup/pickup_$(printf %02d $i).wav" \
      pad 0.03 0.1 \
      reverb 15 50 30

  rm -rf "$T"
  echo "готов pickup_$(printf %02d $i).wav"
done
