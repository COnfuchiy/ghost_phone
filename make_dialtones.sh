#!/bin/bash
# make_dialtones.sh
mkdir -p sounds/dialtone

# 1) Короткий отбой — 3 гудка «занято» (советский стандарт 425 Гц, 0.35/0.35)
for i in 01 02 03; do
  sox -n -r 48000 -c 1 "sounds/dialtone/busy_$i.wav" \
      synth 0.35 sine 425 vol 0.22 \
      : synth 0.35 sine 0 \
      : synth 0.35 sine 425 vol 0.22 \
      : synth 0.35 sine 0 \
      : synth 0.35 sine 425 vol 0.22 \
      fade h 0.01 0 0.08
done

# 2) Длинный гудок «линия свободна» — непрерывный 425 Гц
for i in 01 02; do
  DUR=$(awk -v min=1.5 -v max=2.5 'BEGIN{srand('$RANDOM'); print min+rand()*(max-min)}')
  sox -n -r 48000 -c 1 "sounds/dialtone/dial_$i.wav" \
      synth $DUR sine 425 vol 0.18 \
      fade t 0.05 $DUR 0.2
done

# 3) «Отбой связи» — нисходящая последовательность тонов, как потеря соединения
sox -n -r 48000 -c 1 sounds/dialtone/disconnect_01.wav \
    synth 0.25 sine 950 vol 0.2 \
    : synth 0.25 sine 1400 vol 0.2 \
    : synth 0.25 sine 1800 vol 0.2 \
    : synth 0.4 sine 0 \
    fade h 0.02 0 0.1

# 4) Одиночный короткий «пик» — как сигнал точного времени
sox -n -r 48000 -c 1 sounds/dialtone/pip_01.wav \
    synth 0.2 sine 1000 vol 0.25 \
    fade h 0.005 0.2 0.03

# 5) Советский длинный гудок с лёгким искажением как через трубку
sox -n -r 48000 -c 1 sounds/dialtone/dial_old_01.wav \
    synth 2.0 sine 425 vol 0.2 \
    bandpass 425 250h \
    fade t 0.05 2.0 0.2

echo "готово"
