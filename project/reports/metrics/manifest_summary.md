# Sumar ingestie și audit manifest (`00-data-acquisition`)

- **Data generării (UTC):** `2026-11-17T19:50:46.462371+00:00`
- **Versiune schemă ingestie:** `1.0.0`
- **Subiecți inventariați:** `[1, 2, 3]` (utilizabili: `[1, 2, 3]`, neutilizabili: `[]`)
- **Fișiere lipsă:** NPZ=`[]`, Parquet=`[]`
- **Mapare canale:** `64` total (`59` EEG, `1` ECG, `4` EOG) — compatibil `standard_1005`: `True`

## Discrepanțe și proveniență documentate

1. **Prototipul inițial:** nota text menționează 53 canale, dar datele reale și `config.py` au **64 canale** (`59 EEG + 1 ECG + 4 EOG`).
2. **Schema Parquet:** coloana `session` lipsește din `data_sub_*.parquet`; sesiunile `['0', '1', '2']` sunt păstrate în `metadata` din `data_sub_*.npz`.
3. **`sfreq`:** nu este stocat în NPZ; valoarea `1000.0 Hz` este inferată din `data/README.md` (`[1.5, 5.5] s` -> `4001` eșantioane) și marcată `sfreq_verified=False`.
4. **Proveniență CSV (`X_data.csv`, `y_data.csv`):** `X_data.csv si y_data.csv provin din exportul concatenat al celor 3 subiecti (7.201.800 randuri), insa X_data.csv contine doar cele 59 canale EEG (ch_0..ch_58), fara ECG/EOG, fara subject si fara session.`

## Tabel manifest

| subject_id | format | file_size_mb | n_trials | n_channels | n_samples | sfreq | sessions | has_session_metadata | usable | discrepancies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | npz | 1080.909 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | True | True |  |
| 1 | parquet | 672.346 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | False | True | Coloana 'session' lipseste din Parquet (disponibila in NPZ metadata) |
| 2 | npz | 1094.454 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | True | True |  |
| 2 | parquet | 675.783 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | False | True | Coloana 'session' lipseste din Parquet (disponibila in NPZ metadata) |
| 3 | npz | 1075.235 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | True | True |  |
| 3 | parquet | 668.19 | 600 | 64 | 4001 | 1000.0 | 0,1,2 | False | True | Coloana 'session' lipseste din Parquet (disponibila in NPZ metadata) |
