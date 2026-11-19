# Sumar EDA si Integritate (`01-data-analysis`)

- **Subiecti analizati:** [1, 2, 3]
- **Subiecti ce necesita atentie:** [1, 2, 3]
- **Prag flatline:** `1e-08`
- **Prag varianta extrema EEG:** `>5.0x` sau `<0.1x` din mediana EEG
- **Canale semnalate:** 8

## Tabel integritate per subiect

```text

 subject_id  pq_rows  pq_trials  pq_samples  rows_ok  nulls   eeg_min  eeg_max  aux_min    aux_max  npz_trials  n_sessions sessions  match_parquet  n_nan  n_inf  flatline_channels  partial_flatline_channels  extreme_var_eeg  needs_attention
          1  2400600        600        4001     True      0 -0.164860 0.186856   -375.0 185.344437         600           3    0,1,2           True      0      0                  0                          2                0             True
          2  2400600        600        4001     True      0 -0.199141 0.192733   -375.0 226.498108         600           3    0,1,2           True      0      0                  0                          2                0             True
          3  2400600        600        4001     True      0 -0.200235 0.148855   -375.0 262.516296         600           3    0,1,2           True      0      0                  0                          4                0             True
```