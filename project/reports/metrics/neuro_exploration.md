# Sumar Explorare Neurofiziologica (`02-data-explore`)

- **Subiecti analizati:** [1, 2, 3]
- **Benzi declarate:** `Mu=(8.0, 12.0) Hz`, `Beta=(13.0, 30.0) Hz`
- **Fereastra analizata:** `[0.0, 4.0] s` (fara baseline pre-cue)
- **Unitate independenta inferenta:** trial intra-subiect (corectie Benjamini-Hochberg FDR pe 59 canale)
- **Limitare:** analiza descriptiva/exploratorie; nu inlocuieste validarea pe setul de test.

## Canale semnificative Mu (p_FDR < 0.05) per subiect

```text

 subject_id  significant_channels_fdr
          1                        10
          2                         0
          3                         0
```

## Top 15 canale dupa p_FDR

```text

 subject_id channel  log2_fc_right_vs_left    p_raw    p_fdr  significant_fdr
          1      C4               0.262571 0.000140 0.005859             True
          1      C6               0.240573 0.000199 0.005859             True
          1     CP4               0.282071 0.000351 0.006907             True
          1      Fz               0.148813 0.007868 0.046421             True
          1      Cz               0.165910 0.004986 0.046421             True
          1     FCz               0.178068 0.004579 0.046421             True
          1     CP2               0.223856 0.003390 0.046421             True
          1     CP6               0.166612 0.006982 0.046421             True
          1      P3               0.118150 0.007232 0.046421             True
          1     PO5               0.112239 0.006177 0.046421             True
```