# NHANES Hypertension Experiment Results

Numbers below come from the executed external-training protocol using the pinned Delta Sharing exports.

## LogisticRegression

| Dataset | AUC (mean ± std) | Macro-F1 (mean ± std) |
| --- | ---: | ---: |
| d_base | 0.8139 +- 0.0000 | 0.7343 +- 0.0000 |
| d_fabric | 0.8278 +- 0.0000 | 0.7412 +- 0.0000 |

## RandomForestClassifier

| Dataset | AUC (mean ± std) | Macro-F1 (mean ± std) |
| --- | ---: | ---: |
| d_base | 0.8156 +- 0.0010 | 0.7329 +- 0.0032 |
| d_fabric | 0.8337 +- 0.0020 | 0.7341 +- 0.0026 |

## ExtraTreesClassifier

| Dataset | AUC (mean ± std) | Macro-F1 (mean ± std) |
| --- | ---: | ---: |
| d_base | 0.8155 +- 0.0008 | 0.7272 +- 0.0025 |
| d_fabric | 0.8369 +- 0.0011 | 0.7409 +- 0.0036 |

## Export Reproducibility

| Dataset | Pinned version | Delta Sharing repeated-read hash match | Delta Sharing vs MinIO hash match | Hash match rate |
| --- | ---: | --- | --- | ---: |
| d_base | 0 | True | True | 100.0% |
| d_fabric | 0 | True | True | 100.0% |
