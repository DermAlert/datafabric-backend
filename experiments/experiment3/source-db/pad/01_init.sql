CREATE DATABASE IF NOT EXISTS isic
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE isic;

CREATE TABLE metadata (
    isic_id TEXT,
    attribution TEXT,
    copyright_license TEXT,
    age_approx TEXT,
    anatom_site_1 TEXT,
    anatom_site_2 TEXT,
    anatom_site_3 TEXT,
    anatom_site_4 TEXT,
    anatom_site_5 TEXT,
    anatom_site_special TEXT,
    clin_size_long_diam_mm TEXT,
    diagnosis_1 TEXT,
    diagnosis_2 TEXT,
    diagnosis_3 TEXT,
    diagnosis_confirm_type TEXT,
    fitzpatrick_skin_type TEXT,
    image_type TEXT,
    lesion_id TEXT,
    patient_id TEXT,
    sex TEXT,
    source_collection_id VARCHAR(16) NOT NULL DEFAULT '406',
    source_collection_name VARCHAR(100) NOT NULL DEFAULT 'PAD-UFES-20',
    source_snapshot_sha256 VARCHAR(64) NOT NULL DEFAULT '1f5f12758931a73068179bfe0b5eb9c0e03f2e605cf2499eb1d39325138bd8d9',
    UNIQUE KEY pad_ufes_20_isic_id_idx (isic_id(32)),
    KEY pad_ufes_20_patient_id_idx (patient_id(32))
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

LOAD DATA INFILE '/var/lib/mysql-files/pad_ufes_20_metadata.csv'
INTO TABLE metadata
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  isic_id,
  attribution,
  copyright_license,
  age_approx,
  anatom_site_1,
  anatom_site_2,
  anatom_site_3,
  anatom_site_4,
  anatom_site_5,
  anatom_site_special,
  clin_size_long_diam_mm,
  diagnosis_1,
  diagnosis_2,
  diagnosis_3,
  diagnosis_confirm_type,
  fitzpatrick_skin_type,
  image_type,
  lesion_id,
  patient_id,
  @sex
)
SET sex = TRIM(TRAILING '\r' FROM @sex);
