CREATE SCHEMA IF NOT EXISTS isic;

CREATE TABLE isic.metadata (
    isic_id TEXT,
    attribution TEXT,
    copyright_license TEXT,
    age_approx TEXT,
    anatom_site_1 TEXT,
    anatom_site_2 TEXT,
    anatom_site_3 TEXT,
    anatom_site_special TEXT,
    concomitant_biopsy TEXT,
    diagnosis_1 TEXT,
    diagnosis_2 TEXT,
    diagnosis_3 TEXT,
    diagnosis_confirm_type TEXT,
    image_manipulation TEXT,
    image_type TEXT,
    lesion_id TEXT,
    melanocytic TEXT,
    sex TEXT,
    source_collection_id TEXT NOT NULL DEFAULT '212',
    source_collection_name TEXT NOT NULL DEFAULT 'HAM10000',
    source_snapshot_sha256 TEXT NOT NULL DEFAULT 'd836a9fefa617204aae502d50b55299edfbfe1cb690a4b1a6c7e50803783229f'
);

COPY isic.metadata (
    isic_id, attribution, copyright_license, age_approx,
    anatom_site_1, anatom_site_2, anatom_site_3, anatom_site_special,
    concomitant_biopsy, diagnosis_1, diagnosis_2, diagnosis_3,
    diagnosis_confirm_type, image_manipulation, image_type, lesion_id,
    melanocytic, sex
)
FROM '/data/ham10000_metadata.csv'
WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8');

CREATE UNIQUE INDEX ham10000_metadata_isic_id_idx
ON isic.metadata (isic_id);
