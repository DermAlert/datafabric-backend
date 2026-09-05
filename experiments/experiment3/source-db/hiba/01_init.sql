CREATE SCHEMA IF NOT EXISTS isic;

CREATE TABLE isic.metadata (
    isic_id TEXT,
    attribution TEXT,
    copyright_license TEXT,
    age_approx TEXT,
    anatom_site_1 TEXT,
    anatom_site_2 TEXT,
    anatom_site_special TEXT,
    concomitant_biopsy TEXT,
    dermoscopic_type TEXT,
    diagnosis_1 TEXT,
    diagnosis_2 TEXT,
    diagnosis_3 TEXT,
    diagnosis_confirm_type TEXT,
    family_hx_mm TEXT,
    fitzpatrick_skin_type TEXT,
    image_type TEXT,
    lesion_id TEXT,
    patient_id TEXT,
    personal_hx_mm TEXT,
    sex TEXT,
    source_collection_id TEXT NOT NULL DEFAULT '251',
    source_collection_name TEXT NOT NULL DEFAULT 'HIBA Skin Lesions',
    source_snapshot_sha256 TEXT NOT NULL DEFAULT '9593166a1f3767679c0d69554013906ab096d171ad15914fcdf3decb5dd67b47'
);

COPY isic.metadata (
    isic_id, attribution, copyright_license, age_approx,
    anatom_site_1, anatom_site_2, anatom_site_special, concomitant_biopsy,
    dermoscopic_type, diagnosis_1, diagnosis_2, diagnosis_3,
    diagnosis_confirm_type, family_hx_mm, fitzpatrick_skin_type, image_type,
    lesion_id, patient_id, personal_hx_mm, sex
)
FROM '/data/hiba_metadata.csv'
WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8');

CREATE UNIQUE INDEX hiba_metadata_isic_id_idx
ON isic.metadata (isic_id);
CREATE INDEX hiba_metadata_patient_id_idx
ON isic.metadata (patient_id);

-- Compatibility alias used by the original experiment-v1 metadata snapshot.
CREATE VIEW isic.hiba_metadata AS
SELECT * FROM isic.metadata;
