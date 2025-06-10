import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pint import UnitRegistry

# Einheitensystem initialisieren
ureg = UnitRegistry()
Q_ = ureg.Quantity

# Verbindung zur Datenbank
engine = create_engine('postgresql://postgres:passwort@localhost:5432/mimic-demo')

# Daten aus der Bronze-Tabelle laden
bronze_df = pd.read_sql('SELECT * FROM bronze."collection_disease "', engine)

# OMOP-Mapping für vorkommende ItemIDs
omop_mapping = {
    '220615': {'concept_id': 3016723, 'concept_name': 'Creatinine [Mass/volume] in Serum or Plasma', 'vocabulary_id': 'LOINC', 'standard_uom': 'mg/dL'},
    '220045': {'concept_id': 3027018, 'concept_name': 'Heart rate', 'vocabulary_id': 'LOINC', 'standard_uom': 'beats/min'},
    '223762': {'concept_id': 4302666, 'concept_name': 'Body temperature', 'vocabulary_id': 'SNOMED', 'standard_uom': 'degC'},
    '220179': {'concept_id': 4152194, 'concept_name': 'Systolic blood pressure', 'vocabulary_id': 'SNOMED', 'standard_uom': 'mmHg'},
    '220180': {'concept_id': 4154790, 'concept_name': 'Diastolic blood pressure', 'vocabulary_id': 'SNOMED', 'standard_uom': 'mmHg'},
    '228232': {'concept_id': 3013502, 'concept_name': 'Oxygen saturation in Blood', 'vocabulary_id': 'LOINC', 'standard_uom': '%'},
    '220228': {'concept_id': 3000963, 'concept_name': 'Hemoglobin [Mass/volume] in Blood', 'vocabulary_id': 'LOINC', 'standard_uom': 'g/dL'}
}

# Funktion zur Einheitenkonvertierung
def convert_units(value, from_uom, to_uom):
    try:
        quantity = Q_(value, from_uom)
        return quantity.to(to_uom).magnitude
    except:
        return None

# Outlier-Funktionen
def is_outlier_creatinine(v): return v < 0.3 or v > 15
def is_outlier_heart_rate(v): return v < 30 or v > 220
def is_outlier_temperature(v): return v < 30 or v > 42
def is_outlier_bp_systolic(v): return v < 50 or v > 250
def is_outlier_bp_diastolic(v): return v < 30 or v > 150
def is_outlier_spo2(v): return v < 50 or v > 100
def is_outlier_hemoglobin(v): return v < 3 or v > 22

outlier_functions = {
    '220615': is_outlier_creatinine,
    '220045': is_outlier_heart_rate,
    '223762': is_outlier_temperature,
    '220179': is_outlier_bp_systolic,
    '220180': is_outlier_bp_diastolic,
    '228232': is_outlier_spo2,
    '220228': is_outlier_hemoglobin
}

# Daten verarbeiten
silver_data = []

for _, row in bronze_df.iterrows():
    itemid = str(row['itemid'])
    if itemid not in omop_mapping:
        continue

    mapping = omop_mapping[itemid]
    orig_uom = row['valueuom']
    target_uom = mapping['standard_uom']
    valuenum = row['valuenum']
    standardized_value = None

    if pd.notna(valuenum):
        if orig_uom and target_uom and orig_uom != target_uom:
            standardized_value = convert_units(valuenum, orig_uom, target_uom)
        else:
            standardized_value = valuenum

    is_outlier = False
    if standardized_value is not None and itemid in outlier_functions:
        is_outlier = outlier_functions[itemid](standardized_value)

    silver_data.append({
        'bronze_id': row['id'],
        'subject_id': row['subject_id'],
        'hadm_id': row['hadm_id'],
        'stay_id': row['stay_id'],
        'charttime': row['charttime'],
        'storetime': row['storetime'],
        'itemid': row['itemid'],
        'value': row['value'],
        'valuenum': standardized_value,
        'valueuom': target_uom,
        'concept_name': mapping['concept_name'],
        'concept_id': mapping['concept_id'],
        'source_table': row['source_table'],
        'is_outlier': is_outlier
    })

# Als DataFrame speichern
silver_df = pd.DataFrame(silver_data)

# In die Datenbank schreiben
silver_df.to_sql('standardized_parameters', engine, schema='silver', if_exists='append', index=False)
