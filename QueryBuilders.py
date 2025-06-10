import csv
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String, TIMESTAMP,Numeric

engine = sqlalchemy.create_engine( 'postgresql://postgres:Bierbrauer1!@localhost:5433/mimic', echo = True)

with engine.begin() as conn:
# Verbindung zur Datenbank herstellen
# Dictionary-Tabelle in einen DataFrame laden
    conn.execute(text("CREATE SCHEMA bronze;"))
    metadata = MetaData()

    bronze = Table(
        'collection_disease ', metadata,
        Column('id', Integer, primary_key=True),
        Column('subject_id', Integer),
        Column('hadm_id', Integer),
        Column('stay_id', Integer),
        Column('charttime', TIMESTAMP),
        Column('storetime', TIMESTAMP),
        Column('itemid', String),
        Column('value', String),
        Column('valuenum', Numeric),
        Column('valueuom', String),
        Column('source_table', String),
    )
    metadata.create_all(engine)

def select_ditems(liste,con):
    stack = []
    for item in liste:
        query_select = 'SELECT itemid, linksto FROM mimiciv_icu.d_items '
        query_where = 'WHERE itemid::text LIKE \'%' + item + '%\';'
        rs = con.execute(text(query_select + query_where))
        data = rs.first()
        if data is not None:
            stack.append((item, "mimiciv_icu."+ data[1]))
    return stack

with engine.begin() as con:

    query_d_items = []
    query_d_icd_diagnoses = []
    query_d_labitems = []

    with open('itemids.csv', 'r') as file:
        reader = csv.reader(file, delimiter="\t")
        for line in reader:
            if line[1] == "d_icd_diagnoses":
                query_d_icd_diagnoses.append(line[0])
            if line[1] == "d_items":
                query_d_items.append(line[0])
            else:
                query_d_labitems.append(line[0])
    id = 0
    stack = select_ditems(query_d_items,con)
    for item in stack:
        query_select = 'SELECT * FROM ' + item[1]
        query_where = ' WHERE itemid::text LIKE \'%' + item[0] + '%\';'
        rs = con.execute(text(query_select + query_where))
        for row in rs:
            insert = bronze.insert().values(id =id, subject_id=row[0],hadm_id=row[1],stay_id=row[2],charttime=row[4].strftime("%Y-%m-%d %H:%M:%S"),storetime=row[5].strftime("%Y-%m-%d %H:%M:%S"),itemid=item[0],value=row[7],valuenum=row[8],valueuom=row[9],source_table=item[1])
            con.execute(insert)
            id += 1


    for item in query_d_labitems:
        query_select = 'SELECT * FROM mimiciv_hosp.labevents'
        query_where = ' WHERE itemid::text LIKE \'%' + item + '%\';'
        rs = con.execute(text(query_select + query_where))

        for row in rs:
            id += 1
            insert = bronze.insert().values(id =id, subject_id=row[1],hadm_id=row[2],stay_id= None,charttime=row[6].strftime("%Y-%m-%d %H:%M:%S"),storetime=row[7].strftime("%Y-%m-%d %H:%M:%S"),itemid=item,value=row[8],valuenum=row[9],valueuom=row[10],source_table="mimiciv_hosp.labevents")
            con.execute(insert)

    for item in query_d_icd_diagnoses:

        query_select = 'SELECT * FROM mimiciv_hosp.diagnoses_icd'
        query_where = ' WHERE icd_code::text LIKE \'%' + item + '%\';'
        rs = con.execute(text(query_select + query_where))

        for row in rs:
            id += 1
            insert = bronze.insert().values(id = id, subject_id=row[0], hadm_id= row[1], stay_id= None, charttime= None, itemid=item, storetime= None,value= None, valuenum=None, valueuom=None, source_table="mimiciv_hosp.diagnoses_icd")
            con.execute(insert)

