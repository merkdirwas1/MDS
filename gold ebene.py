import csv
import collections
from sqlalchemy import text
from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String, TIMESTAMP,Numeric, Boolean
import os
import yaml
from string import Template

# read config
with open('config.yaml', 'r') as file:
    config_template = Template(file.read())

config_str = config_template.substitute(os.environ)
config = yaml.safe_load(config_str)
db_config = config['database']

user = db_config['user']
password = db_config['password']
host = db_config['host']
port = str(db_config['port'])
database = db_config['database']
engine = create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+database, echo=True)
silver = db_config['schema_silver']
table_silver= db_config['table_silver']

print('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+database)


gcs_config = config['parameters_gcs']

with engine.begin() as con:
    data_dict = {}
    rs = con.execute(text('SELECT * FROM '+silver+'.'+table_silver))
    for row in rs:
        hadm_id = int(row[2])
        char_time = row[4].strftime("%Y-%m-%d %H:%M:%S")
        if hadm_id not in data_dict:
            data_dict[hadm_id] = {char_time:[]}
        else:
            data_dict[hadm_id][char_time] = []


    for container in gcs_config:
        rs = con.execute(text('SELECT * FROM ' + silver + '.' + table_silver))
        for row in rs:
            hadm_id = int(row[2])
            char_time = row[4].strftime("%Y-%m-%d %H:%M:%S")
            if row[6] == str(container['concept_id']):
                data_dict[hadm_id][char_time].append(row[8])
            else:
                continue

    #bereinungung
    for hadm_id in data_dict:
        tmp = []
        tmp2 = []
        for datetime in data_dict[hadm_id]:
            if data_dict[hadm_id][datetime] ==[]:
                tmp.append(datetime)
            if None in data_dict[hadm_id][datetime]:
                index = [i for i, e in enumerate(data_dict[hadm_id][datetime]) if e == None]
                for missing_value in index:
                    data_dict[hadm_id][datetime][missing_value] = tmp2[missing_value]
            else:
                tmp2.append(data_dict[hadm_id][datetime])
        for datetime in tmp:
            del data_dict[hadm_id][datetime]

    #create gold_gcs

    schema = db_config['schema_gold']
    table = db_config['table_gold_gcs']

    con.execute(text("CREATE SCHEMA IF NOT EXISTS " + schema + ";"))
    metadata = MetaData(schema)

    gold_gcs = Table(
        table, metadata,
        Column('id', Integer, primary_key=True),
        Column('subject_id', Integer),
        Column('hadm_id', Integer),
        Column('stay_id', Integer),
        Column('window_start', TIMESTAMP),
        Column('window_end', TIMESTAMP),
        Column('concept', Integer),
        Column('concept_count', Integer),
        Column('concept_4152194_imputed', Boolean),
        Column('score', Numeric),
    )
    metadata.create_all(engine)
    id = 0
    for hadm_id in data_dict:
        rs = con.execute(text('SELECT * FROM silver.standardized_parameters WHERE hadm_id::TEXT LIKE \'%' + str(hadm_id) + '%\';'))
        data = rs.first()
        for datetime in data_dict[hadm_id]:
            score = sum(data_dict[hadm_id][datetime])
            insert = gold_gcs.insert().values(id=id, subject_id=data[0], hadm_id=data[1], stay_id=data[2], window_start=datetime, window_end=datetime,concept=4093836,concept_count=1,concept_4152194_imputed=False,score=score )
            con.execute(insert)
            id += 1
