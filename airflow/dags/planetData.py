import pandas as pd
import sqlalchemy as db


def get_planet_data():
    data = pd.read_csv('http://exoplanet.eu/catalog/csv/')
    data.rename(columns = {'# name':'name'}, inplace = True)
    DATABSE_LOCATION = "postgresql://airflow:airflow@host.docker.internal:5432/spaceData"
    engine = db.create_engine(DATABSE_LOCATION)
    data.to_sql('planetdata', engine, index=False, if_exists='append')

    remove_duplicates = ''' DELETE FROM planetData
                                WHERE ctid IN (SELECT min(ctid)
                                    FROM planetData
                                    GROUP BY name
                                    HAVING count(*) > 1
                                    )
                                RETURNING *;'''
    engine.execute(remove_duplicates)
    

