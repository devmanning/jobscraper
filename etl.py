""" to import other python script files into the current file to make use of their content,use the following command:
    
    import file without the '.py' extension. - eg import 'filename' - import connections- where connections is a python
    script file where the connection to the database is defined. 

"""

# pandas datframe to postgres tutorial:https://pythontic.com/pandas/serialization/postgresql

#import connections
import scrapejobs as  sj
import psycopg2

from sqlalchemy import create_engine # the library that is used to connect to postgres database
# Create an engine instance
# connection string format:'postgres+psycopg2://<username>:<password>@<host>/<dbname>[?<options>]

alchemyEngine = create_engine('postgres+psycopg2://dev:dev@localhost/dev')

# Connect to PostgreSQL server

dbConnection    = alchemyEngine.connect();

tempTable='temp_joblist'
postgreSQLTable = 'joblist'      

#To do: load the dataframe to a temp table using sqlalchemy

try:
    sj.jobs_df.to_sql(tempTable, dbConnection, if_exists='replace');
    
except ValueError as vx:
    print(vx)

except Exception as ex:
    print(ex)
                 

# To do extract all the records from the joblist table:
# step 1 use the "dbConnection"- to connect to the db

# step 2 use sqlalchemy from_sql function to load a new dataframe

try:
    dataFrame = sj.pd.read_sql("select * from \"joblist\"", dbConnection)
except Exception as ex:
    print(ex)
    
# To do extract all the records from the temp_joblist table:
# step 1 use the "dbConnection"- to connect to the db

# step 2 use sqlalchemy from_sql function to load a new dataframe

try:
    tempdataFrame = sj.pd.read_sql("select * from \"temp_joblist\"", dbConnection)
except Exception as ex:
    print(ex)
    
# To merge two dataframes to get one Dataframe with distinct values:- since sqlalchemy doesnt support
# union statement: we have to concatenate the 2 dataframes then run a dropduplicate function.

try:
    bigdata = sj.pd.concat([tempdataFrame, dataFrame], ignore_index=True, sort=False)
    
except Exception as ex:
    print(ex)

#step 2: use the remove duplicate function to remove the duplicate values:

try:
    bigdata.drop_duplicates()
    
except Exception as ex:
    print(ex)

# Final load of the dataframe to postgres table:
try:
    frame = bigdata.to_sql(postgreSQLTable, dbConnection, if_exists='replace');
    
except ValueError as vx:
    print(vx)

except Exception as ex:
    print(ex)

else:
    print("PostgreSQL Table %s has been created successfully."%postgreSQLTable);

finally:
    dbConnection.close();
