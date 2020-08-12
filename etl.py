""" 
    ℹ️Learning Moment:to import other python script files into the current file to make use of their content,use the following command:    
    import file without the '.py' extension. - eg import 'filename' - import connections- where connections is a python
    script file where the connection to the database is defined. 
"""
'''
Resources that can be used:
 ℹ️ pandas datframe to postgres tutorial:https://pythontic.com/pandas/serialization/postgresql
 To write a dataframe to csv file: dataFrame.to_csv (r'/home/dev/pythonprojects/virtualenv/python3.8.2/jobscraper
                                                        /query.csv', index = False,header=True)
'''

'''
    ℹ️Learning Moment:To load data or extract data from python to postgres, 2 libraries are needed.
        the psycopg2 is the driver that connects python to postgres & the sqlalchemy library which needs
        psycopg2. Sqlalchemy provides lostd of methods for datamanipulation. If you just want to insert data
        or do sql queries psycopg2 is enough you can use the connection.execute statement. Here I use sqlalchemy
        so that the data bulk insert of the dataframe was possible.
'''

# Todo: import required libraries

import scrapejobs as  sj    # user defined python script

import psycopg2
from sqlalchemy import create_engine # the library that is used to connect to postgres database

# To Do: Create an engine instance
# ℹ️connection string format:'postgres+psycopg2://<username>:<password>@<host>/<dbname>[?<options>]

alchemyEngine = create_engine('postgres+psycopg2://dev:dev@localhost/dev')

# To do: Connect to PostgreSQL server

dbConnection = alchemyEngine.connect();

# To do: Defining variables that will be rquired by the script

tempTable='temp_joblist'
postgreSQLTable = 'joblist'      

#To do: load the dataframe to a temp table using sqlalchemy

'''ℹ️Learning Moment:The dataframe obtained from scrapejobs.py,could not be directly used to run the
    dataframe.drop_duplicates() operation because of not hashable error, so as a work around, I exported it to a
    temporary db.
'''

try:
    sj.jobs_df.to_sql(tempTable, dbConnection, index= False, if_exists='replace');
    
except ValueError as vx:
    print(vx)

except Exception as ex:
    print(ex)
                 
'''
ℹ️Learning Moment: The program flow follows the following logic
    1. export the scrapejobs.py dataframe to a temp table (temp_joblist)
    2. extract the history of the records stored in the joblist table
    3. extract the records from the temp_joblist (which has all the job postings till the current date
        that would include records from history
    4. Join both data frames to one dataframe
    5. run the pandas-dataframe.drop_duplicates method to get unique records
    6. These steps are done because SQLalchemy doesnt support union query to do deduplication. 
    
'''
# To do: extract history from the joblist table:

''' step 1 use the "dbConnection"- to connect to the db
    step 2 use sqlalchemy from_sql function to load a new dataframe
'''

try:
    fromjoblist_df = sj.pd.read_sql("select * from \"joblist\"", dbConnection)
except Exception as ex:
    print(ex)
    
    
# To do: extract all the records from the temp_joblist table:
try:
    tempjoblist_df = sj.pd.read_sql("select * from \"temp_joblist\"", dbConnection)
except Exception as ex:
    print(ex)
    
# To merge two dataframes to get one Dataframe with distinct values:- since sqlalchemy doesnt support
# union statement: we have to concatenate the 2 dataframes then run a dropduplicate function.

try:
    bigdata = sj.pd.concat([tempjoblist_df, fromjoblist_df], ignore_index=True, sort=False)
except Exception as ex:
    print(ex)
    
#step 2: use the remove duplicate function to remove the duplicate values:

try:
    dedupe=bigdata.drop_duplicates()
except Exception as ex:
    print(ex)

# Final load of the dataframe to postgres table:

try:
    dedupe.to_sql(postgreSQLTable, dbConnection,index= False, if_exists='replace');
    
except ValueError as vx:
    print(vx)

except Exception as ex:
    print(ex)

else:
    print("PostgreSQL Table %s has been created successfully."%postgreSQLTable);

finally:
    dbConnection.close();
