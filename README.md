# BITA


The solution built has the following assumptions:

A postgre SQL server running on localhost and port 5432 which is default

A user by the name of "stockbroker" and password "stock"

A database by the name of "stocks" with all the privileges to this schema granted to the stockbroker user

A schema by the name of "stocks" with all the privileges to this schema granted to the stockbroker user

If you want to use a different user database and schema, then kindly make the adjustments in the python code files and csv.load file accordingly


The solution uses the following libraries to accomplish the task, reasons are provided

pandas : For reading the csv file into a dataframe so that it can be pushed in the database table using the sqlalchemy engine and the connection.

pgloader tool: This a tool specially made for bulk uploads, Installing this tool on linux machine or mac is fairly straightforward and I found its performance and efficiency to be quite good, I was able to load the file into the database in about 7 to 8 minutes approximately utilizing on 200MB of memory, besides this tool gives the possiblilty to do a parallel upload. However, The only drawback I found is that this tool does not have a straightfoward installation process for windows, and upon doing some research, I found that windows users have reported issues , however it seems to be popular amongst mac and linux users. 

sqlalchemy: This is one of the most popular libraries out there besides psycopg2 and provides the possibility to do a bulk upload to the table without leveraging the COPY function of the postgresql db. This library was utilized with Pandas library as an alternative to the pgloader tool so that the solution works as expected on both windows and linux machines. Complemented with the pandas library, I found its performance to be quite close to the pgloader tool, It was able to complete the upload in less than 10 minutes on average.

psycopg2: This library is using for interfacing with the database, checking if table exists, creating a new table, deleting existing data on the table and other house keeping stuff.

  ###############################################################################

Before running the stockimporter.py file please make sure, you copy the Stock.CSV file into the folder

The previousImport.json file is used as a mock datastore to keep track of the previous imports, the script checks
the datestring inside the json object to ascertain if there was a previous import made, if yes then the previous
imported data is deleted, if a null value is found then it proceeds to begin the bulk upload and at the end of each upload, the value of the datestring is updated. In a real world deployment, I would save this information differently for example in a dynamodb table or a persistent storage or state variable. For the sake of simplicity,I followed this approach.


