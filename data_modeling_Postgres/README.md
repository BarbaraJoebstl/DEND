# Sparkify Data Pipeline

## About 
We have data about songs and user activity for the music streaming app Sparkify.
With the help of two datasets
 - song dataset
 - log dataset 
 we create a database schema and ETL pipeline for an easier and better analysis of the data.
 
## Schema
We create a star schema to optimize queries for our analysis.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong

### Dimension Tables
*users*: users in the app
*songs*: - songs in music database
*artists*: - artists in music database
*time*: - timestamps of records in songplays broken down into specific units

![Image caption](https://user-images.githubusercontent.com/6280553/101984548-03102b00-3c7a-11eb-9f55-d1fdd800a2ad.png)

### ETL (Extract, Transform, Load) Process
#### Create Tables
`create_tables.py` is used to establish the database connection and create or delete tables.
`sql_queries.py` is used to create the above mentioned tables.
`test.ipynb` is used to test and see if tables are created properly and data can be stored.Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

#### Process Data
The `etl.ipynb` is used to test the sequences. 
First we load data from _song_data_ and extract the values for the *song table* and *artist table*. 
Then we load data from _log_data_ and extract values for the *user table* and the *time table*.
In the last step we combine the extracted data into *songplays table*.

The code used in `etl.py` is based on the notebook code.

Once the whole pipeline works, everything can be run with:

`!python create_tables.py` - delete previous and create new tables
`!python etl.py` - !python create_tables.py

from the console.

To make sure everything was inserted correctly run `test.ipynb` again.

### Furter Information
[Cheatsheet and more info links](https://video.udacity-data.com/topher/2019/October/5d9683fc_dend-p1-lessons-cheat-sheet/dend-p1-lessons-cheat-sheet.pdf)