## Data Modeling with Postgres

A sample company, Sparkify, has provided data from their music streaming app in JSON format, [song data](data/song_data/) and [log data](data/log_data/). The goal is to model a relational database using these JSON files and create an ETL pipeline so it can be queried to understand what songs their users are listening to. The data is a sample subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

<hr>

### A. Create a relational database
Note: The database is created locally with a sample username and password. To use this on your local environment, all connections to the databse must be replaced with your username and password.

1. First, I modeled the following tables, their relationships and constraints in a star schema. Each of the dimension tables are denormalized so there are duplicate references to data but this allows for fast queries on simple joins.

   #### `songplays` (fact table)
   | Column Name | Data Source | Constraints |
   | ----------- | ----------- | ----------- |
   | `songplay_id` | auto-generated SERIAL | PRIMARY KEY | 
   | `start_time` | `ts` from [log data](data/log_data/) converted to timestamp | FOREIGN KEY references time|
   | `user_id` | `userId` from [log data](data/log_data/) | FOREIGN KEY references users |
   | `level` | `level` from [log data](data/log_data/) | NOT NULL |
   | `song_id` | `song_id` from [song data](data/song_data/) | |
   | `artist_id` | `artist_id` from [song data](data/song_data/) | |
   | `session_id` | `sessionId` from [log data](data/log_data/) | NOT NULL |
   | `location` | `location` from [log data](data/log_data/) | NOT NULL |
   | `user_agent` | `userAgent` from [log data](data/log_data/) | NOT NULL |

   #### `songs` (dimension table)
   | Column Name | Data Source | Constraints |
   | ----------- | ----------- | ----------- |
   | `song_id` | `song_id` from [song data](data/song_data/) | PRIMARY KEY |
   | `title` | `title` from [song data](data/song_data/) | NOT NULL |
   | `artist_id` | `artist_id` from [song data](data/song_data/) | REFERENCES artists |
   | `year` | `year` from [song data](data/song_data/) | |
   | `duration` | `duration` from [song data](data/song_data/) | NOT NULL |

   #### `artists` (dimension table)
   | Column Name | Data Source | Constraints |
   | ----------- | ----------- | ----------- |
   | `artist_id` | `artist_id` from [song data](data/song_data/) | PRIMARY KEY |
   | `name` | `artist_name` from [song data](data/song_data/) | NOT NULL |
   | `location` | `artist_location` from [song data](data/song_data/) | |
   | `latitude` | `artist_latitude` from [song data](data/song_data/) | |
   | `longitude` | `artist_longitude` from [song data](data/song_data/) | |

   #### `time` (dimension table)
   | Column Name | Data Source | Constraints |
   | ----------- | ----------- | ----------- |
   | `start_time` | `ts` from [log data](data/log_data/) converted to timestamp | PRIMARY KEY |
   | `hour` | hour (`%H`) parsed from `start_time` | NOT NULL |
   | `day` | day (`%d`) parsed from `start_time` | NOT NULL |
   | `week` | week (`%U`) parsed from `start_time` | NOT NULL |
   | `month` | month (`%m`) parsed from `start_time` | NOT NULL |
   | `year` | year (`%Y`) parsed from `start_time` | NOT NULL |
   | `weekday` | weekday (`%u`) parsed from `start_time` | NOT NULL |
   
   #### `users` (dimension table)
   | Column Name | Data Source | Constraints |
   | ----------- | ----------- | ----------- |
   | `user_id` | `userId` from [log data](data/log_data/) | PRIMARY KEY |
   | `first_name ` | `firstName` from [log data](data/log_data/) | NOT NULL |
   | `last_name` | `lastName` from [log data](data/log_data/) | NOT NULL |
   | `gender ` | `gender` from [log data](data/log_data/) | |
   | `level ` | `level` from [log data](data/log_data/) | |

2. Then in [sql_queries.py](sql_queries.py), I wrote SQL queries to CREATE, INSERT INTO, and DROP each table in the database. The last query selects `song_id` and `artist_id` from a table JOIN (`songs` and `artists`) to then insert into the `songplays` table. 

3. [create_tables.py](create_tables.py) connects to the database using Psycopg (a Postgres wrapper) to drop any existing tables and create new tables for each session.  

<hr>

### B. Create an ETL Pipeline

1. **Extract**: In [etl.ipynb](etl.ipynb), I created a connection to the database and developed an ETL procedure for each table, converting the JSON to Python dataframes according to the model schema above. 
2. **Transform**: Some of the [log data](data/log_data/) required cleaning of null values, duplicates and data types. In addition, since the original data was only a subset, many of the songs/artists in the [song data](data/song_data/) were not found in the [log data](data/log_data/). These logs were still recorded in `songplays` but missing `song_id` and `artist_id` were recorded as `None` if they were not found.
3. **Load**: Once transforming the data was complete for each table, the data was loaded into the database using Psycopg to execute the SQL insert queries derived from [sql_queries.py](sql_queries.py).
4. The relevant code from [etl.ipynb](etl.ipynb) was transferred to [etl.py](etl.py) to be able to run from bash/Terminal.
5. [test.ipynb](test.ipynb) was used to confirm the tables were created properly and the data was loaded.

<hr>

### C. Execute the app in Bash/Terminal
1. To run the program, first run `python create_tables.py` to drop any old tables and create new empty tables.
2. Then run `python etl.py` to perform ETL on any sample data in the `data/` folder.