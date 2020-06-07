# ETL for Million Songs Dataset for Sparkify

The goal of this project is to perform data modeling with PostgreSQL on two main data sources:

* Data from the Million Songs dataset that consists of song and artist data
* Logs generated with this dataset and the event generator[1]

This documentation is split up into these following sections:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

## 1. Table Design and Schemas

#### Table schemas

**`songplays` schema**

| column name | datatype | size | constraint|
|-------------|----------|------|-----------|
| songplay_id |int       |-     |primary key|
| start_time  |bigint    |-     ||
| user_id     |int       |-     ||
| level       |text      |-     ||
| song_id     |varchar   |20    ||
| artist_id   |varchar   |20    ||
| session_id  |int       |-     ||
| location    |text      |-     ||
| user_agent  |text      |-     ||

**`users` schema**

| column name | datatype | size | constraint|
|-------------|----------|------|-----------|
| user_id     |int       |-     |primary key|
| first_name  |text      |-     ||
| last_name   |text      |-     ||
| gender      |varchar   |1     ||
| level       |text      |-     ||

**`songs` schema**

| column name | datatype | size | constraint|
|-------------|----------|------|-----------|
| song_id     |varchar   |20    |primary key|
| title       |text      |-     ||
| artist_id   |varchar   |20    ||
| year        |int       |-     ||
| duration    |float     |-     ||

**`artists` schema**

| column name | datatype | size | constraint|
|-------------|----------|------|-----------|
| artist_id   |varchar   |20    |primary key|
| name        |text      |-     ||
| location    |text      |-     ||
| latitude    |float     |-     ||
| longitude   |float     |-     ||

**`time` schema**

| column name | datatype | size | constraint|
|-------------|----------|------|-----------|
| start_time  |bigint    |      |primary key|
| hour        |int       |-     ||
| day         |int       |-     ||
| week        |int       |-     ||
| year        |int       |-     ||
| weekday     |int       |-     ||

#### Comments on schema choices

* `song_id` and `artist_id` are set to `varchar(20)` since these unique identifiers for each artist and song are 20 character alphanumeric codes
* `start_time` is treated as `bigint` since it has integer values bigger than 2147483647, which is the limit for `int` in PostgreSQL 

### Table design

When designing the tables, the starting point was the requirement for each table itself i.e what goes into `songplays`, `songs`, etc. The types of these attributes was studied from the dataset and a decision on the datatype to use for the schema was made. The decisions made with regards to the tables `songs`, `artists` and `time`, their primary keys were motivated by the fact that the single source of truth for these is the fact table `songplays`. There might be multiple instances of a song or artist or timestamp but the _facts_ related to each ID will not change. For example, for a specific timestamp xxxxxxx, the hour, day, week and year will be the same, regardless of where it's encountered in our dataset. We use `ON CONFLICT` clauses to deal with key conflicts in these cases. However, for `users`, we consider the assumption that a user might, over time, switch to a paid tier from a free service tier and deal with this in our cluase.

## 2. Data extraction and transformation

The data provided is a subset of the actual log and song data. Extraction is done differently for these two type of files - **log** files and **song** files. Song files are used to populate our dimension tables `songs` and `artists`. Each file contains information about a song, its artist and metadata on the artist. We use the identifiers provided in the song JSON files to index our artists and songs. Log files are structured differently - each JSON file has multiple logs and each of these logs is a separate record. As such, we process each row and insert to `songplays`. Using `COPY` was considered but the decision to simply iterate over each row was made because using `COPY` would entail creating (possibly temporary) CSV/JSON files. Theoretically, these files, with the right structure post transformation could be directly inserted into the database.


Most transformation is done with the help of `pandas` - we load each file, select specific attributes and carry out any transformations we want to. Each utility function used in the ETL pipeline is documented in `etl.py` - `etl.ipynb` was strictly used for prototyping and initial checks on the data & queries.


## 3. Explanation of files in the repository

* `create_tables.py`: Script that drops and recreates tables. It does not define any queries of its own and imports queries from `sql_queries.py`

* `sql_queries.py`: List of queries for our project. This consists of `INSERT`, `DROP` and `CREATE` queries for all our fact and dimension tables

* `etl.ipynb`: Jupyter notebook that acts as a testing environment to check the dataset structure, data integrity, transformations on a subset of our data and verify the correctness of our pipeline

* `test.ipynb`: Contains a list of SQL queries to test the data piped into our database. It also contains a small analytics dashboard with some observations on the dataset

* `etl.py`: Dataset level implementation of the transformations we tested in `etl.ipynb`. This is our complete pipeline code - it extracts data from song and log files in `data/`, runs transformations on them (that we tested) and loads them into our PostgreSQL database

## 4. Running the scripts

To run the scripts:

**Inside a terminal:**

```
python create_tables.py
python etl.py
```

If the ETL process ran correctly, you should see 100% of the song and log files processed. The next step is to take a look at the simple analytics queries provided in `test.ipynb`. These queries try to gain some insight into how this data can help Sparkify as a product and learn some patterns about its userbase.

----
### References

[1] - https://github.com/Interana/eventsim