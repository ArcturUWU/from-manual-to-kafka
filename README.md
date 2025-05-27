# kafka and hand made synchronizing learn 🚀
Big project with 5 databases that can help you master Kafka and hand-made synchronizing with databases as Postgres, Elasticsearch, Mongo, Redis and Neo4j🔧

# Project Purpose and Background
This project aims to demonstrate a comprehensive understanding of Kafka and its applications by creating a suite of 5 databases that can be synchronized in real-time with hand made synchronizing services and kafka connectors configurations. The project targets young developers and data engineers looking to improve their skills in designing and implementing big data systems and CDC.

# Features and Functionality
The project includes the following key features:

* Supports 5 different databases: ElasticSearch, MongoDB, Neo4j, PostgreSQL, and Redis 🔥
* Provides real-time data synchronization between databases ⏱️
* Hand-made syncing using Python scripts 🔧
* Comprehensive documentation for easy setup and use 📚
* Synthetic data for you on University example to show how it actually work🚀

What sets this project apart is its unique combination of Kafka and multiple database support, making it an ideal showcase.

# Technology Stack
The project uses the following technologies:

* Python for scripting and data processing. Also data adding and deletion made on Python 💡
* Docker for containerization and easy deployment. You can easily deploy it by your side and study that architecture 🛠️
* No Next.js or React, as this project focuses on data processing and Kafka integration 💥

# Installation and Setup
To get started, follow these steps:

1. Make sure you have installed Docker on your machine 🛠️
2. Clone the repository and navigate to the project directory ⬇️
3. Run `docker-compose up -d` to start the containers 🔧
4. Configure the databases and Kafka settings as needed 🔧
5. Feel free to change databases passwords and my JWT token for auth but don't forget - auth by itself is required 🔧

# Kafka Setuo
To create connectors, follow this instruction:

1. It's important to firstly create tables before sending any connectors!
2. Go to `Debezium.txt` and copy-paste commands to post connctors(if you want to discover them - just open needed json)
3. Insert data
4. See magic
IMPORTANT: if you want to configure it on your database - don't forget to install needed connector class on docker-compose.yml and provide valid JSON.


# Usage Examples
To use the project, follow these basic steps:

1. Start the containers using `docker-compose up` (but first, see *Lab building* part) ⏱️
2. Run the Python scripts to start syncing data 💡
3. Use the command-line interface to monitor and control the syncing process ⏱️
4. use `postgres.py` to generate table structure on PostgreSQL
5. use `random_attendance_generator.py` to add some data only to postgres(maybe you'll need to complete this file to call `generate_students_and_attendance()` to insert data properly)
6. `total_generator.py` is needed to syncronise databases with postgres using HAND MADE method. It's just importing some of `sync` files so don't forget to check it out too!
7. You can generate up to 3 reports on this University data(attendance report, counting audience size for specific course and group report on listened hours)
8. Reports are generated through gateway that contrain auth with JWT that you need to pass in every querry.

Example code snippets and screenshots will be provided in the documentation 📚.

# Project Structure
The project structure is as follows:

* `elastic_gen_sync.py` generates and syncs data with ElasticSearch 🔥
* `elastic_output.py` outputs data from ElasticSearch 🔥
* `postgres.py` creates tables structure 🔥
* `mongo_sync.py` creates and synchronise mongo structure 🔥
* `neo4j_sync.py` creates and synchronise neo4j structure 🔥
* `redis_sync.py` creates and synchronise redis structure 🔥
* `purge.py` deletes all the data from all the databases 🔥
* `mongo_sink.py` is a hand made kafka cunsumer for mongo(default configs can't create hierarchy) 🔥

# Contributing Guidelines
Contributions are welcome! 👋 To contribute, follow these steps:

1. Fork the repository and create a new branch 🌐
2. Make your changes and commit them 💡
3. Create a pull request and describe your changes 📝
* Follow the code style and standards guide 📊

# Labs building
To use labs full functionally
1. Go straight to every lab directory that you want to build
2. Use `docker build -t lab{num}_app .` to build up lab containers(use whatever lab number instead of num).
3. Use `docker build -t gateway_app .` to build gateway.
4. create a db-network by yourself using `docker network create`
5. If you want to generate data - you should use (1) `random_attendance_generator.py` and (2) `total_generator.py`
6. total generator also synchronizing data to 4 other dbs so if you want to synk only needed one - make sure you commented needed rows
P.S. if you have a trouble with building services - try to rename `DOCKERFILE` to `Dockerfile` or `dockerfile`

# Labs info
Info about what every lab is doing

**Lab 1: Student Attendance Report**
Objective: Retrieve a report on the 10 students with the lowest lecture attendance percentage for a given term or phrase within a specified academic period.
Output includes:

1. Full student details
2. Attendance percentage
3. Reporting period
4. Matching course term/phrase

**Lab 2: Classroom Capacity Planning**
Objective: Calculate the required classroom capacity for a given course (semester/year), accounting for technical equipment requirements.
Output includes:

1. Full course and lecture details
2. Number of enrolled students

**Lab 3: Group Lecture Hours Analysis**
Objective: Generate a report for a specific student group, comparing attended vs. planned lecture hours across all courses tagged as department-specific disciplines.
Notes:

1. 1 lecture = 2 academic hours
2. Only tagged lectures are included

Output includes:

1. Group, student, and course details
2. Planned vs. attended hours

# License Information
This project is licensed under the MIT License 📜. You are free to use, modify, and distribute the project as you see fit, but please provide attribution and mention the original authors 🎉
