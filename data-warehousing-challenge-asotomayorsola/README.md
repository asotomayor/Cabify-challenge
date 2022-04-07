# Data Engineering Data Warehouse Code Challenge
## Problem
At Cabify, apart from taking great care of providing you as many ways of moving through the city as possible, we like to help bring about better decisions based on data.

## Task
We would like you to develop a `Python` job that calculates some stats and saves it in a relational database.
The job/s will use the [included dataset](./dataset.zip) that contains Cabify data. It will need to:
- Ingest the raw data into a relational database
- Also, build a top layer without duplicate data  

Additionally, once the data is saved, we would like you to build a python job that executes sql queries like:
- Aggregate table with the last `drop_off` by user.
- Aggregate table with the number of `drop_off` in the last 7, 15 and 30 days by user

## Additional considerations
- Note that the processes must be run `N times a day individually`.
- Choose the relational database that you want.
- Design as many jobs as you need.
- General software engineering patterns/good practices must be applied.
- Please `document` your decisions the same way you would be generating documentation for any other deliverable.
- Extra points if you provide additional stats.

## Last words
We are not looking for a perfect solution to this code challenge, it's more interesting for us to see your style and how you solve some problems. Even an incorrect result does not have to be bad. Please, be thorough explaining your thinking process in any part of the code challenge, and make sure to include any further instructions to run it.
