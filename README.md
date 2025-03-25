# Airflow DAG: La Liga Top Scorers ETL ⚽

This is a quick project to explore basic Apache Airflow functionality. The project includes a DAG that performs basic ETL (Extract, Transform, Load) tasks. It pulls top scorer data from a football API, transforms the response into the correct format, and saves the results to a local CSV file.

## DAG Overview

The DAG consists of three tasks:

1. **Extract Task**:  
   Pulls top scorer data from the [Football Data API](https://www.football-data.org/). This task sends a request to the API and retrieves data about football scorers.

2. **Transform Task**:  
   Transforms the response from the API into a Pandas DataFrame, selecting relevant fields such as player, team, goals, assists, penalties, and played matches.

3. **Load Task**:  
   Saves the transformed data into a local CSV file (`top_scorers.csv`).

## Workflow

- **Extract**: Pulls the top scorers data from the API.
- **Transform**: Processes the data into a structured format (a Pandas DataFrame).
- **Load**: Saves the transformed data into a local CSV file.

## Example of the Transformed Data

The CSV file will contain the following columns:

- **player**: The name of the player.
- **team**: The team the player belongs to.
- **goals**: The number of goals scored.
- **assists**: The number of assists made.
- **penalties**: The number of penalties scored.
- **playedMatches**: The number of matches the player has played.

Example:

```csv
player,team,goals,assists,penalties,playedMatches
Robert Lewandowski,FC Barcelona,22,2.0,3.0,27
Kylian Mbappé,Real Madrid CF,20,3.0,5.0,26
Ante Budimir,CA Osasuna,15,2.0,7.0,28
