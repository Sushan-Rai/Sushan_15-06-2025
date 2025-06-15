# README for Store Monitoring Backend Application

## Project Overview

This project is a backend application designed to monitor the online status of various restaurants in the US. It provides APIs to generate reports on the uptime and downtime of these restaurants during their business hours.

## Data Sources

The application utilizes three primary data sources:

1. **Store Activity Data**: 
   - CSV file containing columns: `store_id`, `timestamp_utc`, `status` (active/inactive).
   - Polling occurs roughly every hour, with timestamps in **UTC**.

2. **Business Hours Data**: 
   - Schema: `store_id`, `dayOfWeek` (0=Monday, 6=Sunday), `start_time_local`, `end_time_local`.
   - Times are in the **local time zone**. If missing, assume 24/7 operation.

3. **Store Timezone Data**: 
   - Schema: `store_id`, `timezone_str`.
   - If missing, default to **America/Chicago**.

## Tech Stack

- **Framework**: FastAPI
  - FastAPI is used for building the APIs, providing high performance and easy-to-use features for creating RESTful services.

- **Database**: PostgreSQL
  - PostgreSQL is utilized to store the data from the CSV files, allowing for efficient querying and data management.

### Running the Application

To run the application, use the following command:

```bash
fastapi dev ./src/main.py
```

### Database Queries

The `db.py` file contains all the queries related to converting the static dataset into tables. It handles the ingestion of CSV data into the PostgreSQL database and manages the necessary transformations for report generation.

## API Endpoints

### 1. `/trigger_report`

- **Method**: POST
- **Input**: None
- **Output**: 
  - `report_id`: A random string used to track report generation status.

### 2. `/get_report`

- **Method**: GET
- **Input**: 
  - `report_id`: The ID of the report to check.
- **Output**: 
  - If report generation is ongoing: `"Running"`
  - If complete: `"Complete"` along with the CSV file containing the report.
 
## Report Link

https://drive.google.com/drive/folders/1UE0hYEW1MW1blWRdt4NuutfZXb9BszuV?usp=sharing

## Data Output Schema

The report will include the following fields:

```
store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), uptime_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)
```

- Uptime and downtime calculations are based on business hours and extrapolated from polling data.

## Future Improvements

1. **Security Enhancements**:
   - Implement rate limiting and IP blocking to protect APIs from abuse.

2. **Performance Optimization**:
   - Utilize Redis caching for SQL queries to improve response times.

3. **Deployment**:
   - Consider containerization (e.g., using Docker) for easier deployment and scalability.

## Acknowledgments

Thank you for considering this project. I look forward to your feedback!
