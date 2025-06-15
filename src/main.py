from fastapi import FastAPI,BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import os 
import uuid
import psycopg2
from datetime import timedelta,datetime,time
from dotenv import load_dotenv
import pytz
import csv

load_dotenv()
app = FastAPI()
REPORTS = {}
CSV_DIR = os.getenv("CSV_DIR")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

conn = None
cur = None
@app.on_event("startup")
async def database_connection():
    global conn,cur
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

@app.on_event("shutdown")
async def database_termination():
    if cur:
        cur.close()
    if conn:
        conn.close()

# def uptime_downtime_hour(store_id,current_timestamp,last_hour,local_time_range,current_local_timestamp):
#     cur.execute("""SELECT (timestamp_utc,status) FROM store_status
#                 WHERE store_id=%s AND timestamp_utc >= %s AND timestamp_utc < %s 
#                 ORDER BY timestamp_utc;""",(store_id,last_hour,current_timestamp))
#     uptime_downtime_hours_res = cur.fetchall()
#     print(uptime_downtime_hours_res,local_time_range)
#     return uptime_downtime_hours_res


# def uptime_downtime_hour(store_id, current_timestamp, last_hour, local_time_range, current_local_timestamp, timezone):
#     cur.execute("""
#         SELECT timestamp_utc, status 
#         FROM store_status
#         WHERE store_id = %s 
#           AND timestamp_utc >= %s 
#           AND timestamp_utc < %s 
#         ORDER BY timestamp_utc;
#     """, (store_id, last_hour, current_timestamp))
    
#     uptime_downtime_hours_res = cur.fetchall()
    
#     start_time = []
#     end_time = []
#     for local_time in local_time_range:
#         print(local_time)
#         range_str = local_time.strip('()')
#         start_str, end_str = range_str.split(',')
#         start_time.append(time.fromisoformat(start_str.strip()))
#         end_time.append(time.fromisoformat(end_str.strip()))

#     store_tz = pytz.timezone(timezone)
#     uptime = 0
#     downtime = 0

#     uptime_downtime_hours_res.append((current_timestamp, None))

#     for i in range(len(uptime_downtime_hours_res) - 1):
#         ts_utc, status = uptime_downtime_hours_res[i]
#         next_ts_utc, _ = uptime_downtime_hours_res[i + 1]
#         ts_local = ts_utc.astimezone(store_tz)
#         local_t = ts_local.time()
#         duration = (next_ts_utc - ts_utc).total_seconds() / 60
#         for i in range(len(start_time)):
#             in_range = start_time[i] <= local_t <= end_time[i]

#             if status == "active" and in_range:
#                 uptime += duration
#             elif status == "inactive" and in_range:
#                 downtime += duration

#     return {
#         "uptime_last_hour": round(uptime),
#         "downtime_last_hour": round(downtime)
#     }

def uptime_downtime_hour(store_id, current_timestamp, last_hour, local_time_ranges, timezone_str):
    cur.execute("""
        SELECT timestamp_utc, status 
        FROM store_status
        WHERE store_id = %s 
          AND timestamp_utc > %s 
          AND timestamp_utc <= %s 
        ORDER BY timestamp_utc;
    """, (store_id, last_hour, current_timestamp))
    
    uptime_downtime_hours_res = cur.fetchall()
    store_tz = pytz.timezone(timezone_str)
    uptime = 0
    downtime = 0

    if isinstance(local_time_ranges[0], str):
        local_time_ranges = [local_time_ranges]

    ranges = []
    for r in local_time_ranges:
        s, e = r[0].strip('()').split(',')
        ranges.append((time.fromisoformat(s.strip()), time.fromisoformat(e.strip())))

    uptime_downtime_hours_res.append((current_timestamp, None))

    for i in range(len(uptime_downtime_hours_res) - 1):
        ts_utc, status = uptime_downtime_hours_res[i]
        next_ts_utc, _ = uptime_downtime_hours_res[i + 1]
        ts_local = ts_utc.astimezone(store_tz)
        local_t = ts_local.time()
        duration = (next_ts_utc - ts_utc).total_seconds() / 60
        in_range = any(start <= local_t <= end for start, end in ranges)

        if status == "active" and in_range:
            uptime += duration
        elif status == "inactive" and in_range:
            downtime += duration

    return {
        "uptime_last_hour": round(uptime),
        "downtime_last_hour": round(downtime)
    }

def uptime_downtime_week(store_id, current_timestamp, last_day, local_time_ranges, timezone_str):
    cur.execute("""
        SELECT timestamp_utc, status 
        FROM store_status
        WHERE store_id = %s 
          AND timestamp_utc > %s 
          AND timestamp_utc <= %s 
        ORDER BY timestamp_utc;
    """, (store_id, last_day, current_timestamp))
    
    res = cur.fetchall()
    store_tz = pytz.timezone(timezone_str)
    uptime = 0
    downtime = 0

    if isinstance(local_time_ranges[0], str):
        local_time_ranges = [local_time_ranges]

    ranges = []
    for r in local_time_ranges:
        s, e = r[0].strip('()').split(',')
        ranges.append((time.fromisoformat(s.strip()), time.fromisoformat(e.strip())))

    res.append((current_timestamp, None))

    for i in range(len(res) - 1):
        ts_utc, status = res[i]
        next_ts_utc, _ = res[i + 1]
        ts_local = ts_utc.astimezone(store_tz)
        local_t = ts_local.time()
        duration = (next_ts_utc - ts_utc).total_seconds() / 3600  

        in_range = any(start <= local_t <= end for start, end in ranges)

        if status == "active" and in_range:
            uptime += duration
        elif status == "inactive" and in_range:
            downtime += duration

    return {
        "uptime_last_week": round(uptime, 2),
        "downtime_last_week": round(downtime, 2)
    }


def uptime_downtime_day(store_id, current_timestamp, last_day, local_time_ranges, timezone_str):
    cur.execute("""
        SELECT timestamp_utc, status 
        FROM store_status
        WHERE store_id = %s 
          AND timestamp_utc > %s 
          AND timestamp_utc <= %s 
        ORDER BY timestamp_utc;
    """, (store_id, last_day, current_timestamp))
    
    res = cur.fetchall()
    store_tz = pytz.timezone(timezone_str)
    uptime = 0
    downtime = 0

    if isinstance(local_time_ranges[0], str):
        local_time_ranges = [local_time_ranges]

    ranges = []
    for r in local_time_ranges:
        s, e = r[0].strip('()').split(',')
        ranges.append((time.fromisoformat(s.strip()), time.fromisoformat(e.strip())))

    res.append((current_timestamp, None))

    for i in range(len(res) - 1):
        ts_utc, status = res[i]
        next_ts_utc, _ = res[i + 1]
        ts_local = ts_utc.astimezone(store_tz)
        local_t = ts_local.time()
        duration = (next_ts_utc - ts_utc).total_seconds() / 3600  

        in_range = any(start <= local_t <= end for start, end in ranges)

        if status == "active" and in_range:
            uptime += duration
        elif status == "inactive" and in_range:
            downtime += duration

    return {
        "uptime_last_day": round(uptime, 2),
        "downtime_last_day": round(downtime, 2)
    }

def uptime_and_downtime_per_store(store_id,current_timestamp):
    cur.execute("SELECT timezone_str FROM timezones WHERE store_id=%s",(store_id,))
    timezone = cur.fetchone()[0]
    last_hour = current_timestamp - timedelta(hours=1)
    last_day = current_timestamp - timedelta(days=1)
    last_week = current_timestamp - timedelta(weeks=1)

    date = datetime.fromisoformat(str(current_timestamp))
    date_hour = datetime.fromisoformat((str(last_hour)))

    tz = pytz.timezone(timezone)

    current_local_timestamp = date.astimezone(tz)
    last_hour_local_timestamp = date_hour.astimezone(tz)

    day = current_local_timestamp.weekday()
    hour_day = last_hour_local_timestamp.weekday()
    cur.execute("""SELECT (start_time_local, end_time_local) FROM store_business_hours 
                    WHERE store_id=%s AND dayOfWeek=%s;""",(store_id, day))
    local_time_ranges = cur.fetchall()
    if not local_time_ranges:
        local_time_ranges = ["(00:00:00,23:59:59)"]

    if day != hour_day:
        cur.execute("""SELECT (start_time_local, end_time_local) FROM store_business_hours 
                WHERE store_id=%s AND dayOfWeek=%s;""",(store_id, hour_day))
        local_time_ranges_before_hour = cur.fetchall()
        if not local_time_ranges_before_hour:
           local_time_ranges_before_hour = ["(00:00:00,23:59:59)"]
        previous_day_end = (current_local_timestamp - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        previous_day_end = previous_day_end.astimezone(pytz.utc)
        uptime_downtime_hours_previous_day = uptime_downtime_hour(store_id,previous_day_end,last_hour,local_time_ranges_before_hour,timezone)
        uptime_downtime_hours = uptime_downtime_hour(store_id,current_timestamp,previous_day_end,local_time_ranges,timezone)
        uptime_downtime_hours.update(uptime_downtime_hours_previous_day)
    else:  
        uptime_downtime_hours = uptime_downtime_hour(store_id,current_timestamp,last_hour,local_time_ranges,timezone)

    cur.execute("""SELECT (start_time_local, end_time_local) FROM store_business_hours 
                WHERE store_id=%s AND dayOfWeek=%s;""",(store_id, hour_day))
    local_time_ranges_before_day = cur.fetchall()
    if not local_time_ranges_before_day:
        local_time_ranges_before_day = ["(00:00:00,23:59:59)"]
    previous_day_end = (current_local_timestamp - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    previous_day_end = previous_day_end.astimezone(pytz.utc)
    uptime_downtime_previous_day = uptime_downtime_day(store_id,previous_day_end,last_day,local_time_ranges_before_day,timezone)
    uptime_downtime_days = uptime_downtime_day(store_id,current_timestamp,previous_day_end,local_time_ranges,timezone)
    uptime_downtime_days.update(uptime_downtime_previous_day) 
    uptime_downtime_weeks = [{} for i in range(7)]
    for i in range(7):
        cur.execute("""SELECT (start_time_local, end_time_local) FROM store_business_hours 
                WHERE store_id=%s AND dayOfWeek=%s;""",(store_id, day))
        local_time_ranges = cur.fetchall()
        if not local_time_ranges:
            local_time_ranges = ["(00:00:00,23:59:59)"]
        previous_day_end = (current_local_timestamp - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        previous_day_end = previous_day_end.astimezone(pytz.utc)
        if i == 6:
            uptime_downtime_weeks[i] = uptime_downtime_week(store_id,current_timestamp,last_week,local_time_ranges,timezone)
        else:
            uptime_downtime_weeks[i] = uptime_downtime_week(store_id,current_timestamp,previous_day_end,local_time_ranges,timezone)
        
        current_timestamp = previous_day_end
        day-=1
        if day < 0:
            day = 6
    
    uptime_downtime_weeks_result = {
        "uptime_last_week": 0,
        "downtime_last_week": 0
    }
    for i in uptime_downtime_weeks:
        uptime_downtime_weeks_result["uptime_last_week"] += i.get("uptime_last_week", 0)
        uptime_downtime_weeks_result["downtime_last_week"] += i.get("downtime_last_week", 0)
    store_id = {"store_id":store_id}
    store_id.update(uptime_downtime_hours)
    store_id.update(uptime_downtime_days)
    store_id.update(uptime_downtime_weeks_result)

    return store_id

def generate_report(report_id):
    file_path = os.path.join(CSV_DIR, f"{report_id}.csv")
    cur.execute("SELECT MAX(timestamp_utc) FROM store_status;")
    current_timestamp = cur.fetchone()[0]

    cur.execute("SELECT store_id FROM timezones;")
    rows = cur.fetchall()
    report = []
    for i in rows:
        report.append(uptime_and_downtime_per_store(i[0],current_timestamp))

    if report:
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=report[0].keys())
            writer.writeheader()
            writer.writerows(report)

    REPORTS[report_id]["status"] = "Complete"
    REPORTS[report_id]["file_path"] = file_path

@app.get('/trigger_report')
#triggering report generation
def trigger_report(background_tasks: BackgroundTasks):
    #generates a random unique string
    report_id = str(uuid.uuid4())
    REPORTS[report_id] = {"status": "Running", "file_path": None}
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    report = REPORTS.get(report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if report["status"] == "Running":
        return {"status": "Running"}

    response = FileResponse(report["file_path"], media_type="text/csv", filename=f"{report_id}.csv")
    #Header response of HTTP to be set to complete
    response.headers["X-Report-Status"] = "Complete"
    return response


