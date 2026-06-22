from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import pymysql
import pymysql.cursors
import os
from datetime import datetime
from typing import Optional

load_dotenv()

app = FastAPI(
    title="Rivery Pipeline Monitor API",
    description="API to monitor Rivery pipeline logs, health, metrics and summaries",
    version="1.0.0"
)

# ── DB Connection ─────────────────────────────────────────────
def get_db():
    try:
        conn = pymysql.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


# ─────────────────────────────────────────────────────────────
# 1. ROOT
# ─────────────────────────────────────────────────────────────
@app.get("/", tags=["Root"])
def root():
    return {"message": "Rivery Pipeline Monitor API is running!"}


# ─────────────────────────────────────────────────────────────
# 2. HEALTH
# ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
def health():
    try:
        conn = get_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as total FROM rivery_runs")
            total = cursor.fetchone()["total"]
        conn.close()
        return {
            "status": "healthy",
            "database": "connected",
            "total_logs_stored": total,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# ─────────────────────────────────────────────────────────────
# 3. PIPELINES
# ─────────────────────────────────────────────────────────────
@app.get("/pipelines", tags=["Pipelines"])
def get_pipelines():
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                river_name,
                river_id,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
                MAX(start_time) as last_run_time,
                (SELECT status FROM rivery_runs r2
                 WHERE r2.river_name = r1.river_name
                 ORDER BY start_time DESC LIMIT 1) as last_run_status
            FROM rivery_runs r1
            GROUP BY river_name, river_id
            ORDER BY river_name
        """)
        pipelines = cursor.fetchall()
    conn.close()
    return {"total_pipelines": len(pipelines), "pipelines": pipelines}


# ─────────────────────────────────────────────────────────────
# 4. LATEST LOGS
# ─────────────────────────────────────────────────────────────
@app.get("/logs/latest", tags=["Logs"])
def get_latest_logs(
    from_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD")
):
    conn = get_db()
    where = "WHERE 1=1"
    params = []
    if from_date:
        where += " AND start_time >= %s"
        params.append(from_date)
    if to_date:
        where += " AND start_time <= %s"
        params.append(to_date + " 23:59:59")

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT run_id, river_name, status, start_time, end_time,
                   TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
                   error_message
            FROM rivery_runs
            {where}
            ORDER BY start_time DESC
        """, params)
        logs = cursor.fetchall()
    conn.close()
    return {"total": len(logs), "logs": logs}


# ─────────────────────────────────────────────────────────────
# 5. FAILED LOGS
# ─────────────────────────────────────────────────────────────
@app.get("/logs/failed", tags=["Logs"])
def get_failed_logs(
    from_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD")
):
    conn = get_db()
    where = "WHERE status = 'FAILED'"
    params = []
    if from_date:
        where += " AND start_time >= %s"
        params.append(from_date)
    if to_date:
        where += " AND start_time <= %s"
        params.append(to_date + " 23:59:59")

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT run_id, river_name, status, start_time, end_time,
                   TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
                   error_message
            FROM rivery_runs
            {where}
            ORDER BY start_time DESC
        """, params)
        logs = cursor.fetchall()
    conn.close()
    return {"total_failed": len(logs), "logs": logs}


# ─────────────────────────────────────────────────────────────
# 6. SUCCESS LOGS
# ─────────────────────────────────────────────────────────────
@app.get("/logs/success", tags=["Logs"])
def get_success_logs(
    from_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD")
):
    conn = get_db()
    where = "WHERE status = 'SUCCEEDED'"
    params = []
    if from_date:
        where += " AND start_time >= %s"
        params.append(from_date)
    if to_date:
        where += " AND start_time <= %s"
        params.append(to_date + " 23:59:59")

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT run_id, river_name, status, start_time, end_time,
                   TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds
            FROM rivery_runs
            {where}
            ORDER BY start_time DESC
        """, params)
        logs = cursor.fetchall()
    conn.close()
    return {"total_success": len(logs), "logs": logs}


# ─────────────────────────────────────────────────────────────
# 7. LOGS BY PIPELINE NAME
# ─────────────────────────────────────────────────────────────
@app.get("/logs/{pipeline_name}", tags=["Logs"])
def get_pipeline_logs(
    pipeline_name: str,
    from_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD")
):
    conn = get_db()
    where = "WHERE river_name = %s"
    params = [pipeline_name]
    if from_date:
        where += " AND start_time >= %s"
        params.append(from_date)
    if to_date:
        where += " AND start_time <= %s"
        params.append(to_date + " 23:59:59")

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT run_id, river_name, status, start_time, end_time,
                   TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
                   error_message
            FROM rivery_runs
            {where}
            ORDER BY start_time DESC
        """, params)
        logs = cursor.fetchall()
    conn.close()

    if not logs:
        raise HTTPException(status_code=404, detail=f"No logs found for pipeline: {pipeline_name}")

    return {"pipeline_name": pipeline_name, "total": len(logs), "logs": logs}


# ─────────────────────────────────────────────────────────────
# 8. SUMMARY
# ─────────────────────────────────────────────────────────────
@app.get("/summary", tags=["Summary"])
def get_summary(
    from_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD")
):
    conn = get_db()
    where = "WHERE 1=1"
    params = []
    if from_date:
        where += " AND start_time >= %s"
        params.append(from_date)
    if to_date:
        where += " AND start_time <= %s"
        params.append(to_date + " 23:59:59")

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as total_success,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as total_failed,
                SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as total_running,
                ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as overall_success_rate,
                ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 2) as avg_duration_seconds
            FROM rivery_runs {where}
        """, params)
        overall = cursor.fetchone()

        cursor.execute("""
            SELECT
                COUNT(*) as today_total,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as today_success,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as today_failed
            FROM rivery_runs
            WHERE DATE(start_time) = CURDATE()
        """)
        today = cursor.fetchone()

        cursor.execute(f"""
            SELECT
                river_name,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
                ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate,
                MAX(start_time) as last_run_time,
                ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 2) as avg_duration_seconds
            FROM rivery_runs {where}
            GROUP BY river_name
            ORDER BY river_name
        """, params)
        per_pipeline = cursor.fetchall()

    conn.close()
    return {"overall": overall, "today": today, "per_pipeline": per_pipeline}


# ─────────────────────────────────────────────────────────────
# 9. METRICS
# ─────────────────────────────────────────────────────────────
@app.get("/metrics", tags=["Metrics"])
def get_metrics():
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                river_name,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as total_success,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as total_failed,
                ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate,
                ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate,
                MAX(start_time) as last_run_time,
                MAX(CASE WHEN status = 'FAILED' THEN start_time END) as last_failed_time,
                MAX(CASE WHEN status = 'FAILED' THEN error_message END) as last_error_message,
                ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 2) as avg_duration_seconds,
                MAX(TIMESTAMPDIFF(SECOND, start_time, end_time)) as longest_run_seconds,
                MIN(TIMESTAMPDIFF(SECOND, start_time, end_time)) as shortest_run_seconds,
                (SELECT status FROM rivery_runs r2
                 WHERE r2.river_name = r1.river_name
                 ORDER BY start_time DESC LIMIT 1) as last_run_status
            FROM rivery_runs r1
            GROUP BY river_name
            ORDER BY river_name
        """)
        pipeline_metrics = cursor.fetchall()

        cursor.execute("""
            SELECT river_name, COUNT(*) as failed_count
            FROM rivery_runs WHERE status = 'FAILED'
            GROUP BY river_name
            ORDER BY failed_count DESC LIMIT 1
        """)
        most_failing = cursor.fetchone()

        cursor.execute("""
            SELECT river_name, COUNT(*) as total_runs
            FROM rivery_runs
            GROUP BY river_name
            ORDER BY total_runs DESC LIMIT 1
        """)
        most_active = cursor.fetchone()

        cursor.execute("""
            SELECT river_name, MAX(start_time) as last_run_time
            FROM rivery_runs
            GROUP BY river_name
            HAVING MAX(start_time) < NOW() - INTERVAL 1 HOUR
        """)
        stuck_pipelines = cursor.fetchall()

        cursor.execute("""
            SELECT
                COUNT(DISTINCT river_name) as total_pipelines,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as total_success,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as total_failed,
                ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as overall_success_rate,
                ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 2) as overall_avg_duration_seconds,
                SUM(CASE WHEN DATE(start_time) = CURDATE() THEN 1 ELSE 0 END) as total_runs_today,
                SUM(CASE WHEN DATE(start_time) = CURDATE() AND status = 'FAILED' THEN 1 ELSE 0 END) as failed_today,
                SUM(CASE WHEN DATE(start_time) = CURDATE() AND status = 'SUCCEEDED' THEN 1 ELSE 0 END) as success_today
            FROM rivery_runs
        """)
        overall = cursor.fetchone()

    conn.close()
    return {
        "overall": overall,
        "most_failing_pipeline": most_failing,
        "most_active_pipeline": most_active,
        "stuck_pipelines": stuck_pipelines,
        "per_pipeline": pipeline_metrics
    }