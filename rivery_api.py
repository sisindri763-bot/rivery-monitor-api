from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import pymysql
import pymysql.cursors
import os

from datetime import datetime, date
from typing import Optional

load_dotenv()

app = FastAPI(
    title="Rivery Pipeline Monitor API",
    description="API to monitor Rivery pipeline logs",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    try:
        return pymysql.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {str(e)}"
        )


@app.get("/")
def root():
    return {
        "message": "Rivery Pipeline Monitor API Running",
        "timestamp": datetime.utcnow()
    }


@app.get("/health")
def health():

    conn = None

    try:
        conn = get_db()

        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS total FROM rivery_runs"
            )
            result = cursor.fetchone()

        return {
            "status": "healthy",
            "database": "connected",
            "total_logs": result["total"]
        }

    except Exception as e:

        return {
            "status": "unhealthy",
            "error": str(e)
        }

    finally:
        if conn:
            conn.close()


@app.get("/pipelines")
def pipelines():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    river_name,
                    river_id,
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    MAX(start_time) AS last_run_time
                FROM rivery_runs
                GROUP BY river_name, river_id
                ORDER BY river_name
            """)

            rows = cursor.fetchall()

        return {
            "total_pipelines": len(rows),
            "pipelines": rows
        }

    finally:
        conn.close()


@app.get("/logs/latest")
def latest_logs(
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    limit: int = Query(100)
):

    conn = get_db()

    try:

        where = "WHERE 1=1"
        params = []

        if from_date:
            where += " AND start_time >= %s"
            params.append(from_date)

        if to_date:
            where += " AND start_time <= %s"
            params.append(f"{to_date} 23:59:59")

        sql = f"""
            SELECT
                run_id,
                river_name,
                status,
                start_time,
                end_time,
                error_message
            FROM rivery_runs
            {where}
            ORDER BY start_time DESC
            LIMIT %s
        """

        params.append(limit)

        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            logs = cursor.fetchall()

        return {
            "total": len(logs),
            "logs": logs
        }

    finally:
        conn.close()


@app.get("/logs/failed")
def failed_logs():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    run_id,
                    river_name,
                    status,
                    start_time,
                    end_time,
                    error_message
                FROM rivery_runs
                WHERE status='FAILED'
                ORDER BY start_time DESC
            """)

            logs = cursor.fetchall()

        return {
            "total_failed": len(logs),
            "logs": logs
        }

    finally:
        conn.close()


@app.get("/logs/success")
def success_logs():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    run_id,
                    river_name,
                    status,
                    start_time,
                    end_time
                FROM rivery_runs
                WHERE status='SUCCEEDED'
                ORDER BY start_time DESC
            """)

            logs = cursor.fetchall()

        return {
            "total_success": len(logs),
            "logs": logs
        }

    finally:
        conn.close()


@app.get("/logs/{pipeline_name}")
def pipeline_logs(pipeline_name: str):

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    run_id,
                    river_name,
                    status,
                    start_time,
                    end_time,
                    error_message
                FROM rivery_runs
                WHERE river_name=%s
                ORDER BY start_time DESC
            """, (pipeline_name,))

            logs = cursor.fetchall()

        if not logs:
            raise HTTPException(
                status_code=404,
                detail=f"No logs found for {pipeline_name}"
            )

        return {
            "pipeline_name": pipeline_name,
            "total": len(logs),
            "logs": logs
        }

    finally:
        conn.close()


@app.get("/summary")
def summary():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    ROUND(
                        SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END)
                        * 100.0 / COUNT(*),
                        2
                    ) AS success_rate
                FROM rivery_runs
            """)

            result = cursor.fetchone()

        return result

    finally:
        conn.close()


@app.get("/metrics")
def metrics():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    river_name,
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    ROUND(
                        SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)
                        * 100.0 / COUNT(*),
                        2
                    ) AS failure_rate
                FROM rivery_runs
                GROUP BY river_name
                ORDER BY river_name
            """)

            data = cursor.fetchall()

        return {
            "total_pipelines": len(data),
            "pipelines": data
        }

    finally:
        conn.close()


if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "rivery_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )