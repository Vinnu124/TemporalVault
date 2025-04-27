from .models import TemporalRecord, Snapshot
from .database import get_db
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, List
import json
import redis
import os
import parsedatetime
from datetime import datetime
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, make_asgi_app
from prometheus_client.core import CollectorRegistry
from sqlalchemy import text
from pydantic import BaseModel
from .database import init_db
import app.models
init_db()


load_dotenv()


class RecordRequest(BaseModel):
    record_id: str
    data: dict


# Create Prometheus metrics
registry = CollectorRegistry()
metrics_app = make_asgi_app(registry=registry)

# Define metrics
record_operations = Counter(
    'temporal_vault_record_operations_total',
    'Total number of record operations',
    ['operation_type'],
    registry=registry
)

query_latency = Histogram(
    'temporal_vault_query_latency_seconds',
    'Query latency in seconds',
    ['endpoint'],
    registry=registry
)

cache_hits = Counter(
    'temporal_vault_cache_hits_total',
    'Total number of cache hits',
    registry=registry
)

app = FastAPI(title="Temporal Vault API")

# Mount Prometheus metrics endpoint
app.mount("/metrics", metrics_app)

# Redis connection for caching
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0
)


@app.post("/records")
async def create_record(
    record_id: str,
    data: str = Query(..., description="JSON-encoded data"),
    db: Session = Depends(get_db)
):
    """Create a new record version"""
    with query_latency.labels(endpoint='create_record').time():
        # Get the latest version
        latest_record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id
        ).order_by(TemporalRecord.timestamp.desc()).first()

        # Create new version
        new_version = f"v{1 if not latest_record else int(latest_record.version[1:]) + 1}"
        new_record = TemporalRecord(
            record_id=record_id,
            version=new_version,
            data=data,
            previous_version=latest_record.version if latest_record else None
        )

        db.add(new_record)
        db.commit()
        db.refresh(new_record)

        # Invalidate cache
        redis_client.delete(f"record:{record_id}")

        # Record metrics
        record_operations.labels(operation_type='create').inc()

        return {"message": "Record created successfully", "version": new_version}


@app.get("/query")
async def query_records(
    timestamp: str = Query(..., description="Timestamp to query data at (e.g., 'yesterday at 4:00 PM')"),
    db: Session = Depends(get_db)
):
    """Query all records at or before a specific timestamp with Redis caching"""
    # Parse the natural language timestamp
    cal = parsedatetime.Calendar()
    parsed_time, _ = cal.parseDT(timestamp, datetime.now())  # Parse relative to the current time
    timestamp = parsed_time.replace(microsecond=0)  # Truncate microseconds for consistency

    # Generate a cache key based on the timestamp
    cache_key = f"query:{timestamp.isoformat()}"

    # Check if the result is already in the cache
    cached_data = redis_client.get(cache_key)
    if cached_data:
        cache_hits.inc()  # Increment cache hit metric
        return json.loads(cached_data)

      

    with query_latency.labels(endpoint='query').time():
        # Query all records with a timestamp <= the provided timestamp
        records = db.query(TemporalRecord).filter(
            TemporalRecord.timestamp <= timestamp
        ).order_by(TemporalRecord.timestamp.desc()).all()

        if not records:
            raise HTTPException(status_code=404, detail="No records found for the specified timestamp")

        # Format the response
        result = [
            {
                "record_id": record.record_id,
                "version": record.version,
                "data": record.data,
                "timestamp": record.timestamp.isoformat(),
                "previous_version": record.previous_version
            }
            for record in records
        ]

        # Store the result in Redis with a 1-hour expiration time
        redis_client.setex(cache_key, 3600, json.dumps(result))

        # Record metrics
        record_operations.labels(operation_type='query').inc()

        return {"timestamp": timestamp.isoformat(), "records": result}

@app.post("/rollback")
async def rollback_database(
    timestamp: datetime,
    db: Session = Depends(get_db)
):
    """Rollback the database to a specific timestamp"""
    with query_latency.labels(endpoint='rollback').time():
        try:
            # Start transaction
            db.begin()

            # 1. Find all records that need to be rolled back
            records_to_rollback = db.query(TemporalRecord).filter(
                TemporalRecord.timestamp > timestamp
            ).all()

            if not records_to_rollback:
                return {"message": "No changes to rollback", "timestamp": timestamp}

            # 2. Create a rollback log entry
            rollback_log = {
                "timestamp": datetime.utcnow().isoformat(),
                "rollback_to": timestamp.isoformat(),
                "affected_records": len(records_to_rollback),
                "record_ids": [r.record_id for r in records_to_rollback]
            }

            # 3. Store rollback log in a separate table
            db.execute(text("""
                INSERT INTO rollback_logs (timestamp, rollback_data)
                VALUES (:timestamp, :data)
            """), {
                "timestamp": datetime.utcnow(),
                "data": json.dumps(rollback_log)
            })

            # 4. For each record, find its state at the target timestamp
            for record in records_to_rollback:
                # Find the version at the target timestamp
                target_version = db.query(TemporalRecord).filter(
                    TemporalRecord.record_id == record.record_id,
                    TemporalRecord.timestamp <= timestamp
                ).order_by(TemporalRecord.timestamp.desc()).first()

                if target_version:
                    # Update the current state to match the target version
                    db.execute(text("""
                        UPDATE temporal_records
                        SET data = :data,
                            version = :version,
                            timestamp = CURRENT_TIMESTAMP,
                            previous_version = :prev_version
                        WHERE record_id = :record_id
                        AND timestamp > :target_timestamp
                    """), {
                        "data": target_version.data,
                        "version": target_version.version,
                        "prev_version": target_version.previous_version,
                        "record_id": record.record_id,
                        "target_timestamp": timestamp
                    })
                else:
                    # If no version exists at target timestamp, delete the record
                    db.execute(text("""
                        DELETE FROM temporal_records
                        WHERE record_id = :record_id
                        AND timestamp > :target_timestamp
                    """), {
                        "record_id": record.record_id,
                        "target_timestamp": timestamp
                    })

            # 5. Clear cache for affected records
            for record in records_to_rollback:
                redis_client.delete(f"record:{record.record_id}")

            # 6. Commit the transaction
            db.commit()

            # Record metrics
            record_operations.labels(operation_type='rollback').inc()

            return {
                "message": f"Successfully rolled back to {timestamp}",
                "affected_records": len(records_to_rollback),
                "rollback_log": rollback_log
            }

        except Exception as e:
            # Rollback the transaction on error
            db.rollback()
            raise HTTPException(
                status_code=500,
                detail=f"Rollback failed: {str(e)}"
            )


@app.get("/rollback/history")
async def get_rollback_history(
    db: Session = Depends(get_db),
    limit: int = 10
):
    """Get the history of rollbacks"""
    rollbacks = db.execute(text("""
        SELECT timestamp, rollback_data
        FROM rollback_logs
        ORDER BY timestamp DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return [{
        "timestamp": row[0],
        "details": json.loads(row[1])
    } for row in rollbacks]


@app.get("/compare")
async def compare_records(
    record_id: str,
    start: Optional[datetime] = Query(None, description="Start timestamp for comparison"),
    end: Optional[datetime] = Query(None, description="End timestamp for comparison"),
    db: Session = Depends(get_db)
):
    """Compare a record between two timestamps or between its first and last occurrences in the database"""
    with query_latency.labels(endpoint='compare').time():
        # If start or end is not provided, determine the range from the database
        if not start:
            start_record = db.query(TemporalRecord).filter(
                TemporalRecord.record_id == record_id
            ).order_by(TemporalRecord.timestamp.asc()).first()
            if not start_record:
                raise HTTPException(status_code=404, detail="Record not found in the database")
            start = start_record.timestamp

        if not end:
            end_record = db.query(TemporalRecord).filter(
                TemporalRecord.record_id == record_id
            ).order_by(TemporalRecord.timestamp.desc()).first()
            if not end_record:
                raise HTTPException(status_code=404, detail="Record not found in the database")
            end = end_record.timestamp

        # Fetch the record at the start timestamp
        start_record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id,
            TemporalRecord.timestamp <= start
        ).order_by(TemporalRecord.timestamp.desc()).first()

        # Fetch the record at the end timestamp
        end_record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id,
            TemporalRecord.timestamp <= end
        ).order_by(TemporalRecord.timestamp.desc()).first()

        if not start_record or not end_record:
            raise HTTPException(
                status_code=404, detail="Records not found for the specified timestamps"
            )

        # Record metrics
        record_operations.labels(operation_type='compare').inc()

        try:
            # Attempt to parse data as JSON
            start_data = json.loads(start_record.data) if isinstance(start_record.data, str) else start_record.data
            end_data = json.loads(end_record.data) if isinstance(end_record.data, str) else end_record.data

            # If both are JSON, compare them
            changes = {
                key: {"from": start_data.get(key), "to": end_data.get(key)}
                for key in set(start_data.keys()) | set(end_data.keys())
                if start_data.get(key) != end_data.get(key)
            }

            return {
                "start": start_data,
                "end": end_data,
                "changes": changes
            }

        except (json.JSONDecodeError, TypeError):
            # If the data is not JSON, return it as plain text
            return {
                "start": start_record.data,
                "end": end_record.data,
                "changes": {
                    "from": start_record.data,
                    "to": end_record.data
                }
            }
