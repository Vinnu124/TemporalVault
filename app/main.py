from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, List
import json
import redis
import os
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, make_asgi_app
from prometheus_client.core import CollectorRegistry
from sqlalchemy import text

from .database import get_db
from .models import TemporalRecord, Snapshot

load_dotenv()

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
    data: dict,
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
async def query_record(
    record_id: str,
    timestamp: datetime = Query(..., description="Timestamp to query data at"),
    db: Session = Depends(get_db)
):
    """Query a record at a specific timestamp"""
    with query_latency.labels(endpoint='query').time():
        # Try cache first
        cache_key = f"record:{record_id}:{timestamp.isoformat()}"
        cached_data = redis_client.get(cache_key)
        if cached_data:
            cache_hits.inc()
            return json.loads(cached_data)

        # Find the appropriate version
        record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id,
            TemporalRecord.timestamp <= timestamp
        ).order_by(TemporalRecord.timestamp.desc()).first()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        # Cache the result
        redis_client.setex(cache_key, 3600, json.dumps(record.data))

        # Record metrics
        record_operations.labels(operation_type='query').inc()

        return record.data


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
    start_timestamp: datetime,
    end_timestamp: datetime,
    db: Session = Depends(get_db)
):
    """Compare a record between two timestamps"""
    with query_latency.labels(endpoint='compare').time():
        start_record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id,
            TemporalRecord.timestamp <= start_timestamp
        ).order_by(TemporalRecord.timestamp.desc()).first()

        end_record = db.query(TemporalRecord).filter(
            TemporalRecord.record_id == record_id,
            TemporalRecord.timestamp <= end_timestamp
        ).order_by(TemporalRecord.timestamp.desc()).first()

        if not start_record or not end_record:
            raise HTTPException(
                status_code=404, detail="Records not found for the specified timestamps")

        # Record metrics
        record_operations.labels(operation_type='compare').inc()

        return {
            "start": start_record.data,
            "end": end_record.data,
            "changes": {
                key: {"from": start_record.data.get(
                    key), "to": end_record.data.get(key)}
                for key in set(start_record.data.keys()) | set(end_record.data.keys())
                if start_record.data.get(key) != end_record.data.get(key)
            }
        }
