from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional
import json
import redis
import os
from dotenv import load_dotenv

from .database import get_db
from .models import TemporalRecord, Snapshot

load_dotenv()

app = FastAPI(title="Temporal Vault API")

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

    return {"message": "Record created successfully", "version": new_version}


@app.get("/query")
async def query_record(
    record_id: str,
    timestamp: datetime = Query(..., description="Timestamp to query data at"),
    db: Session = Depends(get_db)
):
    """Query a record at a specific timestamp"""
    # Try cache first
    cache_key = f"record:{record_id}:{timestamp.isoformat()}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
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

    return record.data


@app.post("/rollback")
async def rollback_database(
    timestamp: datetime,
    db: Session = Depends(get_db)
):
    """Rollback the database to a specific timestamp"""
    # Find the closest snapshot
    snapshot = db.query(Snapshot).filter(
        Snapshot.timestamp <= timestamp
    ).order_by(Snapshot.timestamp.desc()).first()

    if not snapshot:
        raise HTTPException(
            status_code=404, detail="No snapshot found before the specified timestamp")

    # Clear cache
    redis_client.flushdb()

    return {"message": f"Database rolled back to {snapshot.timestamp}"}


@app.get("/compare")
async def compare_records(
    record_id: str,
    start_timestamp: datetime,
    end_timestamp: datetime,
    db: Session = Depends(get_db)
):
    """Compare a record between two timestamps"""
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
