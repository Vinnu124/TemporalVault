from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey, Index, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

from app.database import Base
class TemporalRecord(Base):
    __tablename__ = 'temporal_records'

    id = Column(Integer, primary_key=True)
    record_id = Column(String, nullable=False)
    version = Column(String, nullable=False)
    data = Column(JSON, nullable=False)
    timestamp = Column(
        DateTime(timezone=True),
        default=func.date_trunc('second', func.now()),  # Truncate to seconds
        nullable=False
    )
    previous_version = Column(String, nullable=True)

    __table_args__ = (
        Index('idx_record_timestamp', 'record_id', 'timestamp'),
        Index('idx_version', 'version'),
    )


class Snapshot(Base):
    __tablename__ = 'snapshots'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True),
                       default=func.now(), nullable=False)
    # Stores the complete state at this timestamp
    data = Column(JSON, nullable=False)

    __table_args__ = (
        Index('idx_snapshot_timestamp', 'timestamp'),
    )


class RollbackLog(Base):
    __tablename__ = 'rollback_logs'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True),
                       default=func.now(), nullable=False)
    rollback_data = Column(JSON, nullable=False)  # Stores rollback metadata

    __table_args__ = (
        Index('idx_rollback_timestamp', 'timestamp'),
    )
