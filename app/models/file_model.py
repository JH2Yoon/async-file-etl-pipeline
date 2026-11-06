from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String, Text
from app.database import Base


class FileJob(Base):
    __tablename__ = "file_jobs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    status = Column(
        String(50), default="PENDING"
    )  # PENDING, PROCESSING, COMPLETED, FAILED
    total_rows = Column(Integer, default=0)
    processed_rows = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)