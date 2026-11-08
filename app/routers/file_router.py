import os
import shutil
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models.file_model import FileJob
from app.schemas.file_schema import FileJobResponse
from app.services.file_service import process_etl_file

router = APIRouter(prefix="/files", tags=["File ETL"])

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload", response_model=FileJobResponse, status_code=202)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename.endswith((".csv", ".txt", ".xml")):
        raise HTTPException(
            status_code=400,
            detail="지원하지 않는 파일 형식입니다. (CSV, TXT, XML만 허용)",
        )

    # 1. 서버 로컬에 파일 임시 저장 (동기 바이너리 쓰기로 확실하게 디스크에 기록)
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    try:
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 저장 실패: {str(e)}")

    # 2. DB에 작업(Job) 초기 상태(PENDING) 기록
    db_job = FileJob(filename=file.filename, status="PENDING")
    db.add(db_job)
    await db.commit()
    await db.refresh(db_job)

    # 3. 백그라운드 태스크 등록
    background_tasks.add_task(process_etl_file, job_id=db_job.id, file_path=file_path)

    return db_job


@router.get("/{job_id}", response_model=FileJobResponse)
async def get_job_status(job_id: int, db: AsyncSession = Depends(get_db)):
    db_job = await db.get(FileJob, job_id)
    if not db_job:
        raise HTTPException(status_code=404, detail="해당 작업을 찾을 수 없습니다.")
    return db_job
