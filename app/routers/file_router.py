import asyncio
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models.file_model import FileJob
from app.schemas.file_schema import FileJobResponse

router = APIRouter(prefix="/files", tags=["File ETL"])


# [실제 백그라운드 파싱 시뮬레이션 함수]
async def process_large_file(job_id: int, file_path: str):
    print(f"[Background] Job {job_id} 파일 파싱 시작...")
    await asyncio.sleep(5)  # 대용량 처리를 가정하여 5초 지연
    print(f"[Background] Job {job_id} 파싱 완료!")


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

    # 1. DB에 작업(Job) 초기 상태(PENDING) 저장
    db_job = FileJob(filename=file.filename, status="PENDING")
    db.add(db_job)
    await db.commit()
    await db.refresh(db_job)

    # 2. 백그라운드 태스크 등록
    background_tasks.add_task(process_large_file, job_id=db_job.id, file_path=file.filename)

    return db_job