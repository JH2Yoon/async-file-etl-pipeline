import os
import shutil
import aiofiles
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from app.models.file_model import FileJob, FileDataRecord


async def process_etl_file(job_id: int, file_path: str):
    # 백그라운드 작업용 독립 DB 세션 생성
    async with AsyncSessionLocal() as db:
        # 1. 작업 상태를 PROCESSING으로 변경
        db_job = await db.get(FileJob, job_id)
        if not db_job:
            return

        db_job.status = "PROCESSING"
        await db.commit()

        try:
            total_rows = 0
            processed_rows = 0
            batch_size = 1000  # 1000라인씩 끊어서 벌크 인서트
            batch_records = []

            # 2. 비동기로 파일 읽기 (UTF-8 기준)
            async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
                async for line in f:
                    total_rows += 1
                    cleaned_line = line.strip()
                    if cleaned_line:
                        processed_rows += 1
                        # 배치 리스트에 추가
                        batch_records.append(
                            FileDataRecord(
                                job_id=job_id,
                                row_number=total_rows,
                                content=cleaned_line,
                            )
                        )

                    # 배치가 꽉 차면 일괄 DB 적재 (Bulk Insert)
                    if len(batch_records) >= batch_size:
                        db.add_all(batch_records)
                        await db.commit()
                        batch_records.clear()

            # 3. 남은 잔여 데이터 처리
            if batch_records:
                db.add_all(batch_records)
                await db.commit()

            # 4. 성공 시 완료 상태 및 row 수 업데이트
            db_job.status = "COMPLETED"
            db_job.total_rows = total_rows
            db_job.processed_rows = processed_rows
            await db.commit()

        except Exception as e:
            # 5. 실패 시 에러 메시지 및 FAILED 상태 업데이트
            db_job.status = "FAILED"
            db_job.error_message = str(e)
            await db.commit()

        finally:
            # 6. 파일 라이프사이클 관리 (아카이빙 및 에러 격리 전략)
            if db_job.status == "COMPLETED":
                archive_dir = "archive"
                os.makedirs(archive_dir, exist_ok=True)
                target_path = os.path.join(archive_dir, os.path.basename(file_path))
                if os.path.exists(file_path):
                    if os.path.exists(target_path):
                        os.remove(target_path)
                    shutil.move(file_path, target_path)
            elif db_job.status == "FAILED":
                pass  # 실패 시 디버깅을 위해 uploads/ 폴더에 원본 파일 유지
