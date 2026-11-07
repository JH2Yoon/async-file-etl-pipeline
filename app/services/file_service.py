import os
import aiofiles
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from app.models.file_model import FileJob


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

            # 2. 비동기로 파일 읽기 (UTF-8 기준)
            async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
                async for line in f:
                    total_rows += 1
                    # 실제 비즈니스 데이터 가공/파싱 로직이 들어가는 자리
                    # 예시로 각 라인 공백 제거 후 처리 가정
                    cleaned_line = line.strip()
                    if cleaned_line:
                        processed_rows += 1

            # 3. 성공 시 완료 상태 및 row 수 업데이트
            db_job.status = "COMPLETED"
            db_job.total_rows = total_rows
            db_job.processed_rows = processed_rows
            await db.commit()

        except Exception as e:
            # 4. 실패 시 에러 메시지 및 FAILED 상태 업데이트
            db_job.status = "FAILED"
            db_job.error_message = str(e)
            await db.commit()

        finally:
            # 5. 처리가 끝난 임시 파일 삭제 (클린업)
            if os.path.exists(file_path):
                os.remove(file_path)