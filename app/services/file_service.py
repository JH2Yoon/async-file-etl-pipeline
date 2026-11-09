import os
import shutil
import time
import logging
import aiofiles
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from app.models.file_model import FileJob, FileDataRecord

# 구조화된 로깅 설정 (콘솔 및 파일 출력)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("ETL_Pipeline")


async def process_etl_file(job_id: int, file_path: str):
    start_time = time.time()
    logger.info(f"ETL 작업 시작 [Job ID: {job_id}] - 파일 경로: {file_path}")

    # 백그라운드 작업용 독립 DB 세션 생성
    async with AsyncSessionLocal() as db:
        # 1. 작업 상태를 PROCESSING으로 변경
        db_job = await db.get(FileJob, job_id)
        if not db_job:
            logger.error(f"Job ID {job_id}를 찾을 수 없습니다.")
            return

        db_job.status = "PROCESSING"
        await db.commit()

        total_rows = 0
        processed_rows = 0
        skipped_rows = 0
        error_details = []
        batch_size = 1000  # 1000라인씩 끊어서 벌크 인서트
        batch_records = []

        try:
            # 2. 비동기로 파일 읽기 (UTF-8 기준)
            async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
                line_num = 0  # 비동기 반복자를 위한 수동 라인 카운트
                async for line in f:
                    line_num += 1
                    total_rows += 1
                    cleaned_line = line.strip()

                    # 빈 라인 체크 (유효성 검증)
                    if not cleaned_line:
                        skipped_rows += 1
                        continue

                    try:
                        # 개별 행 비즈니스 유효성 검증 (필요시 규칙 추가 가능)
                        if "INVALID_PATTERN" in cleaned_line:
                            raise ValueError("허용되지 않는 에러 패턴 감지")

                        processed_rows += 1
                        # 배치 리스트에 추가
                        batch_records.append(
                            FileDataRecord(
                                job_id=job_id, row_number=line_num, content=cleaned_line
                            )
                        )
                    except Exception as row_err:
                        skipped_rows += 1
                        err_msg = f"Line {line_num} 파싱 실패: {str(row_err)}"
                        error_details.append(err_msg)
                        logger.warning(err_msg)
                        continue

                    # 배치가 꽉 차면 일괄 DB 적재 (Bulk Insert)
                    if len(batch_records) >= batch_size:
                        db.add_all(batch_records)
                        await db.commit()
                        logger.info(
                            f"[Job ID: {job_id}] {processed_rows}건 중간 벌크 적재 완료"
                        )
                        batch_records.clear()

            # 3. 남은 잔여 데이터 처리
            if batch_records:
                db.add_all(batch_records)
                await db.commit()

            # 4. 성공 시 완료 상태 및 row 수 업데이트
            db_job.status = "COMPLETED"
            db_job.total_rows = total_rows
            db_job.processed_rows = processed_rows
            if error_details:
                db_job.error_message = (
                    f"스킵된 라인 {skipped_rows}개 발생. 주요 사유: {error_details[:3]}"
                )
            await db.commit()

            elapsed_time = time.time() - start_time
            logger.info(
                f"ETL 작업 성공 [Job ID: {job_id}] - 소요 시간: {elapsed_time:.2f}초 (총 라인: {total_rows}, 처리: {processed_rows}, 스킵: {skipped_rows})"
            )

        except Exception as e:
            # 5. 실패 시 치명적 에러 메시지 및 FAILED 상태 업데이트
            db_job.status = "FAILED"
            db_job.error_message = str(e)
            await db.commit()
            logger.exception(
                f"ETL 작업 치명적 오류 발생 [Job ID: {job_id}] - 사유: {str(e)}"
            )

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
                    logger.info(f"파일 아카이빙 완료: {target_path}")
            elif db_job.status == "FAILED":
                logger.warning(
                    f"작업 실패로 인해 디버깅용 원본 파일 보존됨: {file_path}"
                )
                pass  # 실패 시 디버깅을 위해 uploads/ 폴더에 원본 파일 유지
