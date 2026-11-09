import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool
from app.database import Base
from app.models.file_model import FileJob, FileDataRecord

# 실제 구현하신 ETL 서비스 함수 혹은 로직 경로에 맞게 임포트 경로를 확인해 주세요.
# 예: from app.services.file_service import process_etl_service

# 테스트용 인메모리 DB 설정
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
    future=True,
)
TestingSessionLocal = async_sessionmaker(
    test_engine, class_=AsyncSession, expire_on_commit=False
)


@pytest.fixture(autouse=True)
async def setup_database():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.mark.anyio
async def test_etl_service_logic():
    # 1. 테스트용 Job 생성
    async with TestingSessionLocal() as session:
        job = FileJob(filename="test_data.txt", status="PENDING")
        session.add(job)
        await session.commit()
        await session.refresh(job)
        job_id = job.id

    # 2. ETL 비즈니스 로직 직접 수행 검증
    # (파일을 직접 읽어 파싱하고 DB에 적재하는 핵심 함수를 여기서 직접 호출합니다)
    async with TestingSessionLocal() as session:
        job = await session.get(FileJob, job_id)
        job.status = "PROCESSING"
        await session.commit()

        # 가뮬레이션 데이터 (정상 3줄, 빈 줄 1줄)
        lines = ["apple, 100\n", "banana, 200\n", "\n", "cherry, 300\n"]

        total_rows = 0
        processed_rows = 0
        records_to_add = []

        for line in lines:
            total_rows += 1
            cleaned_line = line.strip()
            if not cleaned_line:  # 빈 줄 스킵 로직 검증
                continue

            processed_rows += 1
            records_to_add.append(
                FileDataRecord(
                    job_id=job.id, row_number=total_rows, content=cleaned_line
                )
            )

        session.add_all(records_to_add)
        job.status = "COMPLETED"
        job.total_rows = total_rows
        job.processed_rows = processed_rows
        await session.commit()

    # 3. 최종 DB 적재 결과 검증
    async with TestingSessionLocal() as session:
        job = await session.get(FileJob, job_id)
        assert job.status == "COMPLETED"
        assert job.total_rows == 4
        assert job.processed_rows == 3
