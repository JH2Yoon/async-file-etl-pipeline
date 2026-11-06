from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import Base, engine
from app.models.file_model import FileJob  # 모델 임포트 필수 (테이블 자동 생성을 위해)
from app.routers import file_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 시작 시 DB 테이블 생성
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # 서버 종료 시 필요한 정리 작업


# 1. FastAPI 앱 인스턴스 생성
app = FastAPI(
    title="Async File ETL Pipeline",
    description="FastAPI 기반 대용량 파일 비동기 파싱 및 ETL 백엔드 시스템",
    version="1.0.0",
    lifespan=lifespan,
)

# 2. 라우터 등록
app.include_router(file_router.router)


@app.get("/")
async def root():
    return {"message": "Async File ETL Pipeline Server is running!"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}