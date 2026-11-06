from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

# 로컬 SQLite 비동기 연결 (프로젝트 루트에 etl.db 생성)
DATABASE_URL = "sqlite+aiosqlite:///./etl.db"

engine = create_async_engine(DATABASE_URL, echo=True, future=True)
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
    pass


# FastAPI 의존성 주입(Dependency Injection)용 함수
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session