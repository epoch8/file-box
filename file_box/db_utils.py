from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from file_box.settings import db_config


def get_engine() -> Engine:
    engine = create_engine(db_config.dsn, pool_size=20)
    return engine
    
    
def get_sessionmaker() -> sessionmaker[Session]:
    engine = get_engine()
    return sessionmaker(bind=engine, autoflush=True, expire_on_commit=False)

