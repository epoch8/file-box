from typing import Generator

import pytest
from sqlalchemy import create_engine, orm

from file_box.configs.model import CompressItemModel, FileConfigModel, ModerationItemModel
from file_box.file_utils import ResamplingMapEnum
from file_box.pipeline import datapipe_app
from file_box.service import FileBoxService, FileBoxServiceProtocol
from file_box.settings import db_config, pipeline_config


def generate_mock_config() -> FileConfigModel:
    return FileConfigModel(
        compress=[
            CompressItemModel(
                file_type="image",
                file_format="WEBP",
                compress_name="image_lanczos_webp",
                width=0,
                resampling=ResamplingMapEnum.LANCZOS
            ),
            CompressItemModel(
                file_type="image",
                file_format="WEBP",
                compress_name="image_327_lanczos_webp",
                width=327,
                resampling=ResamplingMapEnum.LANCZOS
            ),
            CompressItemModel(
                file_type="image",
                file_format="WEBP",
                compress_name="image_1000_lanczos_webp",
                width=1000,
                resampling=ResamplingMapEnum.LANCZOS
            ),
        ],
        moderation=[]
    )


@pytest.fixture(scope="session", autouse=True)
def db_session() -> Generator[orm.Session, None, None]:
    assert db_config.host == "localhost"
    engine = create_engine(db_config.dsn)
    session = orm.Session(engine)
    yield session
    session.rollback()
    session.close()
    

@pytest.fixture(scope="session", autouse=True)
def get_file_service(db_session: orm.Session) -> FileBoxServiceProtocol:
    tmp = FileBoxService(datapipe_app, pipeline_config)
    tmp.set_config(generate_mock_config())
    return tmp