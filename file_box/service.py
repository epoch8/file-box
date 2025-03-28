import json
import uuid
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Any, Protocol

import fsspec
import pandas as pd
import sqlalchemy as sa
from datapipe.compute import DatapipeApp, run_steps, run_steps_changelist
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import TableStoreFiledir
from datapipe.types import ChangeList
from loguru import logger

from file_box import tables
from file_box.catalog import FILENAME_PATTERN_RAW, IMAGE_PATTERN_COMPRESSED
from file_box.configs.model import FileConfigModel
from file_box.db_utils import get_sessionmaker
from file_box.file_utils import get_signed_url, is_config_exists, read_full_config_from_json
from file_box.pipeline import datapipe_app
from file_box.settings import PipelineConfig, pipeline_config

get_signed_url_30_days = partial(get_signed_url, file_system_name=pipeline_config.file_system_name, days_expiration=30)


@dataclass(kw_only=True)
class ItemDTO:
    file_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    file_type: str
    file_bytes: bytes
    meta_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self, exclude: set | None = None) -> dict[str, Any]:
        if exclude is None:
            exclude = set()
        res = {key: value for key, value in asdict(self).items() if key not in exclude}
        return res


@dataclass
class CompressInfoDTO:
    compress_name: str
    path: str

    @classmethod
    def from_table(cls, compress_data: tables.CompressData) -> "CompressInfoDTO":
        path = get_signed_url_30_days(compress_data.path)
        if not path:
            path = compress_data.path
        return cls(compress_data.compress_name, path)


@dataclass
class ResponseDTO:
    file_id: str
    source_path: str
    compress_info: list[CompressInfoDTO] | None = None
    meta_data: dict[str, Any] = field(default_factory=dict)


def generate_response(data: list[tuple[tables.FileData, tables.CompressData]]) -> ResponseDTO:
    compress_data = []
    for _, compress_item in data:
        if compress_item is None:
            continue
        compress_data.append(CompressInfoDTO.from_table(compress_item))
    file_data = data[0][0]
    path = get_signed_url_30_days(file_data.path)
    if not path:
        path = file_data.path
    return ResponseDTO(
        file_id=file_data.file_id,
        source_path=path,
        compress_info=compress_data,
        meta_data=file_data.meta_data,
    )


def get_file_by_id(file_id: str) -> ResponseDTO | None:
    stmt = (
        sa.select(tables.FileData, tables.CompressData)
        .join(tables.CompressData, tables.FileData.file_id == tables.CompressData.file_id, isouter=True)
        .where(tables.FileData.file_id == file_id)
    )
    with get_sessionmaker()() as session:
        stmt_res = session.execute(stmt).tuples().all()
    logger.info(stmt_res)
    if not stmt_res:
        return None
    res = generate_response(list(stmt_res))
    return res


def save_file_meta_data(item: ItemDTO) -> None:
    stmt = (
        sa.update(tables.FileData)
        .where(tables.FileData.file_id == item.file_id)
        .values(meta_data=item.meta_data)
        )
    with get_sessionmaker().begin() as session:
        session.execute(stmt)

class FileBoxServiceProtocol(Protocol):

    def upload_file(self, item: ItemDTO) -> ResponseDTO:
        raise NotImplementedError()

    def get_file_response(self, file_id: str) -> ResponseDTO | None:
        raise NotImplementedError()

    def get_file_bytes(self, path: str) -> bytes:
        raise NotImplementedError()

    def get_config(self) -> FileConfigModel:
        raise NotImplementedError()

    def set_config(self, config: FileConfigModel) -> None:
        raise NotImplementedError()


class FileBoxService(FileBoxServiceProtocol):
    def __init__(self, app: DatapipeApp, pipeline_config: PipelineConfig) -> None:
        self.app = app
        self.pipeline_config = pipeline_config

    def _save_data_to_filedir(self, item: ItemDTO, table_name: str) -> dict[str, Any]:
        table = self.app.ds.get_table(table_name)
        if not isinstance(table.table_store, TableStoreFiledir):
            raise ValueError("Table store is not Filedir")
        data_dict = item.to_dict()
        changes = table.store_chunk(pd.DataFrame([data_dict]))
        return {table_name: changes}
    
    def _save_file_to_store_table(self, item: ItemDTO, table_name: str) -> dict[str, Any]:
        table = self.app.ds.get_table(table_name)
        if not isinstance(table.table_store, TableStoreDB):
            raise ValueError("Table store is not DB")
        data_dict = item.to_dict(exclude={"file_bytes"})
        changes = table.store_chunk(pd.DataFrame([data_dict]))
        return {table_name: changes}

    def upload_file(self, item: ItemDTO) -> ResponseDTO:
        logger.info(f"Uploading file {item.file_id}")
        if not is_config_exists(self.pipeline_config.file_config_json_path):
            logger.warning("Config file not found, Please set config via set_config method")
            raise ValueError("Config file not found, Please set config via set_config method")
        
        changes_from_raw = self._save_data_to_filedir(item, "file_box_file_raw")
        changes_from_db = self._save_file_to_store_table(item, "file_box_file_data")
        changes = {**changes_from_raw, **changes_from_db}
        change_list = ChangeList(changes)
        run_steps_changelist(self.app.ds, self.app.steps, change_list)
        res = get_file_by_id(item.file_id)
        assert res is not None, f"File not found by id {item.file_id}"
        logger.info(f"File {item.file_id} uploaded")
        return res

    def get_file_response(self, file_id: str) -> ResponseDTO | None:
        logger.info(f"Getting file {file_id}")
        res = get_file_by_id(file_id)
        if res is None:
            logger.warning(f"File {file_id} not found")
        return res

    def get_file_bytes(self, path: str) -> bytes:
        with fsspec.open(path, "rb") as file:
            file_content = file.read()  # type: ignore
        return file_content

    def get_config(self) -> FileConfigModel:
        logger.info("Getting config")
        config = read_full_config_from_json(config_path=self.pipeline_config.file_config_json_path)
        logger.info("Config loaded")
        return FileConfigModel(**config)

    def set_config(self, config: FileConfigModel) -> None:
        logger.info("Setting config")
        with open(self.pipeline_config.file_config_json_path, "w", encoding="utf-8") as config_file:
            json.dump(config.model_dump(mode="json"), config_file, indent=4)
        run_steps(self.app.ds, self.app.steps)
        logger.info("Config set")
        
        
def get_file_box_service() -> FileBoxServiceProtocol:
    return FileBoxService(datapipe_app, pipeline_config)


def main() -> None:
    file_box_service = FileBoxService(datapipe_app, pipeline_config)
    file = open("local/test.jpeg", "rb").read()
    tmp = ItemDTO(file_bytes=file, file_type="image", meta_data={"test": "test"})
    res = file_box_service.upload_file(tmp)
    # config = open("file_box/configs/file_config.json", "r", encoding="utf-8").read()
    # file_box_service.set_config(FileConfigModel(**json.loads(config)))
    # res = file_box_service.get_file_response("a7402058-9cb3-4422-865f-1e2dacde9126")
    # res = file_box_service.get_file_bytes("/Users/nerudxlf/work/epoch8/image-box/files/user/a7402058-9cb3-4422-865f-1e2dacde9126/raw.bytes")
    # res = file_box_service.get_file_bytes("/Users/nerudxlf/work/epoch8/image-box/files/user/a7402058-9cb3-4422-865f-1e2dacde9126/user_327_lanczos_webp/image.WEBP")

    print(res)


if __name__ == "__main__":
    main()
