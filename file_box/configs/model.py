from pydantic import BaseModel

from file_box.file_utils import ResamplingMapEnum


class CompressItemModel(BaseModel):
    file_type: str
    file_format: str
    compress_name: str
    width: int
    resampling: ResamplingMapEnum | None = None


class LsDataItemModel(BaseModel):
    default_metadata: dict
    moderation_choices: dict
    tags_choices: dict
    pick_of_the_week_choices: dict

class ModerationItemModel(BaseModel):
    file_type: str
    ls_data: LsDataItemModel


class FileConfigModel(BaseModel):
    compress: list[CompressItemModel]
    moderation: list[ModerationItemModel]