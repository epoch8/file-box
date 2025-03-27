import datetime
from enum import StrEnum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ImageResamplingEnum(StrEnum):
    LANCZOS = "LANCZOS"
    BILINEAR = "BILINEAR"
    BICUBIC = "BICUBIC"
    NEAREST = "NEAREST"


class ImageFormatEnum(StrEnum):
    WEBP = "WEBP"
    JPEG = "JPEG"
    PNG = "PNG"
        

class Base(DeclarativeBase):
    def __repr__(self) -> str:
        params = ", ".join(f"{k}={v}" for k, v in self.to_dict().items())
        return f"{self.__class__.__name__}({params})"

    def to_dict(self, exclude: tuple = ("_sa_adapter", "_sa_instance_state")) -> dict[str, Any]:
        return {
            key: value
            for key, value in vars(self).items()
            if not key.startswith("_") and not any(hasattr(value, arg) for arg in exclude)
        }
        

class ImageCompressConfig(Base):
    __tablename__ = "file_box_image_compress_config"
    
    file_type: Mapped[str] = mapped_column(primary_key=True)
    file_format: Mapped[ImageFormatEnum] = mapped_column(sa.String, primary_key=True)
    compress_name: Mapped[str] = mapped_column(primary_key=True)
    resampling: Mapped[ImageResamplingEnum | None] = mapped_column(sa.String)
    width: Mapped[int]


class ImageModerationConfig(Base):
    __tablename__ = "file_box_image_moderation_config"
    
    file_type: Mapped[str] = mapped_column(primary_key=True)
    ls_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    

class ImageExcludeModeration(Base):
    __tablename__ = "file_box_image_exclude_moderation"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)


class ImageFilteredForModeration(Base):
    __tablename__ = "file_box_image_filtered_for_moderation"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    file_url: Mapped[str] = mapped_column(sa.String)
    file_gs_url: Mapped[str] = mapped_column(sa.String)
    ls_data: Mapped[dict] = mapped_column(JSONB)


class ImageGoogleModerationData(Base):
    __tablename__ = "file_box_image_google_moderation_data"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    google_details: Mapped[dict | None] = mapped_column(JSONB)

class FileData(Base):
    __tablename__ = "file_box_file_data"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    meta_data: Mapped[dict] = mapped_column(JSONB)
    path: Mapped[str]
    

class CompressData(Base):
    __tablename__ = "file_box_compress_data"
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    compress_name: Mapped[str] = mapped_column(primary_key=True)
    file_format: Mapped[str]
    path: Mapped[str]
    

class ImageToModerateLsInput(Base):
    __tablename__ = "image_to_moderate_ls_input"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    meta_data: Mapped[dict] = mapped_column(JSONB)
    google_review_status: Mapped[str | None] = mapped_column(sa.String)
    file_url: Mapped[str] = mapped_column(sa.String)
    ls_data: Mapped[dict] = mapped_column(JSONB)


class ImageModerationManual(Base):
    __tablename__ = "file_box_image_moderation_manual"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    moderation_data: Mapped[dict] = mapped_column(JSONB)
    last_reviewed: Mapped[datetime.datetime] = mapped_column(sa.DateTime)


class FileDeletedData(Base):
    __tablename__ = "file_box_file_deleted_data"
    
    file_id: Mapped[str] = mapped_column(primary_key=True)
    file_type: Mapped[str] = mapped_column(primary_key=True)
    last_reviewed: Mapped[datetime.datetime]
