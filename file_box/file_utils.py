import datetime
import io
import json
import os
from enum import StrEnum
from typing import Optional
from urllib.parse import urlparse

import fsspec
import pandas as pd
from PIL import Image


def read_config_from_json(config_path: str, config_name: str) -> list:
    with open(config_path, "r", encoding="utf-8") as config_file:
        config_data = json.load(config_file)
    config_list = config_data.get(config_name, [])

    return config_list


def read_full_config_from_json(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as config_file:
        config_data = json.load(config_file)

    return config_data


def is_config_exists(config_path: str) -> bool:
    return os.path.isfile(config_path)



class CraftReviewStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    APPROVED_HIDDEN = "approved_hidden"
    BLOCKED = "blocked"
    REPORTED = "reported"
    BLACKLISTED = "blacklisted"
    BLACKLISTED_HIDDEN = "blacklisted_hidden"
    DELETED = "deleted"
    SELF_DELETED = "self_deleted"


def get_image_bytes(image_url: str, file_system_name: str, file_system_creds_path: Optional[str] = None) -> Image.Image:
    if file_system_creds_path is not None:
        file_system = fsspec.filesystem(file_system_name, token=file_system_creds_path)
    else:
        file_system = fsspec.filesystem(file_system_name)

    with file_system.open(image_url, "rb") as image_file:
        image_bytes = image_file.read()
    image = Image.open(io.BytesIO(image_bytes))

    return image


def get_gs_path_from_image_url(image_url: str) -> str:
    if image_url.startswith("https://"):
        parsed_url = urlparse(image_url)
        bucket_name = parsed_url.path.split("/")[1]
        blob_name = "/".join(parsed_url.path.split("/")[2:])
        return f"gs://{bucket_name}/{blob_name}"

    return image_url



def get_signed_url(
    url: str,
    file_system_name: str,
    file_system_creds_path: Optional[str] = None,
    days_expiration: int = 365
) -> str:
    if file_system_creds_path is not None:
        file_system = fsspec.filesystem(file_system_name, token=file_system_creds_path)
    else:
        file_system = fsspec.filesystem(file_system_name)

    image_fs_path = get_gs_path_from_image_url(image_url=url)
    
    try:
        expiration = datetime.datetime.now() + datetime.timedelta(days=days_expiration)
        signed_url = file_system.sign(image_fs_path, expiration=expiration)
        return signed_url
    except (Exception,) as _:
        return ""
    
    
def merge_metadata(row: pd.Series) -> dict:
    """
    Метод для подстановки default значений в metadata изображения пользователя (нужно, чтоб не сломать LabelStudio).
    """

    # Получаем словарь default_metadata из колонки ls_data.
    default_metadata = row["ls_data"].get("default_metadata", {})

    # Получаем словарь raw_metadata
    raw_metadata = row["raw_metadata"]

    # Объединяем два словаря: значения из metadata имеют приоритет
    merged_metadata = {**default_metadata, **raw_metadata}

    return merged_metadata


def google_details_to_status(details: dict) -> str:
    """
    Метод для конвертации details из Google Vision API в статус.

    :param details: классификация изображения из Google Vision API.
    :return: статус.
    """

    if details is None:
        return CraftReviewStatus.PENDING

    if bool({details.get("adult", ""), details.get("racy", "")} & {"VERY_LIKELY"}):
        return CraftReviewStatus.BLOCKED

    return CraftReviewStatus.APPROVED


def remove_data_by_keys(initial_df: pd.DataFrame, remove_df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Метод для удаления данных из DataFrame, которые есть в другом DataFrame по заданным ключам.

    :param initial_df: DataFrame, откуда удаляем.
    :param remove_df: DataFrame, по которому удаляем.
    :param keys: ключи для обнаружения совпадений.
    """

    merged_df = pd.merge(initial_df, remove_df, on=keys, how="left", indicator=True)
    filtered_df = merged_df.loc[merged_df["_merge"] == "left_only"].drop("_merge", axis=1)

    result_df = initial_df.loc[filtered_df.index].reset_index(drop=True)
    return result_df


def save_image_to_io_bytes(img: Image.Image, image_format: str) -> bytes:
    """
    Метод сохранения PIL.Image в bytes в указанном image_format.

    :param img: PIL Image.
    :param image_format: формат изображения.
    """

    image_format = image_format.upper()
    with io.BytesIO() as output:
        img.save(output, format=image_format)
        return output.getvalue()
    
    
class ResamplingMapEnum(StrEnum):
    LANCZOS = "LANCZOS"
    BILINEAR = "BILINEAR"
    BICUBIC = "BICUBIC"
    NEAREST = "NEAREST"
    
    
RESAMPLING_MAP = {
    ResamplingMapEnum.LANCZOS: Image.Resampling.LANCZOS,
    ResamplingMapEnum.BILINEAR: Image.Resampling.BILINEAR,
    ResamplingMapEnum.BICUBIC: Image.Resampling.BICUBIC,
    ResamplingMapEnum.NEAREST: Image.Resampling.NEAREST,
}


def get_resampling_mode(name: ResamplingMapEnum) -> Image.Resampling:
    return RESAMPLING_MAP.get(name, Image.Resampling.LANCZOS)


def get_image_sizes(img: Image.Image, width: int) -> tuple[int, int]:
    original_width, original_height = img.size

    if width == 0:
        # width = 0 в конфигурации означает нет изменений размера.
        width, height = original_width, original_height
    else:
        ratio = width / original_width
        height = int(original_height * ratio)

    return width, height


def get_modified_image(img: Image.Image, resampling: ResamplingMapEnum, image_format: str, width: int) -> bytes:
    resampling_mode = get_resampling_mode(name=resampling)

    new_width, new_height = get_image_sizes(img=img, width=width)

    # Обрабатываем (ресайзим с указанным resample и сохраняем в bytes).
    modified_img = img.copy()
    modified_img = modified_img.resize((new_width, new_height), resample=resampling_mode)
    modified_img_bytes = save_image_to_io_bytes(img=modified_img, image_format=image_format)

    return modified_img_bytes
    