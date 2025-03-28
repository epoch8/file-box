import datetime
from typing import Any, Generator, cast

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from datapipe.types import IndexDF
from loguru import logger

from file_box.catalog import FILENAME_PATTERN_RAW, IMAGE_PATTERN_COMPRESSED
from file_box.file_utils import (
    ResamplingMapEnum,
    get_image_bytes,
    get_modified_image,
    get_signed_url,
    google_details_to_status,
    merge_metadata,
    read_config_from_json,
    remove_data_by_keys,
)


def file_box_generate_image_compress_config(config_path: str) -> Generator[pd.DataFrame, Any, None]:
    compress_data = read_config_from_json(config_path=config_path, config_name="compress")

    yield pd.DataFrame(
        compress_data,
        columns=["file_type", "file_format", "resampling", "compress_name", "width"],
    )


def file_box_generate_image_moderation_config(config_path: str) -> Generator[pd.DataFrame, Any, None]:
    compress_data = read_config_from_json(config_path=config_path, config_name="moderation")

    yield pd.DataFrame(compress_data, columns=["file_type", "ls_data"])


def file_box_file_data_generate_path(
    file_data_df: pd.DataFrame,
) -> pd.DataFrame:
    res_df = file_data_df[["file_id", "file_type", "meta_data"]]
    res_df["path"] = res_df.apply(
        lambda x: FILENAME_PATTERN_RAW.format(
            file_type=x["file_type"], file_id=x["file_id"]), axis=1
        )
    return res_df


def file_box_image_compress(
    image_compress_config: pd.DataFrame,
    image_raw_df: pd.DataFrame,
    file_system_name: str,
    file_system_creds_path: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged_df = pd.merge(
        image_raw_df,
        image_compress_config,
        on="file_type",
        how="inner",
    )
    merged_df["file_bytes"] = merged_df["filepath"].apply(
        get_image_bytes,
        file_system_name=file_system_name,
        file_system_creds_path=file_system_creds_path,
    )

    compressed_records = []

    for _, row in merged_df.iterrows():
        compressed_bytes = get_modified_image(
            img=row["file_bytes"],
            resampling=ResamplingMapEnum(row["resampling"]),
            image_format=row["file_format"],
            width=row["width"],
        )

        compressed_records.append(
            {
                "file_bytes": compressed_bytes,
                "file_id": row["file_id"],
                "file_type": row["file_type"],
                "file_format": row["file_format"],
                "compress_name": row["compress_name"],
            }
        )

    image_compressed_df = pd.DataFrame(
        compressed_records,
        columns=["file_bytes", "file_id", "file_type", "file_format", "compress_name"],
    )
    image_compressed_df_without_bytes = image_compressed_df[["file_id", "file_type", "file_format", "compress_name"]]
    image_compressed_df_without_bytes["path"] = (
        image_compressed_df_without_bytes.apply(
            lambda x: IMAGE_PATTERN_COMPRESSED.format(
                file_type=x["file_type"],
                file_id=x["file_id"],
                compress_name=x["compress_name"],
                file_format=x["file_format"]
            ),
            axis=1
        )
    )
    return image_compressed_df, image_compressed_df_without_bytes


def file_box_image_filter_for_moderation(
    image_moderation_config_df: pd.DataFrame,
    image_compressed_df: pd.DataFrame,
    image_exclude_moderation_df: pd.DataFrame,
    config_path: str,
    file_system_name: str,
    file_system_creds_path: str | None = None,
) -> pd.DataFrame:
    """
    Метод для фильтрации пользовательских изображений, подлежащих модерации согласно конфигурации.

    :param image_moderation_config_df: DataFrame с конфигурацией модерации изображений пользователей.
    :param image_compressed_df: DataFrame со сжатыми изображениями пользователей.
    :param image_exclude_moderation_df: DataFrame с изображениями, не требующими модерацию.
    :param config_path: путь к JSON Config.
    :param file_system_name: название файловой системы хранения изображений.
    :param file_system_creds_path: путь к JSON-файлу для авторизации в файловой системе (опционально).
    """
    # Удаление изображений, не нуждающихся в модерации.
    image_compressed_df = remove_data_by_keys(
        image_compressed_df,
        image_exclude_moderation_df,
        keys=["file_id", "file_type"],
    )

    # Переименовываем filepath в image_gs_url для получения GS Path для Google модерации.
    image_compressed_df = image_compressed_df.rename(columns={"filepath": "file_gs_url"})

    # Получаем только сжатые изображения согласно конфигурации.
    compress_config = read_config_from_json(config_path=config_path, config_name="compress")
    compress_names = []
    for config in compress_config:
        if config["width"] == 0:
            compress_names.append(config["compress_name"])
    image_compressed_df = image_compressed_df[image_compressed_df["compress_name"].isin(compress_names)]

    # Объединяем DataFrames по image_type для добавления ls_data.
    # Используем inner join, т.к. должны попасть только записи с image_type из image_moderation_config_df.
    image_filtered_for_moderation_df = image_compressed_df.merge(
        image_moderation_config_df[["file_type", "ls_data"]],
        on="file_type",
        how="inner",
    )

    # Добавляем колонку image_url
    image_filtered_for_moderation_df["file_url"] = image_filtered_for_moderation_df["file_gs_url"].apply(
        get_signed_url,
        file_system_name=file_system_name,
        file_system_creds_path=file_system_creds_path,
    )
    image_filtered_for_moderation_df.dropna(subset=["file_url"], inplace=True)

    return image_filtered_for_moderation_df[["file_id", "file_type", "file_url", "file_gs_url", "ls_data"]]


def image_prepare_for_label_studio(
    image_filtered_for_moderation_df: pd.DataFrame,
    image_google_moderation_data_df: pd.DataFrame,
    image_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Метод для подготовки данных пользовательских изображений, подлежащих модерации, к загрузке в LabelStudio.

    :param image_filtered_for_moderation_df: DataFrame с данными изображений, подлежащих модерации.
    :param image_google_moderation_data_df: DataFrame с данными модерации изображений в Google Vision API.
    :param image_data_df: DataFrame с метаданными изображений пользователей.
    """

    # Получаем Google Review Status из Google Details.
    image_google_moderation_data_df["google_review_status"] = image_google_moderation_data_df["google_details"].apply(
        google_details_to_status
    )

    # Объединяем данные по image_id, image_type, user_id.
    image_to_moderate_ls_input_df = image_filtered_for_moderation_df.merge(
        image_google_moderation_data_df[["file_id", "file_type", "google_review_status"]],
        on=["file_id", "file_type"],
        how="left",
    ).merge(
        image_data_df[["file_id", "file_type", "meta_data"]],
        on=["file_id", "file_type"],
        how="left",
    )

    # Переименовываем metadata в raw_metadata.
    image_to_moderate_ls_input_df = image_to_moderate_ls_input_df.rename(columns={"meta_data": "raw_metadata"})

    # Формируем metadata с учетом default значений из конфигурации модерации.
    if not image_to_moderate_ls_input_df.empty:
        image_to_moderate_ls_input_df["meta_data"] = image_to_moderate_ls_input_df.apply(merge_metadata, axis=1)
    else:
        image_to_moderate_ls_input_df["meta_data"] = []

    return image_to_moderate_ls_input_df[
        ["file_id", "file_type", "meta_data", "google_review_status", "file_url", "ls_data"]
    ]


def image_output_from_label_studio(  # pylint: disable=too-many-locals
    image_to_moderate_ls_output_df: pd.DataFrame,
    datastore: DataStore,
    catalog: Catalog,
) -> pd.DataFrame:
    """
    Метод для обработки данных изображений пользователей после модерации в LabelStudio.

    :param image_to_moderate_ls_output_df: DataFrame с обработанными изображениями пользователей из LS.
    :param datastore: объект DataStore.
    :param catalog: объект каталога.
    :return: DataFrame с данными после ручной модерации в LS.
    """

    # Инициализируем массивы для данных по удаленным и отмодерированным изображениям.
    deleted_data = []
    moderation_data = []

    # Итерация по данным из LS.
    for _, row in image_to_moderate_ls_output_df.iterrows():
        annotations = row["annotations"]
        delete_flag = False
        moderation_entries = []

        for annotation in annotations:
            for result in annotation["result"]:
                from_name = result["from_name"]
                choices = result.get("value", {"choices": []}).get("choices", [])

                # Если модерация содержит удаление, то добавляем в данные для удаления.
                if from_name == "moderation" and "DELETE" in choices:
                    delete_flag = True

                # Собираем данные по модерации.
                moderation_entries.append({"choice_name": from_name, "choices": choices})

        if delete_flag:
            deleted_data.append(
                {
                    "file_id": row["file_id"],
                    "file_type": row["file_type"],
                    "last_reviewed": datetime.datetime.now(tz=datetime.timezone.utc),
                }
            )
        else:
            moderation_data.append(
                {
                    "file_id": row["file_id"],
                    "file_type": row["file_type"],
                    "last_reviewed": datetime.datetime.now(tz=datetime.timezone.utc),
                    "moderation_data": moderation_entries,
                }
            )

    image_deleted_data_df = pd.DataFrame(deleted_data, columns=["file_id", "file_type", "last_reviewed"])
    image_moderation_manual_df = pd.DataFrame(
        moderation_data, columns=["file_id", "file_type", "last_reviewed", "moderation_data"]
    )

    # Сохранение данных по удаляемым изображениям в отдельную таблицу.
    catalog.get_datatable(datastore, "file_deleted_data").store_chunk(
        cast(
            IndexDF,
            image_deleted_data_df[["file_id", "file_type", "last_reviewed"]],
        )
    )

    # Удаление всех изображений в GCS и Pipeline.
    catalog.get_datatable(datastore, "file_raw").delete_by_idx(
        idx=cast(IndexDF, image_deleted_data_df[["file_id", "file_type"]])
    )

    return image_moderation_manual_df.reset_index()[  # pylint: disable=E1136
        ["file_id", "file_type", "last_reviewed", "moderation_data"]
    ]
