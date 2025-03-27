from datapipe.compute import Catalog, Table
from datapipe.store.filedir import BytesFile, TableStoreFiledir

from file_box.settings import pipeline_config

FILENAME_PATTERN_RAW = f"{pipeline_config.document_blob_base_url}/files/{{file_type}}/{{file_id}}/raw.bytes"

IMAGE_PATTERN_COMPRESSED = (
    f"{pipeline_config.document_blob_base_url}/files/{{file_type}}/{{file_id}}/{{compress_name}}/image.{{file_format}}"
)


def get_file_catalog_dict() -> dict[str, Table]:
    return {
        "file_box_file_raw": Table(
            store=TableStoreFiledir(
                FILENAME_PATTERN_RAW,
                adapter=BytesFile(bytes_columns="file_bytes"),
                add_filepath_column=True,
                enable_rm=True,
                read_data=False,
            )
        ),
        "file_box_image_compressed": Table(
            store=TableStoreFiledir(
                IMAGE_PATTERN_COMPRESSED,
                adapter=BytesFile(bytes_columns="file_bytes"),
                add_filepath_column=True,
                enable_rm=True,
                read_data=False,
            )
        ),
    }

def get_file_catalog() -> Catalog:
    catalog = Catalog(
        get_file_catalog_dict()
    )
    return catalog