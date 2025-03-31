from datapipe.compute import DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from datapipe_image_moderation.pipeline import GoogleImageClassificationStep

from file_box import catalog, steps, tables
from file_box.settings import db_config, pipeline_config


def get_pipeline_steps() -> list:
    pipeline = [
        BatchGenerate(
            steps.file_box_generate_image_compress_config,
            outputs=[tables.ImageCompressConfig],
            kwargs={
                "config_path": pipeline_config.file_config_json_path,
            },
            delete_stale=True,
        ),
        BatchGenerate(
            steps.file_box_generate_image_moderation_config,
            outputs=[tables.ImageModerationConfig],
            kwargs={
                "config_path": pipeline_config.file_config_json_path,
            },
            delete_stale=True,
        ),
        BatchTransform(
            steps.file_box_file_data_generate_path, inputs=[tables.FileData], outputs=[tables.FileData], chunk_size=10
        ),
        BatchTransform(
            steps.file_box_image_compress,
            inputs=[tables.ImageCompressConfig, "file_box_file_raw"],
            outputs=["file_box_image_compressed", tables.CompressData],
            chunk_size=10,
            kwargs={
                "file_system_name": pipeline_config.file_system_name,
            },
            labels=[("stage", "image-compress")],
            transform_keys=["file_id", "file_type", "file_format", "compress_name"],
            executor_config=ExecutorConfig(
                cpu=1,
                parallelism=100,
            ),
        ),
        BatchTransform(
            steps.file_box_image_filter_for_moderation,
            inputs=[
                tables.ImageModerationConfig,
                "file_box_image_compressed",
                tables.ImageExcludeModeration,
            ],
            outputs=[tables.ImageFilteredForModeration],
            kwargs={
                "config_path": pipeline_config.file_config_json_path,
                "file_system_name": pipeline_config.file_system_name,
            },
            transform_keys=["file_id", "file_type"],
            labels=[("stage", "image-upload-to-ls")],
        ),
        GoogleImageClassificationStep(
            input="file_box_image_filtered_for_moderation",
            output="file_box_image_google_moderation_data",
            dbconn=db_config.dsn,
            file_system_name=pipeline_config.file_system_name,
            image_field="file_gs_url",
            details_field="google_details",
            step_name="file_box_image_google_moderation",
            executor_config=ExecutorConfig(
                cpu=0.1,
                memory=256 * 1024 * 1024,
                parallelism=1,
            ),
            labels=[("stage", "image-upload-to-ls")],
            create_table=False,
        ),
    ]
    return pipeline


ds = DataStore(
    meta_dbconn=DBConn(
        connstr=db_config.dsn,
        schema=pipeline_config.datapipe_meta_schema,
    )
)
datapipe_app = DatapipeApp(ds=ds, catalog=catalog.get_file_catalog(), pipeline=Pipeline(get_pipeline_steps()))
