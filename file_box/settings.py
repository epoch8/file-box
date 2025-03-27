from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore", env_file=".env")

    host: str
    port: int = 5432
    password: str
    user: str
    dbname: str

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


class PipelineConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file=".env")

    datapipe_meta_schema: str = "public"
    document_blob_base_url: str
    create_topic: bool = False
    new_document_timedelta_days: int = 3
    document_chunk_size: int = 10
    file_config_json_path: str | None = None
    file_system_name: str


pipeline_config = PipelineConfig()  # type: ignore
db_config = PostgresSettings()  # type: ignore
