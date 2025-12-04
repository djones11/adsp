from typing import Any, List, Literal, Optional, Union

from pydantic import PostgresDsn, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# Populate from .env file
class Settings(BaseSettings):
    PROJECT_NAME: str = "ADSP Project"
    PROJECT_VERSION: str = "0.1.0"
    API_V1_STR: str = "/v1"

    POSTGRES_SERVER: str = "db"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: Optional[str] = None
    POSTGRES_DB: str = "adsp"
    POSTGRES_PORT: int = 5432
    DATABASE_URL: Union[str, PostgresDsn] = ""

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def assemble_db_connection(
        cls, v: Union[str, PostgresDsn, None], info: ValidationInfo
    ) -> Any:
        if isinstance(v, str) and v:
            return v

        values = info.data

        if values.get("POSTGRES_PASSWORD") is None:
            raise ValueError(
                "POSTGRES_PASSWORD must be set in your environment or .env file"
            )

        return PostgresDsn.build(
            scheme="postgresql",
            username=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            port=values.get("POSTGRES_PORT"),
            path=f"{values.get('POSTGRES_DB') or ''}",
        )

    CELERY_BROKER_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/0"
    WORKER_PORT: int = 8001
    POLL_HOUR: int = 2
    PROMETHEUS_MULTIPROC_DIR: str = "/tmp/prometheus_multiproc"
    POLICE_FORCES: List[
        Literal[
            "avon-and-somerset",
            "btp",
            "cambridgeshire",
            "cheshire",
            "city-of-london",
            "cleveland",
            "cumbria",
            "derbyshire",
            "devon-and-cornwall",
            "dorset",
            "durham",
            "essex",
            "gloucestershire",
            "hampshire",
            "hertfordshire",
            "kent",
            "lancashire",
            "leicestershire",
            "merseyside",
            "metropolitan",
            "norfolk",
            "north-wales",
            "northamptonshire",
            "northumbria",
            "nottinghamshire",
            "south-wales",
            "south-yorkshire",
            "staffordshire",
            "suffolk",
            "surrey",
            "sussex",
            "thames-valley",
            "warwickshire",
            "west-mercia",
            "west-midlands",
            "west-yorkshire",
        ]
    ] = ["metropolitan"]

    model_config = SettingsConfigDict(
        case_sensitive=True, env_file=".env", extra="ignore"
    )


settings = Settings()
