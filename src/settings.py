from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator


class Settings(BaseSettings):
    database_name: str = Field(default="moniepoint")
    database_user: str = Field(default="postgres")
    database_password: str = Field(default="postgres")
    database_host: str = Field(default="localhost")
    database_port: int = Field(default=5432)

    database_url: str = ""

    data_dir: str = Field(default="data")
    import_on_startup: bool = Field(default=True)
    force_import: bool = Field(default=False)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def build_database_url(self) -> "Settings":
        # Only build if not already explicitly set via APP_DATABASE_URL
        if not self.database_url:
            self.database_url = (
                f"postgresql+psycopg2://{self.database_user}:{self.database_password}"
                f"@{self.database_host}:{self.database_port}/{self.database_name}"
            )
        return self


settings = Settings()
