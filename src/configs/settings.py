from pydantic_settings import BaseSettings
from pydantic import computed_field

class Settings(BaseSettings):
    # Database
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

    @computed_field
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    # API
    api_token: str
    url_charging_points_list: str
    url_charging_points_evses_list: str
    url_locations_list: str
    url_sessions_list: str
    url_session_consumption: str
    url_charging_point_availability: str
    url_users_list: str
    url_user_group_list: str
    url_rfid_list: str
    url_authorization_list: str
    url_partners_list: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

