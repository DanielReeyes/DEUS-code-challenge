from dataclasses import dataclass

import yaml


@dataclass
class GeneralConfig:
    app_name: str


@dataclass
class FilesConfig:
    csv_type: str
    parquet_type: str
    json_type: str
    input_data_folder: str
    export_data_folder: str


@dataclass
class Config:
    general: GeneralConfig
    files: FilesConfig

    @classmethod
    def load_from_yaml(cls, path: str):
        with open(path, "r") as file:
            data = yaml.safe_load(file)
        return cls(general=GeneralConfig(**data["general"]), files=FilesConfig(**data["files"]))
