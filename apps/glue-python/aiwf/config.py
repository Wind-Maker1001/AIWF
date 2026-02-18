from dataclasses import dataclass
import os
from dotenv import load_dotenv


def _to_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Settings:
    root: str
    bus: str
    lake: str
    base_url: str
    api_key: str | None


def _repo_root_from_here() -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.normpath(os.path.join(here, "..", "..", ".."))


def load_settings() -> Settings:
    # Allow external env path; fallback to repo-relative dev.env.
    default_root = _repo_root_from_here()
    env_path = os.getenv("AIWF_ENV_PATH", os.path.join(default_root, "ops", "config", "dev.env"))
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)

    root = os.getenv("AIWF_ROOT", default_root)
    bus = os.getenv("AIWF_BUS", os.path.join(root, "bus"))
    lake = os.getenv("AIWF_LAKE", os.path.join(root, "lake"))
    base_url = os.getenv("AIWF_BASE_URL", "http://127.0.0.1:18080")
    api_key = os.getenv("AIWF_API_KEY")

    return Settings(root=root, bus=bus, lake=lake, base_url=base_url, api_key=api_key)


settings = load_settings()
