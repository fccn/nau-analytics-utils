from .utils_lib import get_iceberg_spark_session,get_delta_spark_session,get_required_env
from .config import Config

__all__ = ["get_iceberg_spark_session","get_delta_spark_session","get_required_env","Config"]
def __version__():
    return "1.0.0"

def describe():
    """Print a description of the package and its features."""
    description = (
        "Common Utils Library\n"
        "Version: {}\n"
        "Provides basic methods used across all data products including:\n"
        "  - Icerberg_spark_session\n"
        "  - Delta_spark_session\n"
        "  - Validade the Envs\n"
    ).format(__version__())
    print(description)