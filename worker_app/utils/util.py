import os
def format_bytes(bytes: int) -> str:
    """Format bytes to a human readable string."""
    if bytes < 1024:
        return f"{bytes} B"
    elif bytes < 1024 * 1024:
        return f"{bytes / 1024} KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes / (1024 * 1024)} MB"
    else:
        return f"{bytes / (1024 * 1024 * 1024)} GB"
    

def create_data_path(shared_path: str, data_id: int, data_name: str) -> str:
    """Create a data path for a given data id and name."""
    return os.path.join(shared_path, str(data_id), data_name)