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