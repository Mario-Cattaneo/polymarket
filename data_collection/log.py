from datetime import datetime, timezone

def log(msg, level="INFO"):
    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    if level not in levels:
        level = "INFO"
    
    # Only log INFO or above (skip DEBUG if you want)
    if levels.index(level) >= 0:
        now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        print(f"[{now_iso}] [{level}] {msg}")
