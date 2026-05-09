from pathlib import Path

from django.conf import settings


MONITOR_MDVR_SEMANAL_ID_LOGICO = "cmsv6-monitor-mdvr-semanal"


def _state_path() -> Path:
    configured = getattr(settings, "CMSV6_MONITOR_TASK_ID_FILE", "")
    if configured:
        return Path(configured)
    return Path(settings.MEDIA_ROOT) / "cmsv6_monitor_mdvr_task_id.txt"


def obtener_task_id_monitor_mdvr() -> str:
    path = _state_path()
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def guardar_task_id_monitor_mdvr(task_id: str) -> None:
    task_id = (task_id or "").strip()
    if not task_id:
        return
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(task_id, encoding="utf-8")
    tmp_path.replace(path)


def limpiar_task_id_monitor_mdvr() -> None:
    try:
        _state_path().unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass
