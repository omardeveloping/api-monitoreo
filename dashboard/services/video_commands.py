import json
import os
import subprocess

from rest_framework.exceptions import ValidationError

FFMPEG_LARGE_PROBE_ARGS = [
    "-probesize",
    "50M",
    "-analyzeduration",
    "50M",
    "-fflags",
    "+genpts",
]


def remove_if_exists(path: str | None) -> None:
    if path and os.path.exists(path):
        os.remove(path)


def build_ffmpeg_command(
    input_path: str,
    output_path: str,
    *,
    input_args: list[str] | None = None,
    output_args: list[str] | None = None,
) -> list[str]:
    cmd = ["ffmpeg", "-y"]
    if input_args:
        cmd.extend(input_args)
    cmd.extend(["-i", input_path])
    if output_args:
        cmd.extend(output_args)
    cmd.append(output_path)
    return cmd


def _build_ffprobe_command(input_path: str, *, show_entries: str) -> list[str]:
    return [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        show_entries,
        "-of",
        "json",
        input_path,
    ]


def _command_error_text(exc: subprocess.CalledProcessError) -> str:
    return (exc.stderr or exc.stdout or str(exc)).strip()


def validation_error_message(exc: Exception) -> str:
    detail = getattr(exc, "detail", exc)
    if isinstance(detail, list):
        return " ".join(str(item) for item in detail)
    if isinstance(detail, dict):
        return " ".join(f"{key}: {value}" for key, value in detail.items())
    return str(detail)


def run_command(
    cmd: list[str],
    *,
    error_prefix: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        detalle = _command_error_text(exc)
    except OSError as exc:
        detalle = str(exc)

    if error_prefix:
        raise ValidationError(f"{error_prefix}: {detalle}")
    raise ValidationError(detalle)


def run_ffprobe_json(
    input_path: str,
    *,
    show_entries: str,
    error_prefix: str,
) -> dict:
    result = run_command(
        _build_ffprobe_command(input_path, show_entries=show_entries),
        error_prefix=error_prefix,
    )
    try:
        return json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError(f"{error_prefix}: respuesta JSON invalida.") from exc
