#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _list_files(folder: Path) -> list[Path]:
    return sorted([p for p in folder.iterdir() if p.is_file()])


def _prepend_ffmpeg_dir(ffmpeg_dir: str) -> None:
    if not ffmpeg_dir:
        return
    path_value = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{ffmpeg_dir}{os.pathsep}{path_value}" if path_value else ffmpeg_dir


def _ensure_binaries() -> bool:
    ffmpeg_bin = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if ffmpeg_bin and ffprobe_bin:
        return True
    print(
        "Missing ffmpeg/ffprobe in PATH.\n"
        "Install FFmpeg or run with --ffmpeg-dir \"C:\\path\\to\\ffmpeg\\bin\".",
        file=sys.stderr,
    )
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert the only video in test/input to MP4 using project code."
    )
    parser.add_argument(
        "--file",
        default="",
        help="Direct path to a single input video file (.h264, .grec, .mp4).",
    )
    parser.add_argument(
        "--input-dir",
        default="test/input",
        help="Folder containing exactly one video file.",
    )
    parser.add_argument(
        "--output-dir",
        default="test/output",
        help="Folder where final MP4 will be copied.",
    )
    parser.add_argument(
        "--ffmpeg-dir",
        default="",
        help="Optional folder containing ffmpeg/ffprobe binaries (bin directory).",
    )
    args = parser.parse_args()

    root = _repo_root()
    sys.path.insert(0, str(root))
    _prepend_ffmpeg_dir(args.ffmpeg_dir)
    if not _ensure_binaries():
        return 1

    output_dir = (root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.file:
        source = Path(args.file)
        if not source.is_absolute():
            source = (root / source).resolve()
        if not source.exists() or not source.is_file():
            print(f"Input file not found: {source}", file=sys.stderr)
            return 1
    else:
        input_dir = (root / args.input_dir).resolve()
        if not input_dir.exists():
            input_dir.mkdir(parents=True, exist_ok=True)
            print(
                f"Input dir created: {input_dir}\n"
                "Put exactly one file there and run again, "
                "or use --file <path>.",
                file=sys.stderr,
            )
            return 1
        if not input_dir.is_dir():
            print(f"Input path is not a directory: {input_dir}", file=sys.stderr)
            return 1

        files = _list_files(input_dir)
        if len(files) != 1:
            print(
                f"Expected exactly 1 file in {input_dir}, found {len(files)}.",
                file=sys.stderr,
            )
            for f in files:
                print(f"- {f.name}", file=sys.stderr)
            return 1

        source = files[0]

    from dashboard.services.calcular_duracion_video import (
        asegurar_mp4_compatible,
        envolver_h264_en_mp4,
    )

    ext = source.suffix.lower()
    if ext in {".h264", ".grec"}:
        mp4_path = Path(envolver_h264_en_mp4(str(source)))
    elif ext == ".mp4":
        asegurar_mp4_compatible(str(source))
        mp4_path = source
    else:
        print(
            f"Unsupported file extension: {source.name}. Use .h264, .grec, or .mp4",
            file=sys.stderr,
        )
        return 1

    target = output_dir / f"{mp4_path.stem}.mp4"
    shutil.copy2(mp4_path, target)
    print(str(target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
