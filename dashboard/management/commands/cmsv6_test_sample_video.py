import datetime
import json
import urllib.parse
import urllib.request
from pathlib import Path
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from dashboard.services.cmsv6_downloader import (
    CMSV6Config,
    CMSV6Session,
    _completar_down_url_cmsv6,
    _descargar_videos_dia,
    _descarga_suficiente,
    _filtrar_videos_testing,
    _ordenar_videos_testing,
    _guardar_metadata_cmsv6_video,
    _video_channel_idx,
    _video_size_bytes,
    nombre_video_cmsv6,
)
from dashboard.services.video_commands import run_ffprobe_json


def _parse_date(raw: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(str(raw).strip())
    except ValueError as exc:
        raise CommandError("Use --date con formato YYYY-MM-DD.") from exc


def _seconds_to_hhmmss(value) -> str:
    try:
        seconds = max(0, int(value or 0)) % 86400
    except (TypeError, ValueError):
        seconds = 0
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _redact_url(url: str) -> str:
    parts = urllib.parse.urlparse(str(url or ""))
    query = []
    for key, value in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in {"jsession", "password", "pwd"}:
            value = "***"
        query.append((key, value))
    return urllib.parse.urlunparse(parts._replace(query=urllib.parse.urlencode(query)))


def _sanitize_cmsv6_entry(entry: dict) -> dict:
    result = {}
    for key, value in dict(entry or {}).items():
        if isinstance(value, str) and value.startswith(("http://", "https://", "ws://", "wss://")):
            result[key] = _redact_url(value)
        else:
            result[key] = value
    return result


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _probe_file(path: Path) -> dict | None:
    try:
        if path.suffix.lower() in {".h264", ".264"}:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-f",
                    "h264",
                    "-show_entries",
                    (
                        "format=format_name,format_long_name,duration,size,bit_rate:"
                        "stream=index,codec_name,codec_long_name,codec_type,profile,"
                        "width,height,pix_fmt,r_frame_rate,avg_frame_rate,time_base,"
                        "duration,bit_rate,nb_frames"
                    ),
                    "-of",
                    "json",
                    str(path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            return json.loads(result.stdout or "{}")
        return run_ffprobe_json(
            str(path),
            show_entries=(
                "format=format_name,format_long_name,duration,size,bit_rate:"
                "stream=index,codec_name,codec_long_name,codec_type,profile,"
                "width,height,pix_fmt,r_frame_rate,avg_frame_rate,time_base,"
                "duration,bit_rate,nb_frames"
            ),
            error_prefix="ffprobe fallo",
        )
    except Exception as exc:
        return {"error": str(exc)}


def _header_hex(path: Path, length: int = 16) -> str:
    try:
        with open(path, "rb") as fh:
            return fh.read(length).hex()
    except OSError:
        return ""


class Command(BaseCommand):
    help = (
        "Descarga una sola muestra real desde CMSV6 usando el flujo actual y genera "
        "un reporte con ffprobe + metadata CMSV6."
    )

    def add_arguments(self, parser):
        parser.add_argument("--date", help="Fecha exacta YYYY-MM-DD.")
        parser.add_argument(
            "--days",
            type=int,
            default=7,
            help="Dias hacia atras para buscar si no se usa --date. Default: 7.",
        )
        parser.add_argument(
            "--all-days",
            action="store_true",
            help=(
                "Busca dia por dia en todo el historial disponible, desde --to-date hacia "
                "atras hasta --from-date. Si no se indica --from-date, usa 2000-01-01."
            ),
        )
        parser.add_argument(
            "--from-date",
            help="Fecha minima YYYY-MM-DD para busqueda historica. Inclusiva.",
        )
        parser.add_argument(
            "--to-date",
            help="Fecha maxima YYYY-MM-DD para busqueda historica. Inclusiva. Default: hoy.",
        )
        parser.add_argument(
            "--channel",
            type=int,
            choices=(1, 2, 3, 4),
            help="Canal especifico, por ejemplo 1 para CH1.",
        )
        parser.add_argument(
            "--order",
            choices=("size_asc", "size_desc"),
            default="size_asc",
            help="Como elegir la muestra. Default: size_asc.",
        )
        parser.add_argument(
            "--max-mb",
            type=float,
            default=0.0,
            help="Tamano maximo en MB para filtrar muestras. 0 = sin limite.",
        )
        parser.add_argument(
            "--output-dir",
            help="Carpeta base para guardar la muestra. Default: BASE_DIR/local_debug/cmsv6_sample_probe",
        )
        parser.add_argument(
            "--keep-raw",
            action="store_true",
            help="Guarda el archivo crudo tal como llega desde CMSV6, sin convertirlo a MP4.",
        )

    def handle(self, *args, **options):
        output_root = self._build_output_dir(options.get("output_dir"))
        output_root.mkdir(parents=True, exist_ok=True)

        config = CMSV6Config.from_settings(str(output_root))
        config.validate()

        session = CMSV6Session(config)
        self.stdout.write("Login CMSV6...")
        session.login()
        self.stdout.write(self.style.SUCCESS("Login CMSV6 exitoso."))

        days_to_scan = self._days_to_scan(options)
        self.stdout.write(f"Dias a revisar: {len(days_to_scan)}")

        day, entry = self._find_sample(session, options, days_to_scan)
        if not entry:
            raise CommandError("No encontre una muestra descargable con esos filtros.")

        channel = _video_channel_idx(entry) + 1
        self.stdout.write(f"Muestra elegida: {day.isoformat()} CH{channel}")
        self.stdout.write(
            f"Rango CMSV6: {_seconds_to_hhmmss(entry.get('beg'))} - {_seconds_to_hhmmss(entry.get('end'))}"
        )
        self.stdout.write(f"Tamano CMSV6: {_video_size_bytes(entry) / 1048576:.2f} MB")

        day_dir = output_root / f"{config.device_id}({config.device_id})" / day.isoformat()
        keep_raw = bool(options.get("keep_raw"))
        if keep_raw:
            sample_path = self._download_raw_sample(session, config, day, entry, day_dir)
            summary = {"descargados": 1, "omitidos": 0, "errores": 0, "total": 1, "raw_mode": True}
        else:
            summary = _descargar_videos_dia(
                session,
                [entry],
                datetime.datetime.combine(day, datetime.time.min),
                output_root / f"{config.device_id}({config.device_id})",
                self.stdout.write,
                lambda *_args, **_kwargs: None,
                config,
                0,
                100,
                download_workers=1,
            )
            expected_name = nombre_video_cmsv6(entry, config.device_id, day)
            sample_path = self._locate_sample(day_dir, expected_name)
            if sample_path is None:
                raise CommandError(
                    "La descarga termino pero no pude ubicar el archivo local generado."
                )

        sidecar_path = Path(f"{sample_path}.cmsv6.json")
        sidecar = _read_json(sidecar_path) if sidecar_path.exists() else None
        probe = _probe_file(sample_path)

        original_name = str(entry.get("file", "") or "")
        original_suffix = Path(original_name).suffix.lower()
        final_suffix = sample_path.suffix.lower()

        report = {
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "output_dir": str(output_root),
            "search_days": len(days_to_scan),
            "selected_day": day.isoformat(),
            "selected_channel": channel,
            "selected_window": {
                "beg": int(entry.get("beg", 0) or 0),
                "end": int(entry.get("end", 0) or 0),
                "beg_hhmmss": _seconds_to_hhmmss(entry.get("beg")),
                "end_hhmmss": _seconds_to_hhmmss(entry.get("end")),
            },
            "download_summary": summary,
            "cmsv6_entry": _sanitize_cmsv6_entry(entry),
            "keep_raw": keep_raw,
            "source_analysis": {
                "cmsv6_file_name": original_name,
                "cmsv6_file_extension": original_suffix,
                "final_local_extension": final_suffix,
                "likely_transformed": bool(original_suffix and final_suffix and original_suffix != final_suffix),
            },
            "local_file": {
                "path": str(sample_path),
                "name": sample_path.name,
                "size_bytes": sample_path.stat().st_size,
                "header_hex": _header_hex(sample_path),
            },
            "cmsv6_sidecar": sidecar,
            "ffprobe": probe,
        }

        report_path = output_root / "cmsv6_sample_report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Archivo local: {sample_path}"))
        self.stdout.write(self.style.SUCCESS(f"Reporte: {report_path}"))
        self.stdout.write(
            "Resumen: "
            f"origen={original_suffix or '(sin extension)'} | "
            f"final={final_suffix or '(sin extension)'}"
        )

    def _build_output_dir(self, raw_output_dir: str | None) -> Path:
        if raw_output_dir:
            base = Path(raw_output_dir).expanduser()
        else:
            base = Path(settings.BASE_DIR) / "local_debug" / "cmsv6_sample_probe"
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return base / stamp

    def _days_to_scan(self, options) -> list[datetime.date]:
        if options.get("date"):
            return [_parse_date(options["date"])]
        if options.get("all_days") or options.get("from_date") or options.get("to_date"):
            today = datetime.date.today()
            end_date = _parse_date(options["to_date"]) if options.get("to_date") else today
            start_date = (
                _parse_date(options["from_date"])
                if options.get("from_date")
                else datetime.date(2000, 1, 1)
            )
            if start_date > end_date:
                start_date, end_date = end_date, start_date
            total_days = (end_date - start_date).days + 1
            return [end_date - datetime.timedelta(days=offset) for offset in range(total_days)]
        days = max(1, int(options.get("days") or 7))
        today = datetime.date.today()
        return [today - datetime.timedelta(days=offset) for offset in range(days)]

    def _find_sample(self, session: CMSV6Session, options, days):
        opts = {
            "test_channel": f"CH{options['channel']}" if options.get("channel") else "Todos",
            "test_max_mb": float(options.get("max_mb") or 0.0),
        }
        order = str(options.get("order") or "size_asc")

        for index, day in enumerate(days, start=1):
            self.stdout.write(f"[{index}/{len(days)}] Consultando {day.isoformat()}...")
            files = session.get_video_files(
                datetime.datetime.combine(day, datetime.time.min),
                log_fn=self.stdout.write,
            )
            candidates = _filtrar_videos_testing(files, opts, require_downloadable=True)
            candidates = _ordenar_videos_testing(candidates, order)
            if candidates:
                return day, candidates[0]
        return None, None

    def _locate_sample(self, day_dir: Path, expected_name: str) -> Path | None:
        direct = day_dir / expected_name
        if direct.exists():
            return direct

        stem = Path(expected_name).stem
        for candidate in sorted(day_dir.glob(f"{stem}*")):
            if candidate.is_file() and not candidate.name.endswith(".cmsv6.json"):
                return candidate
        return None

    def _download_raw_sample(
        self,
        session: CMSV6Session,
        config: CMSV6Config,
        day: datetime.date,
        entry: dict,
        day_dir: Path,
    ) -> Path:
        day_dir.mkdir(parents=True, exist_ok=True)
        source_name = Path(str(entry.get("file", "") or "")).name
        source_suffix = Path(source_name).suffix.lower() or ".raw"
        base_name = Path(nombre_video_cmsv6(entry, config.device_id, day)).stem
        raw_path = day_dir / f"{base_name}{source_suffix}"
        tmp_path = day_dir / f"{base_name}.tmp"
        if raw_path.exists():
            raw_path.unlink()
        if tmp_path.exists():
            tmp_path.unlink()

        raw_down_url = _completar_down_url_cmsv6(
            str(entry.get("DownUrl", "") or "").strip(),
            entry,
            fecha=day,
            nombre_mp4=f"{base_name}.mp4",
        )
        raw_play_url = str(entry.get("PlaybackUrl", "") or "").strip()
        down_task_url = str(entry.get("DownTaskUrl", "") or "").strip()
        file_len = _video_size_bytes(entry)
        if not (raw_down_url or raw_play_url):
            raise CommandError("La muestra elegida no tiene URL de descarga.")

        if down_task_url:
            try:
                task_url = session.refresh_url(down_task_url).replace(" ", "%20")
                request = urllib.request.Request(task_url, method="GET")
                request.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                with session.opener.open(request, timeout=20) as response:
                    payload = json.loads(response.read())
                self.stdout.write(
                    "Tarea CMSV6: "
                    f"result={payload.get('result', '?')} "
                    f"id={payload.get('taskId', payload.get('id', '?'))}"
                )
                session.wait_download_task(entry, datetime.datetime.combine(day, datetime.time.min), self.stdout.write)
            except Exception as exc:
                self.stdout.write(f"DownTaskUrl ignorada por error: {exc}")

        dispositivo_online = None
        try:
            gps = session.get_gps()
            if gps:
                dispositivo_online = gps[0].get("ol") in (1, "1", True)
        except Exception:
            pass

        candidates = []
        if dispositivo_online is False:
            if raw_play_url:
                candidates.append(("PlaybackUrl", raw_play_url, config.playback_stall_secs))
            if raw_down_url:
                candidates.append(("DownUrl", raw_down_url, config.downurl_stall_secs))
            self.stdout.write("Dispositivo OFFLINE: usando PlaybackUrl primero")
        else:
            if raw_down_url:
                candidates.append(("DownUrl", raw_down_url, config.downurl_stall_secs))
            if raw_play_url:
                candidates.append(("PlaybackUrl", raw_play_url, config.playback_stall_secs))
            if dispositivo_online:
                self.stdout.write("Dispositivo ONLINE: usando DownUrl primero")

        last_error = "sin intento"
        used_label = ""
        for label, raw_url, stall_secs in candidates:
            try:
                self.stdout.write(f"[{label}] descargando crudo...")
                session.download_file(
                    raw_url,
                    tmp_path,
                    log_fn=self.stdout.write,
                    max_retries=3,
                    dl_timeout=max(120, min(int(file_len / (100 * 1024)) if file_len else 120, 1800)),
                    stall_secs=stall_secs,
                )
                if _descarga_suficiente(tmp_path, file_len, config):
                    used_label = label
                    break
                last_error = "descarga incompleta o vacia"
                self.stdout.write(f"[{label}] {last_error}")
            except Exception as exc:
                last_error = str(exc)
                self.stdout.write(f"Error [{label}]: {exc}")

        if not used_label:
            raise CommandError(f"No se pudo descargar crudo. Ultimo error: {last_error}")

        tmp_path.rename(raw_path)
        _guardar_metadata_cmsv6_video(
            raw_path,
            entry,
            fecha=day,
            nombre_mp4=raw_path.name,
            descarga_usada=f"{used_label}:raw",
        )
        self.stdout.write(f"Archivo crudo guardado: {raw_path.name}")
        return raw_path
