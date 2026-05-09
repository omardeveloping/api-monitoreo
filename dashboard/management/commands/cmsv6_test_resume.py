import datetime
import hashlib
import json
import time
import urllib.parse
import urllib.request

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from dashboard.services.cmsv6_downloader import CMSV6Config, CMSV6Session


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CMSV6Client/4.0"


def _video_size_bytes(archivo):
    try:
        return int(archivo.get("len", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _video_channel(archivo):
    try:
        return int(archivo.get("chn", 0) or 0) + 1
    except (TypeError, ValueError):
        return 1


def _sha(data):
    return hashlib.sha256(data).hexdigest()[:16]


def _redact_url(url):
    parts = urllib.parse.urlparse(url)
    qs = []
    for key, value in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in {"jsession", "password", "pwd"}:
            value = "***"
        qs.append((key, value))
    return urllib.parse.urlunparse(parts._replace(query=urllib.parse.urlencode(qs)))


def _set_cmsv6_range(raw_url, offset, length):
    parts = urllib.parse.urlparse(raw_url)
    pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    found_offset = False
    found_length = False
    updated = []
    for key, value in pairs:
        upper = key.upper()
        if upper == "FOFFSET":
            value = str(offset)
            found_offset = True
        elif upper == "FLENGTH":
            value = str(length)
            found_length = True
        updated.append((key, value))
    if not found_offset:
        updated.append(("FOFFSET", str(offset)))
    if not found_length:
        updated.append(("FLENGTH", str(length)))
    return urllib.parse.urlunparse(parts._replace(query=urllib.parse.urlencode(updated)))


class Command(BaseCommand):
    help = "Prueba si CMSV6 respeta FOFFSET/FLENGTH descargando chunks de una DownUrl real."

    def add_arguments(self, parser):
        parser.add_argument("--date", help="Fecha exacta a consultar, formato YYYY-MM-DD.")
        parser.add_argument("--days", type=int, default=14, help="Dias hacia atras para buscar videos. Default: 14.")
        parser.add_argument("--chunk-mb", type=float, default=1.0, help="Tamano de cada chunk en MB. Default: 1.")
        parser.add_argument(
            "--order",
            choices=("size_asc", "size_desc"),
            default="size_asc",
            help="Orden para elegir el video encontrado. Default: size_asc.",
        )
        parser.add_argument("--channel", type=int, help="Canal especifico, por ejemplo 1 para CH1.")
        parser.add_argument(
            "--prepare-wait",
            type=int,
            default=180,
            help="Segundos a esperar despues de DownTaskUrl. Default: 180.",
        )
        parser.add_argument(
            "--no-prepare",
            action="store_true",
            help="No llama DownTaskUrl antes de probar la DownUrl.",
        )
        parser.add_argument("--timeout", type=int, default=120, help="Timeout por request en segundos. Default: 120.")

    def handle(self, *args, **options):
        chunk_size = int(float(options["chunk_mb"]) * 1024 * 1024)
        if chunk_size <= 0:
            raise CommandError("--chunk-mb debe ser mayor que 0.")

        config = CMSV6Config.from_settings()
        config.validate()

        session = CMSV6Session(config)
        self.stdout.write("Login CMSV6...")
        session.login()
        self.stdout.write(self.style.SUCCESS("Login CMSV6 exitoso."))

        day, archivo = self._find_video(session, options, chunk_size)
        if not archivo:
            raise CommandError("No encontre videos con DownUrl suficientemente grandes para probar.")

        raw_down_url = str(archivo.get("DownUrl", "") or "").strip()
        down_url = session.refresh_url(raw_down_url).replace(" ", "%20")
        file_len = _video_size_bytes(archivo)
        name = archivo.get("name") or archivo.get("file") or archivo.get("SAVENAME") or "(sin nombre)"
        self.stdout.write("")
        self.stdout.write(f"Video elegido: {day.isoformat()} CH{_video_channel(archivo)}")
        self.stdout.write(f"Nombre/origen: {name}")
        self.stdout.write(f"Tamano CMSV6: {file_len / 1048576:.2f} MB")
        self.stdout.write(f"DownUrl: {_redact_url(down_url)}")

        if not options["no_prepare"]:
            self._prepare_download(session, archivo, options["prepare_wait"])

        self.stdout.write("")
        self.stdout.write(f"Probando chunks de {chunk_size / 1048576:.2f} MB...")
        chunk_mb = chunk_size / 1048576
        chunk_a = self._download_range(session, down_url, 0, chunk_size, options["timeout"], f"chunk 0-{chunk_mb:.2f}MB")
        chunk_b = self._download_range(
            session, down_url, chunk_size, chunk_size, options["timeout"], f"chunk {chunk_mb:.2f}-{chunk_mb * 2:.2f}MB"
        )
        full = self._download_range(session, down_url, 0, chunk_size * 2, options["timeout"], f"rango 0-{chunk_mb * 2:.2f}MB")
        combined = chunk_a + chunk_b

        self.stdout.write("")
        self.stdout.write(f"chunk_a:  {len(chunk_a)} bytes sha={_sha(chunk_a)}")
        self.stdout.write(f"chunk_b:  {len(chunk_b)} bytes sha={_sha(chunk_b)}")
        self.stdout.write(f"full:     {len(full)} bytes sha={_sha(full)}")
        self.stdout.write(f"combined: {len(combined)} bytes sha={_sha(combined)}")
        self.stdout.write("")

        if combined == full and len(chunk_a) == chunk_size and len(chunk_b) == chunk_size:
            self.stdout.write(
                self.style.SUCCESS(
                    "OK: CMSV6 SI entrego bloques correctos. Se puede descargar por partes con DownUrl."
                )
            )
        elif chunk_b == chunk_a:
            self.stdout.write(
                self.style.ERROR(
                    "FALLO: el segundo chunk es igual al primero. CMSV6 ignoro FOFFSET y repitio desde el inicio."
                )
            )
        elif chunk_b == full[: len(chunk_b)]:
            self.stdout.write(
                self.style.ERROR(
                    "FALLO: el segundo chunk parece empezar desde byte 0. No es seguro appendear parciales."
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    "FALLO: los chunks no reconstruyen el rango completo. No confiar en resume sin mas validaciones."
                )
            )

        self.stdout.write("")
        self.stdout.write("Probando HTTP Range estandar sobre la misma DownUrl...")
        range_a = self._download_http_range(
            session, down_url, 0, chunk_size - 1, chunk_size, options["timeout"], f"range 0-{chunk_mb:.2f}MB"
        )
        range_b = self._download_http_range(
            session,
            down_url,
            chunk_size,
            (chunk_size * 2) - 1,
            chunk_size,
            options["timeout"],
            f"range {chunk_mb:.2f}-{chunk_mb * 2:.2f}MB",
        )
        range_resume = self._download_http_range(
            session,
            down_url,
            chunk_size,
            None,
            chunk_size,
            options["timeout"],
            f"range desde {chunk_mb:.2f}MB sin fin",
        )
        range_a_data = range_a["data"]
        range_b_data = range_b["data"]
        range_resume_data = range_resume["data"]
        self.stdout.write(f"range_a:      {len(range_a_data)} bytes sha={_sha(range_a_data)}")
        self.stdout.write(f"range_b:      {len(range_b_data)} bytes sha={_sha(range_b_data)}")
        self.stdout.write(f"range_resume: {len(range_resume_data)} bytes sha={_sha(range_resume_data)}")

        range_b_expected = full[chunk_size : chunk_size + len(range_b_data)]
        resume_expected = full[chunk_size : chunk_size + len(range_resume_data)]
        bounded_ok = range_b["status"] == 206 and range_b_data == range_b_expected
        resume_ok = range_resume["status"] == 206 and range_resume_data == resume_expected

        if bounded_ok:
            self.stdout.write(
                self.style.SUCCESS(
                    "OK: HTTP Range con inicio/fin SI devolvio bytes del offset correcto."
                )
            )
            if len(range_b_data) != chunk_size:
                self.stdout.write(
                    self.style.WARNING(
                        f"AVISO: el bloque vino de {len(range_b_data)} bytes, esperado {chunk_size}; "
                        "hay que implementar validacion de Content-Range/tamano."
                    )
                )
        elif range_b_data == range_a_data:
            self.stdout.write(self.style.ERROR("FALLO: HTTP Range tambien parece ignorado; devuelve desde byte 0."))
        else:
            self.stdout.write(self.style.ERROR("FALLO: HTTP Range con inicio/fin no coincide con el offset esperado."))

        if resume_ok:
            self.stdout.write(
                self.style.SUCCESS(
                    "OK: HTTP Range tipo resume, bytes=N-, SI entrega datos correctos para reanudar."
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    "FALLO: HTTP Range tipo resume, bytes=N-, no coincide con el offset esperado."
                )
            )

    def _find_video(self, session, options, chunk_size):
        days = self._days_to_scan(options)
        min_size = chunk_size * 2
        for index, day in enumerate(days, start=1):
            self.stdout.write(f"[{index}/{len(days)}] Consultando {day.isoformat()}...")
            files = session.get_video_files(day, log_fn=lambda msg: self.stdout.write(msg))
            candidates = []
            for archivo in files:
                down_url = str(archivo.get("DownUrl", "") or "").strip()
                if not down_url:
                    continue
                if _video_size_bytes(archivo) < min_size:
                    continue
                if options.get("channel") and _video_channel(archivo) != options["channel"]:
                    continue
                candidates.append(archivo)

            if not candidates:
                continue

            reverse = options["order"] == "size_desc"
            candidates.sort(key=_video_size_bytes, reverse=reverse)
            return day, candidates[0]
        return None, None

    def _days_to_scan(self, options):
        if options.get("date"):
            try:
                return [datetime.date.fromisoformat(options["date"])]
            except ValueError as exc:
                raise CommandError("--date debe venir en formato YYYY-MM-DD.") from exc

        today = timezone.localdate()
        total_days = max(1, int(options["days"]))
        return [today - datetime.timedelta(days=offset) for offset in range(total_days)]

    def _prepare_download(self, session, archivo, prepare_wait):
        down_task_url = str(archivo.get("DownTaskUrl", "") or "").strip()
        if not down_task_url:
            self.stdout.write("Sin DownTaskUrl; probando DownUrl directamente.")
            return

        task_url = session.refresh_url(down_task_url).replace(" ", "%20")
        self.stdout.write("")
        self.stdout.write("Llamando DownTaskUrl para preparar el archivo...")
        request = urllib.request.Request(task_url, method="GET", headers={"User-Agent": USER_AGENT})
        with session.opener.open(request, timeout=30) as response:
            raw = response.read()
        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            result = {"raw": raw[:200].decode("utf-8", errors="replace")}
        self.stdout.write(f"Respuesta DownTaskUrl: {result}")

        if prepare_wait <= 0:
            return

        waited = 0
        while waited < prepare_wait:
            step = min(10, prepare_wait - waited)
            time.sleep(step)
            waited += step
            self.stdout.write(f"Preparando... {waited}/{prepare_wait}s")

    def _download_range(self, session, down_url, offset, length, timeout, label):
        ranged_url = _set_cmsv6_range(down_url, offset, length)
        request = urllib.request.Request(ranged_url, method="GET", headers={"User-Agent": USER_AGENT})
        with session.opener.open(request, timeout=timeout) as response:
            data = response.read()
            status = response.getcode()
            content_length = response.headers.get("Content-Length", "?")
        self.stdout.write(
            f"{label}: status={status} content-length={content_length} recibidos={len(data)}"
        )
        return data

    def _download_http_range(self, session, down_url, start, end, expected_len, timeout, label):
        request = urllib.request.Request(down_url, method="GET", headers={"User-Agent": USER_AGENT})
        range_header = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
        request.add_header("Range", range_header)
        max_read = expected_len + 1
        with session.opener.open(request, timeout=timeout) as response:
            data = response.read(max_read)
            status = response.getcode()
            content_length = response.headers.get("Content-Length", "?")
            content_range = response.headers.get("Content-Range", "?")
        if len(data) > expected_len:
            data = data[:expected_len]
        self.stdout.write(
            f"{label}: status={status} content-length={content_length} "
            f"content-range={content_range} recibidos={len(data)}"
        )
        return {"data": data, "status": status, "content_length": content_length, "content_range": content_range}
