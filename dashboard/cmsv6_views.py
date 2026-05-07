from celery import current_app
from celery.result import AsyncResult
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST

from dashboard.tasks import (
    cmsv6_analizar_mp4_task,
    cmsv6_descargar_task,
    cmsv6_recortar_mp4_task,
    cmsv6_reparar_mp4_task,
)


def _bool_post(post, name: str, default=False) -> bool:
    if name not in post:
        return default
    return str(post.get(name, "")).lower() in {"1", "true", "yes", "on"}


def _int_post(post, name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(post.get(name, default)))
    except (TypeError, ValueError):
        return default


def _float_post(post, name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(post.get(name, default)))
    except (TypeError, ValueError):
        return default


def _download_params(post):
    modo = post.get("modo") or "dia"
    fecha_inicio_key = "fecha_inicio_auto" if modo == "auto" else "fecha_inicio"
    hora_inicio_key = "hora_inicio_auto" if modo == "auto" else "hora_inicio"
    fecha_inicio = f"{post.get(fecha_inicio_key, '').strip()} {post.get(hora_inicio_key, '00:00').strip()}".strip()
    fecha_fin = f"{post.get('fecha_fin', '').strip()} {post.get('hora_fin', '23:59').strip()}".strip()
    params = {
        "output_dir": (post.get("output_dir") or "").strip() or settings.CMSV6_OUTPUT_DIR,
        "modo": modo,
        "dia": (post.get("dia") or "").strip(),
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "intervalo_minutos": _int_post(post, "intervalo_minutos", 15, minimum=1),
        "auto_ciclos": _int_post(post, "auto_ciclos", 1, minimum=0),
        "excel_ruta": _bool_post(post, "excel_ruta", True),
        "excel_alarmas": _bool_post(post, "excel_alarmas", True),
        "videos": _bool_post(post, "videos", True),
        "testing": _bool_post(post, "testing", False),
        "test_order": post.get("test_order") or "size_asc",
        "test_limit": _int_post(post, "test_limit", 1, minimum=1),
        "test_max_mb": _float_post(post, "test_max_mb", 0, minimum=0),
        "test_channel": post.get("test_channel") or "Todos",
        "test_30d_mode": post.get("test_30d_mode") or "off",
        "test_range_mode": post.get("test_range_mode") or "off",
        "importar_django": _bool_post(post, "importar_django", True),
    }
    return params


def _task_response(task_id: str):
    task = AsyncResult(task_id)
    payload = {"task_id": task_id, "status": task.status}
    info = task.info
    if isinstance(info, dict):
        payload["info"] = info
    elif info:
        payload["info"] = {"message": str(info)}

    if task.ready():
        if task.successful():
            payload["resultado"] = task.result
        else:
            payload["error"] = str(task.result)
    return payload


@login_required
@require_GET
def cmsv6_panel(request):
    return render(
        request,
        "dashboard/cmsv6_panel.html",
        {
            "task_id": request.GET.get("task_id", ""),
            "mp4_task_id": request.GET.get("mp4_task_id", ""),
            "cmsv6_output_dir": settings.CMSV6_OUTPUT_DIR,
            "cmsv6_device_id": settings.CMSV6_DEVICE_ID,
            "cmsv6_base_url": settings.CMSV6_BASE_URL,
        },
    )


@login_required
@require_POST
def cmsv6_iniciar(request):
    task = cmsv6_descargar_task.delay(_download_params(request.POST))
    messages.success(request, f"Tarea CMSV6 encolada: {task.id}")
    return redirect(f"{reverse('cmsv6-panel')}?task_id={task.id}")


@login_required
@require_GET
def cmsv6_estado(request, task_id):
    return JsonResponse(_task_response(task_id))


@login_required
@require_POST
def cmsv6_cancelar(request, task_id):
    current_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
    messages.warning(request, f"Cancelacion solicitada para {task_id}")
    return redirect(f"{reverse('cmsv6-panel')}?task_id={task_id}")


@login_required
@require_POST
def cmsv6_mp4(request):
    ruta = (request.POST.get("ruta_mp4") or "").strip()
    output_dir = (request.POST.get("output_dir") or "").strip() or settings.CMSV6_OUTPUT_DIR
    accion = request.POST.get("accion_mp4") or "analizar"
    if accion == "reparar":
        task = cmsv6_reparar_mp4_task.delay(ruta, output_dir=output_dir)
    elif accion == "recortar":
        task = cmsv6_recortar_mp4_task.delay(ruta, output_dir=output_dir)
    else:
        task = cmsv6_analizar_mp4_task.delay(ruta, output_dir=output_dir)
    messages.success(request, f"Tarea MP4 encolada: {task.id}")
    return redirect(f"{reverse('cmsv6-panel')}?mp4_task_id={task.id}")
