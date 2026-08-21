"""Routines API routes — discover, run, schedule, and view routine results."""

from __future__ import annotations

import inspect
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from condor import routine_hooks
from condor.reports import list_reports
from condor.routine_store import get_routine_store
from condor.web.auth import get_current_user
from condor.web.models import WebUser
from config_manager import get_config_manager

log = logging.getLogger(__name__)
_TRADING_PAIRS_CACHE: dict = {}
_CONTROLLER_TYPES_CACHE: dict = {}
router = APIRouter(prefix="/routines", tags=["routines"])


# ── Request / Response Models ──


class RunRequest(BaseModel):
    config: dict = {}


class ScheduleRequest(BaseModel):
    config: dict = {}
    interval_sec: int = 300


class RunRequestV2(BaseModel):
    routine_name: str
    server_name: str
    config: dict = {}


class ScheduleRequestV2(BaseModel):
    routine_name: str
    server_name: str
    config: dict = {}
    interval_sec: int = 300


class HookTelegram(BaseModel):
    enabled: bool = False
    chat_ids: list[str] = []


class HooksRequest(BaseModel):
    telegram: HookTelegram = HookTelegram()
    trigger: str = "success"


# ── Routes ──


@router.get("")
async def list_routines(user: WebUser = Depends(get_current_user)):
    """List all discovered routines with their fields."""
    store = get_routine_store()
    return store.list_routines()


@router.get("/instances")
async def list_instances(user: WebUser = Depends(get_current_user)):
    """List all active routine instances."""
    store = get_routine_store()
    return store.list_instances()


@router.get("/instances/{instance_id}")
async def get_instance(instance_id: str, user: WebUser = Depends(get_current_user)):
    """Get instance detail including last result."""
    store = get_routine_store()
    inst = store.get_instance(instance_id)
    if not inst:
        raise HTTPException(404, "Instance not found")
    return inst


@router.get("/instances/{instance_id}/image")
async def get_instance_image(
    instance_id: str, user: WebUser = Depends(get_current_user)
):
    """Serve the chart PNG for an instance result."""
    store = get_routine_store()
    result = store.get_result(instance_id)
    if not result or not result.chart_image:
        raise HTTPException(404, "No chart image available")
    return Response(content=result.chart_image, media_type="image/png")


@router.post("/servers/{server_name}/{routine_name}/run")
async def run_routine(
    server_name: str,
    routine_name: str,
    body: RunRequest,
    user: WebUser = Depends(get_current_user),
):
    """Execute a one-shot routine. Returns instance_id for polling."""
    cm = get_config_manager()
    if not cm.has_server_access(user.id, server_name):
        raise HTTPException(status_code=403, detail="No access")
    store = get_routine_store()
    try:
        instance_id = await store.execute(
            routine_name=routine_name,
            config=body.config,
            server_name=server_name,
            user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"instance_id": instance_id}


@router.post("/servers/{server_name}/{routine_name}/schedule")
async def schedule_routine(
    server_name: str,
    routine_name: str,
    body: ScheduleRequest,
    user: WebUser = Depends(get_current_user),
):
    """Schedule a routine at an interval. Returns instance_id."""
    cm = get_config_manager()
    if not cm.has_server_access(user.id, server_name):
        raise HTTPException(status_code=403, detail="No access")
    store = get_routine_store()
    try:
        instance_id = await store.schedule(
            routine_name=routine_name,
            config=body.config,
            server_name=server_name,
            interval_sec=body.interval_sec,
            user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"instance_id": instance_id}


@router.post("/run")
async def run_routine_v2(
    body: RunRequestV2,
    user: WebUser = Depends(get_current_user),
):
    """Execute a routine (supports names with slashes like agent/routine)."""
    cm = get_config_manager()
    if not cm.has_server_access(user.id, body.server_name):
        raise HTTPException(status_code=403, detail="No access")
    store = get_routine_store()
    try:
        instance_id = await store.execute(
            routine_name=body.routine_name,
            config=body.config,
            server_name=body.server_name,
            user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"instance_id": instance_id}


@router.post("/schedule")
async def schedule_routine_v2(
    body: ScheduleRequestV2,
    user: WebUser = Depends(get_current_user),
):
    """Schedule a routine (supports names with slashes like agent/routine)."""
    cm = get_config_manager()
    if not cm.has_server_access(user.id, body.server_name):
        raise HTTPException(status_code=403, detail="No access")
    store = get_routine_store()
    try:
        instance_id = await store.schedule(
            routine_name=body.routine_name,
            config=body.config,
            server_name=body.server_name,
            interval_sec=body.interval_sec,
            user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"instance_id": instance_id}


@router.post("/instances/{instance_id}/stop")
async def stop_instance(instance_id: str, user: WebUser = Depends(get_current_user)):
    """Stop a running or scheduled instance."""
    store = get_routine_store()
    if not store.stop(instance_id):
        raise HTTPException(404, "Instance not found")
    return {"stopped": True}


@router.get("/{routine_name:path}/hooks")
async def get_hooks(routine_name: str, user: WebUser = Depends(get_current_user)):
    """Get the post-execution hook config for a routine."""
    cfg = routine_hooks.load_hooks(routine_name)
    return cfg if cfg is not None else routine_hooks._default_config()


@router.put("/{routine_name:path}/hooks")
async def put_hooks(
    routine_name: str,
    body: HooksRequest,
    user: WebUser = Depends(get_current_user),
):
    """Save the post-execution hook config for a routine."""
    return routine_hooks.save_hooks(routine_name, body.model_dump())


@router.get("/options/{source}")
async def get_field_options(
    source: str,
    server: str = Query("local", alias="server"),
    user: WebUser = Depends(get_current_user),
):
    """Return dynamic options for routine config fields (e.g. controller_configs)."""
    if source == "controller_configs":
        try:
            cm = get_config_manager()
            client = await cm.get_client(server)
            if not client:
                return {"options": []}
            configs = await client.controllers.list_controller_configs()
            names = [c.get("id") or c.get("name", "") for c in (configs or [])]
            return {"options": sorted(n for n in names if n)}
        except Exception as e:
            log.warning(f"Failed to fetch controller configs: {e}")
            return {"options": []}
    if source == "trading_pairs":
        # Distinct trading_pair values across every controller config we can
        # reach. Combines the library + live-bot configs so archived pairs
        # (e.g. old USDT-BRL misters) show up alongside currently active ones.
        #
        # Robustness: brigado's SDK sometimes hits "Server disconnected" on
        # a single call; retry up to 3× with backoff. If we still end up with
        # fewer pairs than the last known good response, return the cached
        # one so the dropdown never *shrinks* due to transient errors.
        log.info(f"trading_pairs options requested for server={server!r}")
        try:
            cm = get_config_manager()
            try:
                client = await cm.get_client(server)
            except ValueError:
                log.warning(f"server {server!r} not found; using default")
                client = await cm.get_client()
            if not client:
                log.warning("no client returned; returning []")
                return {"options": []}

            async def _list_configs():
                import asyncio as _aio
                last = None
                for i in range(3):
                    try:
                        return (await client.controllers.list_controller_configs()) or []
                    except Exception as e:
                        last = e
                        await _aio.sleep(0.3 * (i + 1))
                log.warning(f"list_controller_configs failed after retries: {last}")
                return []

            pairs: set[str] = set()
            for c in await _list_configs():
                inner = c.get("config") if isinstance(c.get("config"), dict) else c
                p = (inner or {}).get("trading_pair")
                if p:
                    pairs.add(str(p).upper())
            try:
                status = await client.bot_orchestration.get_active_bots_status()
                for bn in (status.get("data") or {}).keys():
                    try:
                        for c in (await client.controllers.get_bot_controller_configs(bn)) or []:
                            p = c.get("trading_pair")
                            if p:
                                pairs.add(str(p).upper())
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"get_active_bots_status failed: {e}")

            # Last-known-good cache: if this response is smaller than the
            # previous successful one, prefer the cached copy so a transient
            # disconnect doesn't collapse the dropdown to just active bots.
            cached = _TRADING_PAIRS_CACHE.get(server, set())
            if len(pairs) < len(cached):
                log.warning(
                    f"trading_pairs shrunk ({len(pairs)} < cached {len(cached)}); using cache"
                )
                return {"options": sorted(cached)}
            _TRADING_PAIRS_CACHE[server] = pairs
            return {"options": sorted(pairs)}
        except Exception as e:
            log.warning(f"Failed to fetch trading pairs: {e}")
            return {"options": []}
    if source == "controller_types":
        # Distinct controller_name values across every config we can see —
        # live orchestration, controller library, and archived bots.
        try:
            cm = get_config_manager()
            try:
                client = await cm.get_client(server)
            except ValueError:
                log.warning(f"server {server!r} not found; using default")
                client = await cm.get_client()
            if not client:
                return {"options": []}

            async def _list_configs():
                import asyncio as _aio
                last = None
                for i in range(3):
                    try:
                        return (await client.controllers.list_controller_configs()) or []
                    except Exception as e:
                        last = e
                        await _aio.sleep(0.3 * (i + 1))
                log.warning(f"list_controller_configs failed after retries: {last}")
                return []

            names: set[str] = set()
            for c in await _list_configs():
                inner = c.get("config") if isinstance(c.get("config"), dict) else c
                n = (inner or {}).get("controller_name")
                if n:
                    names.add(str(n))
            try:
                status = await client.bot_orchestration.get_active_bots_status()
                for bn in (status.get("data") or {}).keys():
                    try:
                        for c in (await client.controllers.get_bot_controller_configs(bn)) or []:
                            n = c.get("controller_name")
                            if n:
                                names.add(str(n))
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"get_active_bots_status failed: {e}")

            cached = _CONTROLLER_TYPES_CACHE.get(server, set())
            if len(names) < len(cached):
                log.warning(
                    f"controller_types shrunk ({len(names)} < cached {len(cached)}); using cache"
                )
                return {"options": sorted(cached)}
            _CONTROLLER_TYPES_CACHE[server] = names
            return {"options": sorted(names)}
        except Exception as e:
            log.warning(f"Failed to fetch controller types: {e}")
            return {"options": []}
    return {"options": []}


@router.get("/{routine_name:path}/source")
async def get_routine_source(
    routine_name: str,
    user: WebUser = Depends(get_current_user),
):
    """Return the source code of a routine."""
    store = get_routine_store()
    all_routines = store._discover_all()
    routine = all_routines.get(routine_name)
    if not routine:
        raise HTTPException(404, "Routine not found")
    try:
        source_file = inspect.getfile(routine.run_fn)
        source_path = Path(source_file).resolve()
        routines_dir = Path("routines").resolve()
        if not str(source_path).startswith(str(routines_dir)):
            raise HTTPException(403, "Source not available")
        source = source_path.read_text()
        return {"filename": source_path.name, "source": source}
    except (TypeError, OSError) as e:
        raise HTTPException(404, f"Source not available: {e}")


@router.get("/{routine_name:path}/reports")
async def get_routine_reports(
    routine_name: str,
    limit: int = Query(50, ge=1, le=200),
    user: WebUser = Depends(get_current_user),
):
    """Get reports generated by a specific routine."""
    # Agent routines are prefixed (e.g. "agent_slug/routine_name") but reports
    # may be saved with just the base name. Match both.
    base_name = routine_name.split("/")[-1] if "/" in routine_name else routine_name
    reports, total = list_reports(search=base_name, limit=limit)
    # Filter to exact source_name match (full prefixed or base name)
    exact = [r for r in reports if r.get("source_name") in (routine_name, base_name)]
    return {"reports": exact, "total": len(exact)}
