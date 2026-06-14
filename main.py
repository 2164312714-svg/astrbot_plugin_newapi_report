"""New-API 每小时监控报表 · AstrBot 插件版"""
import asyncio
import hashlib
import ipaddress
import json
import os
import random
import time
from pathlib import Path

import aiohttp
import psutil

from astrbot.api.all import AstrBotConfig, Context, Star, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import StarTools

DEFAULT_BACKGROUND_URL = "https://www.loliapi.com/acg/pc/"
LOLICON_API_URL = "https://api.lolicon.app/setu/v2"
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
SYNC_OVERLAP_SECONDS = 300


def fmt_tok(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return str(v)


def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_timestamp(value):
    timestamp = to_int(value)
    return timestamp // 1000 if timestamp > 1_000_000_000_000 else timestamp


def json_for_script(value):
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


class NewApiHourlyReport(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._migrate_config_sections()
        self._load_config_values()
        self._task = None
        self._sync_task = None
        self._running = False
        self._generating = False
        self._bg_cache_path = None
        self._background_queue = asyncio.Queue()
        self._background_preload_task = None
        self._background_cache_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self._sync_lock = asyncio.Lock()
        self._data_dir = StarTools.get_data_dir()

    def _cfg(self, section: str, key: str, default=None, legacy_keys=()):
        section_cfg = self.config.get(section, {})
        if isinstance(section_cfg, dict) and key in section_cfg:
            return section_cfg.get(key)
        for legacy_key in legacy_keys:
            value = self.config.get(legacy_key)
            if value is not None:
                return value
        return default

    def _migrate_config_sections(self):
        """将旧版平铺配置迁移到分组配置，避免配置页继续堆成一长列。"""
        if not hasattr(self.config, "get") or not hasattr(self.config, "__setitem__"):
            return
        layout = {
            "connection": {
                "newapi_url": ("newapi_url", ""),
                "newapi_token": ("newapi_token", ""),
                "newapi_user_id": ("newapi_user_id", ""),
            },
            "delivery": {
                "group_ids": ("group_ids", ""),
                "enable_cron": ("enable_cron", True),
                "cron_minute": ("cron_minute", 5),
            },
            "report": {
                "time_range_hours": ("time_range_hours", 24),
                "show_ip_addresses": ("show_ip_addresses", True),
                "show_ipv4_addresses": ("show_ipv4_addresses", True),
                "show_ipv6_addresses": ("show_ipv6_addresses", True),
            },
            "background": {
                "bg_provider": ("background_provider", "loli"),
                "bg_fallback_chain": (
                    "background_fallback_chain",
                    "loli&lolicon,local",
                ),
                "bg_custom_url": ("background_url", DEFAULT_BACKGROUND_URL),
                "bg_local_path": ("background_local_path", ""),
                "bg_preload_count": ("background_preload_count", 1),
                "bg_req_timeout": ("background_request_timeout", 10),
                "bg_proxy": ("background_proxy", ""),
                "bg_lolicon_r18_type": ("background_lolicon_r18_type", 0),
            },
            "advanced": {
                "browser_executable": ("browser_executable", ""),
            },
        }
        changed = False
        advanced_cfg = self.config.get("advanced")
        migration_done = (
            isinstance(advanced_cfg, dict)
            and to_int(advanced_cfg.get("config_version"), 0) >= 2
        )
        for section_name, fields in layout.items():
            section_cfg = self.config.get(section_name)
            if not isinstance(section_cfg, dict):
                section_cfg = {}
                self.config[section_name] = section_cfg
                changed = True
            for new_key, (legacy_key, default) in fields.items():
                if migration_done:
                    continue
                legacy_value = self.config.get(legacy_key)
                if legacy_value is not None:
                    current = section_cfg.get(new_key)
                    if (
                        (new_key not in section_cfg or current == default)
                        and current != legacy_value
                    ):
                        section_cfg[new_key] = legacy_value
                        changed = True
        # 兼容更早版本使用过的别名。
        delivery_cfg = self.config.get("delivery")
        if isinstance(delivery_cfg, dict) and not migration_done:
            legacy_group_id = str(self.config.get("group_id") or "").strip()
            if legacy_group_id and not str(delivery_cfg.get("group_ids") or "").strip():
                delivery_cfg["group_ids"] = legacy_group_id
                changed = True
        bg_cfg = self.config.get("background")
        if isinstance(bg_cfg, dict) and not migration_done:
            legacy_r18 = to_int(self.config.get("bg_lolicon_r18_type"), 0)
            current_r18 = to_int(bg_cfg.get("bg_lolicon_r18_type"), 0)
            if legacy_r18 != current_r18 and current_r18 == 0:
                bg_cfg["bg_lolicon_r18_type"] = legacy_r18
                changed = True
        advanced_cfg = self.config.get("advanced")
        if isinstance(advanced_cfg, dict) and not migration_done:
            advanced_cfg["config_version"] = 2
            changed = True
        if changed and hasattr(self.config, "save_config"):
            try:
                self.config.save_config()
            except Exception as e:
                logger.warning(f"保存分组配置失败: {e}")

    def _load_config_values(self):
        self.newapi_url = str(
            self._cfg("connection", "newapi_url", "", ("newapi_url",)) or ""
        ).strip().rstrip("/")
        self.newapi_token = str(
            self._cfg("connection", "newapi_token", "", ("newapi_token",)) or ""
        ).strip()
        if self.newapi_token.lower().startswith("bearer "):
            self.newapi_token = self.newapi_token[7:].strip()
        self.newapi_user_id = str(
            self._cfg("connection", "newapi_user_id", "", ("newapi_user_id",)) or ""
        )
        group_ids_raw = str(
            self._cfg("delivery", "group_ids", "", ("group_ids", "group_id")) or ""
        )
        self.group_ids = [g.strip() for g in group_ids_raw.split(",") if g.strip()]
        self.cron_minute = max(
            0,
            min(
                59,
                to_int(self._cfg("delivery", "cron_minute", 5, ("cron_minute",)), 5),
            ),
        )
        self.enable_cron = bool(
            self._cfg("delivery", "enable_cron", True, ("enable_cron",))
        )
        self.time_range_hours = max(
            1,
            min(
                48,
                to_int(
                    self._cfg(
                        "report", "time_range_hours", 24, ("time_range_hours",)
                    ),
                    24,
                ),
            ),
        )
        self.show_ip_addresses = bool(
            self._cfg(
                "report", "show_ip_addresses", True, ("show_ip_addresses",)
            )
        )
        self.show_ipv4_addresses = bool(
            self._cfg(
                "report", "show_ipv4_addresses", True, ("show_ipv4_addresses",)
            )
        )
        self.show_ipv6_addresses = bool(
            self._cfg(
                "report", "show_ipv6_addresses", True, ("show_ipv6_addresses",)
            )
        )
        self.browser_executable = str(
            self._cfg(
                "advanced", "browser_executable", "", ("browser_executable",)
            )
            or ""
        ).strip()
        self.background_url = str(
            self._cfg(
                "background",
                "bg_custom_url",
                DEFAULT_BACKGROUND_URL,
                ("background_url",),
            )
            or ""
        ).strip()
        configured_provider = str(
            self._cfg(
                "background", "bg_provider", "", ("background_provider",)
            )
            or ""
        ).strip()
        self.background_provider = (
            configured_provider
            or (
                "custom_url"
                if self.background_url and self.background_url != DEFAULT_BACKGROUND_URL
                else "loli"
            )
        ).lower()
        self.background_fallback_chain = str(
            self._cfg(
                "background",
                "bg_fallback_chain",
                "loli&lolicon,local",
                ("background_fallback_chain",),
            )
            or ""
        ).strip()
        self.background_local_path = str(
            self._cfg(
                "background", "bg_local_path", "", ("background_local_path",)
            )
            or ""
        ).strip()
        self.background_request_timeout = max(
            5,
            min(
                60,
                to_int(
                    self._cfg(
                        "background",
                        "bg_req_timeout",
                        10,
                        ("background_request_timeout",),
                    ),
                    10,
                ),
            ),
        )
        self.background_preload_count = max(
            1,
            min(
                10,
                to_int(
                    self._cfg(
                        "background",
                        "bg_preload_count",
                        1,
                        ("background_preload_count", "bg_preload_count"),
                    ),
                    1,
                ),
            ),
        )
        self.background_proxy = str(
            self._cfg(
                "background",
                "bg_proxy",
                "",
                ("background_proxy", "bg_proxy"),
            )
            or ""
        ).strip()
        self.background_lolicon_r18_type = max(
            0,
            min(
                2,
                to_int(
                    self._cfg(
                        "background",
                        "bg_lolicon_r18_type",
                        0,
                        ("background_lolicon_r18_type", "bg_lolicon_r18_type"),
                    ),
                    0,
                ),
            ),
        )
        self.background_fit_mode = str(
            self._cfg("background", "bg_fit_mode", "repeat") or "repeat"
        ).strip().lower()
        if self.background_fit_mode not in {"repeat", "cover", "contain"}:
            self.background_fit_mode = "repeat"
        self.background_position = str(
            self._cfg("background", "bg_position", "top") or "top"
        ).strip().lower()
        if self.background_position not in {"top", "center", "bottom"}:
            self.background_position = "top"
        delivery_cfg = self.config.get("delivery")
        if (
            isinstance(delivery_cfg, dict)
            and delivery_cfg.get("cron_minute") != self.cron_minute
        ):
            delivery_cfg["cron_minute"] = self.cron_minute
            if hasattr(self.config, "save_config"):
                try:
                    self.config.save_config()
                except Exception as e:
                    logger.warning(f"保存规范化配置失败: {e}")

    async def initialize(self) -> None:
        if not self.newapi_url or not self.newapi_token:
            logger.warning("New-API 报表：未配置 newapi_url 或系统访问令牌，插件功能受限")
            return
        # 检查 playwright 是否可用
        try:
            from playwright.async_api import async_playwright  # noqa: F401
        except ImportError:
            logger.error(
                "playwright 未安装，请执行：pip install playwright && playwright install chromium"
            )
            self.enable_cron = False
            return
        # 启动时准备一张背景图；之后每次生成报表都会刷新。
        if self._background_enabled():
            await self._cache_background()
            self._ensure_background_preload()
        # 启动1分钟增量同步
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        if self.enable_cron:
            self._task = asyncio.create_task(self._cron_loop())
            logger.info(f"New-API 定时报表已启动，每小时第 {self.cron_minute} 分钟执行，目标群: {self.group_ids}")
        logger.info("New-API 数据同步已启动，每1分钟增量拉取")

    async def terminate(self) -> None:
        self._running = False
        for t in [self._task, self._sync_task, self._background_preload_task]:
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    # ────────── 1分钟增量同步 ──────────

    async def _sync_loop(self):
        """每1分钟从 New-API 增量拉取日志，存入本地缓存"""
        # 仅在没有缓存时执行全量拉取，正常重启从缓存截止时间继续同步。
        try:
            cache_file = self._data_dir / "log_cache.json"
            await self._sync_incremental(full=not cache_file.exists())
        except Exception as e:
            logger.error(f"首次全量同步失败: {e}")
        while self._running:
            await asyncio.sleep(60)
            if not self._running:
                break
            try:
                await self._sync_incremental(full=False)
            except Exception as e:
                logger.error(f"增量同步失败: {e}")

    async def _sync_incremental(self, full=False):
        """增量拉取日志并更新本地缓存；仅完整拉取成功后推进同步游标。"""
        data_dir = self._data_dir
        cache_file = data_dir / "log_cache.json"
        temp_file = data_dir / "log_cache.tmp"
        now_ts = int(time.time())

        async with self._sync_lock:
            cached_logs = []
            cache_end_ts = 0
            if cache_file.exists():
                try:
                    async with self._cache_lock:
                        cache_data = await asyncio.to_thread(
                            self._read_cache_file, cache_file
                        )
                    cached_logs = cache_data.get("logs", [])
                    cache_end_ts = to_int(cache_data.get("end_ts"))
                    if not isinstance(cached_logs, list):
                        raise ValueError("logs 不是数组")
                except Exception as e:
                    raise RuntimeError(
                        f"读取现有缓存失败，为避免覆盖历史数据已停止同步: {e}"
                    ) from e

            if full or cache_end_ts == 0:
                # 全量：拉最近48小时（按30分钟段采样）
                fetch_start = now_ts - 48 * 3600
            else:
                # 从上次成功截止点回退5分钟，覆盖迟到日志和分页边界变化。
                fetch_start = max(0, min(cache_end_ts, now_ts) - SYNC_OVERLAP_SECONDS)

            if fetch_start >= now_ts:
                return  # 无需拉取

            new_logs = await self._fetch_logs_from_api(fetch_start, now_ts)

            cutoff = now_ts - 48 * 3600
            # fetch_start 之后由本轮完整结果整体替换，既覆盖迟到日志，
            # 又不会把没有请求 ID 的真实重复请求误判为同一条。
            all_logs = []
            for log in cached_logs:
                if not isinstance(log, dict):
                    continue
                created_at = normalize_timestamp(log.get("created_at"))
                if created_at < fetch_start:
                    log = dict(log)
                    log["created_at"] = created_at
                    all_logs.append(log)
            all_logs.extend(new_logs)
            normalized_logs = []
            seen_ids = set()
            for log in all_logs:
                if not isinstance(log, dict):
                    continue
                log = dict(log)
                created_at = normalize_timestamp(log.get("created_at"))
                log["created_at"] = created_at
                if created_at >= cutoff and to_int(log.get("type"), 2) == 2:
                    request_id = log.get("id") or log.get("request_id")
                    if request_id:
                        request_key = str(request_id)
                        if request_key in seen_ids:
                            continue
                        seen_ids.add(request_key)
                    normalized_logs.append(log)

            try:
                async with self._cache_lock:
                    await asyncio.to_thread(
                        self._write_cache_file,
                        temp_file,
                        cache_file,
                        {"logs": normalized_logs, "end_ts": now_ts},
                    )
                if full:
                    logger.info(f"全量同步完成: {len(normalized_logs)} 条")
                else:
                    logger.info(
                        f"完整增量同步: 区间 {time.strftime('%H:%M:%S', time.localtime(fetch_start))}"
                        f"~{time.strftime('%H:%M:%S', time.localtime(now_ts))}, "
                        f"拉取 {len(new_logs)} 条, 缓存共 {len(normalized_logs)} 条"
                    )
            except Exception as e:
                try:
                    temp_file.unlink(missing_ok=True)
                except OSError:
                    pass
                logger.warning(f"写入缓存失败: {e}")

    # ────────── 定时调度 ──────────

    async def _cron_loop(self):
        while self._running:
            now = time.localtime()
            cur_min = now.tm_min
            if cur_min < self.cron_minute:
                wait = (self.cron_minute - cur_min) * 60 - now.tm_sec
            else:
                wait = (60 - cur_min + self.cron_minute) * 60 - now.tm_sec
            await asyncio.sleep(wait)
            if not self._running:
                break
            if self._generating:
                logger.warning("上一次报表尚未完成，跳过本次")
                continue
            try:
                await self._generate_and_send_to_group()
            except Exception as e:
                logger.error(f"定时报表生成失败: {e}", exc_info=True)

    # ────────── 背景图缓存 ──────────

    def _background_enabled(self) -> bool:
        return self.background_provider not in {"", "none", "off", "disabled"}

    def _background_provider_chain(self) -> list[str]:
        if not self._background_enabled():
            return []
        providers = [self.background_provider]
        providers.extend(self.background_fallback_chain.split(","))
        result = []
        for provider in providers:
            normalized = provider.strip().lower()
            if normalized and normalized not in result and normalized != "none":
                result.append(normalized)
        return result

    @staticmethod
    def _looks_like_image(content: bytes) -> bool:
        return (
            content.startswith(b"\xff\xd8\xff")
            or content.startswith(b"\x89PNG\r\n\x1a\n")
            or (content.startswith(b"RIFF") and b"WEBP" in content[:16])
            or content.startswith(b"GIF87a")
            or content.startswith(b"GIF89a")
        )

    async def _download_background_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict | None = None,
    ) -> bytes:
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=self.background_request_timeout),
            allow_redirects=True,
            proxy=self.background_proxy or None,
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            content = await resp.read()
            content_type = resp.headers.get("Content-Type", "")
        if not content_type.startswith("image/") and not self._looks_like_image(content):
            raise RuntimeError(f"响应不是图片: {content_type or 'unknown'}")
        return content

    async def _fetch_lolicon_background(
        self, session: aiohttp.ClientSession
    ) -> bytes:
        async with session.get(
            LOLICON_API_URL,
            params={"num": 1, "r18": self.background_lolicon_r18_type},
            timeout=aiohttp.ClientTimeout(total=self.background_request_timeout),
            proxy=self.background_proxy or None,
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Lolicon API HTTP {resp.status}")
            payload = await resp.json()
        items = payload.get("data") or []
        if not items:
            raise RuntimeError("Lolicon API 未返回图片")
        urls = items[0].get("urls") or {}
        image_url = urls.get("original") or urls.get("regular")
        if not image_url:
            raise RuntimeError("Lolicon API 未返回图片地址")
        return await self._download_background_url(
            session, image_url, headers={"Referer": "https://www.pixiv.net/"}
        )

    def _fetch_local_background(self) -> bytes:
        if not self.background_local_path:
            raise RuntimeError("未配置本地背景路径")
        path = Path(self.background_local_path).expanduser()
        if not path.is_absolute():
            path = Path(__file__).parent / path
        if path.is_dir():
            candidates = [
                item
                for item in path.iterdir()
                if item.is_file() and item.suffix.lower() in IMAGE_SUFFIXES
            ]
            if not candidates:
                raise RuntimeError("本地背景目录中没有图片")
            path = random.choice(candidates)
        if not path.is_file():
            raise RuntimeError(f"本地背景不存在: {path}")
        return path.read_bytes()

    async def _fetch_background_from_provider(
        self, provider: str, session: aiohttp.ClientSession
    ) -> bytes:
        if provider in {"custom", "custom_url", "url"}:
            if not self.background_url:
                raise RuntimeError("未配置自定义背景 URL")
            return await self._download_background_url(session, self.background_url)
        if provider in {"loli", "loliapi"}:
            return await self._download_background_url(session, DEFAULT_BACKGROUND_URL)
        if provider == "lolicon":
            return await self._fetch_lolicon_background(session)
        if provider in {"loli&lolicon", "random"}:
            choices = ["loli", "lolicon"]
            random.shuffle(choices)
            last_error = None
            for choice in choices:
                try:
                    return await self._fetch_background_from_provider(choice, session)
                except Exception as e:
                    last_error = e
            raise RuntimeError(f"随机在线背景获取失败: {last_error}")
        if provider == "local":
            return self._fetch_local_background()
        raise RuntimeError(f"未知背景提供方: {provider}")

    async def _fetch_background_bytes(self) -> bytes:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36"
            ),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        last_error = None
        async with aiohttp.ClientSession(headers=headers) as session:
            for provider in self._background_provider_chain():
                try:
                    return await self._fetch_background_from_provider(provider, session)
                except Exception as e:
                    last_error = e
                    logger.warning(f"背景提供方 {provider} 获取失败: {e}")
        raise RuntimeError(last_error or "没有可用的背景提供方")

    @staticmethod
    def _validate_background_bytes(content: bytes) -> bytes:
        if len(content) < 10_000 or len(content) > 20 * 1024 * 1024:
            raise RuntimeError(f"图片大小异常: {len(content)} bytes")
        return content

    async def _fill_background_queue(self):
        try:
            while self._background_enabled() and (
                self._background_queue.qsize() < self.background_preload_count
            ):
                content = self._validate_background_bytes(
                    await self._fetch_background_bytes()
                )
                await self._background_queue.put(content)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"背景图预加载失败: {e}")
        finally:
            self._background_preload_task = None

    def _ensure_background_preload(self):
        if not self._background_enabled():
            return
        task = self._background_preload_task
        if task and not task.done():
            return
        self._background_preload_task = asyncio.create_task(
            self._fill_background_queue()
        )

    async def _next_background_bytes(self) -> bytes:
        try:
            content = self._background_queue.get_nowait()
        except asyncio.QueueEmpty:
            content = self._validate_background_bytes(
                await self._fetch_background_bytes()
            )
        self._ensure_background_preload()
        return content

    async def _cache_background(self, force_refresh: bool = False):
        async with self._background_cache_lock:
            await self._cache_background_unlocked(force_refresh)

    async def _cache_background_unlocked(self, force_refresh: bool = False):
        """下载随机背景图；刷新失败时继续使用上一次缓存。"""
        data_dir = self._data_dir
        cache_path = data_dir / "bg_cache.jpg"
        temp_path = data_dir / "bg_cache.tmp"
        if not force_refresh and cache_path.exists() and cache_path.stat().st_size > 0:
            self._bg_cache_path = str(cache_path)
            logger.debug(f"背景图已缓存: {cache_path}")
            return
        old_hash = ""
        if cache_path.exists() and cache_path.stat().st_size > 0:
            old_hash = hashlib.sha256(cache_path.read_bytes()).hexdigest()
            self._bg_cache_path = str(cache_path)

        last_error = None
        for attempt in range(1, 4):
            try:
                content = await self._next_background_bytes()
                new_hash = hashlib.sha256(content).hexdigest()
                if force_refresh and old_hash and new_hash == old_hash and attempt < 3:
                    logger.debug(f"背景图刷新第 {attempt} 次仍为同一张，继续重试")
                    continue
                temp_path.write_bytes(content)
                os.replace(temp_path, cache_path)
                self._bg_cache_path = str(cache_path)
                logger.info(
                    f"背景图已刷新: {cache_path.name}, "
                    f"{len(content)} bytes, sha256={new_hash[:10]}"
                )
                return
            except Exception as e:
                last_error = e
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
        logger.warning(f"背景图刷新失败，继续使用旧缓存: {last_error}")

    @staticmethod
    def _build_mirrored_background(source_path: str, output_path: str) -> str:
        """将原图和上下翻转图拼成可无缝纵向重复的背景单元。"""
        from PIL import Image, ImageOps

        source = Path(source_path)
        output = Path(output_path)
        temp = output.with_suffix(".tmp")
        with Image.open(source) as opened:
            image = ImageOps.exif_transpose(opened)
            if image.mode not in {"RGB", "RGBA"}:
                image = image.convert("RGBA" if "A" in image.getbands() else "RGB")
            tile = Image.new(image.mode, (image.width, image.height * 2))
            tile.paste(image, (0, 0))
            tile.paste(ImageOps.flip(image), (0, image.height))
            tile.save(temp, format="WEBP", lossless=True, quality=100, method=4)
        os.replace(temp, output)
        return str(output)

    async def _prepare_report_background(self) -> str | None:
        """为长报表准备镜像平铺背景，失败时回退到原始背景。"""
        if not self._bg_cache_path:
            return None
        if self.background_fit_mode != "repeat":
            return self._bg_cache_path
        output_path = self._data_dir / "bg_repeat_mirror.webp"
        try:
            return await asyncio.to_thread(
                self._build_mirrored_background,
                self._bg_cache_path,
                str(output_path),
            )
        except Exception as e:
            logger.warning(f"镜像平铺背景生成失败，回退到普通纵向重复: {e}")
            return self._bg_cache_path

    # ────────── New-API 数据获取 ──────────

    @staticmethod
    def _read_cache_file(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _write_cache_file(temp_path, cache_path, data):
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, cache_path)

    async def _api_request(self, session, url, params, headers, max_retries=5):
        """带重试的 API 请求，支持 429 限流退避"""
        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        retry_after = to_int(resp.headers.get("Retry-After"), 0)
                        wait = min(300, max(retry_after, 10 * attempt))
                        last_err = RuntimeError(f"API 持续限流 429，已重试 {attempt} 次")
                        if attempt < max_retries:
                            logger.warning(
                                f"API 限流 429，等待 {wait}s 后重试 "
                                f"({attempt}/{max_retries})"
                            )
                            await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        detail = (await resp.text())[:200]
                        last_err = RuntimeError(
                            f"New-API HTTP {resp.status}: {detail or 'server error'}"
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(2 * attempt)
                        continue
                    if resp.status >= 400:
                        detail = (await resp.text())[:200]
                        raise RuntimeError(
                            f"New-API HTTP {resp.status}: {detail or 'request failed'}"
                        )
                    data = await resp.json(content_type=None)
                if not data.get("success"):
                    err_msg = data.get("message", "unknown")
                    raise RuntimeError(f"New-API 请求失败: {err_msg}")
                return data
            except ValueError as e:
                wait = 10 * attempt
                last_err = e
                logger.warning(
                    f"API JSON 响应异常，等待 {wait}s 后重试 "
                    f"({attempt}/{max_retries}): {e}"
                )
                if attempt < max_retries:
                    await asyncio.sleep(wait)
                    continue
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                logger.warning(f"API 请求尝试 {attempt}/{max_retries} 失败: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
        raise last_err or RuntimeError("API 请求重试耗尽")

    async def _fetch_logs(self, start_ts: int, end_ts: int) -> list:
        """从本地缓存读取日志（数据由1分钟同步任务维护）"""
        data_dir = self._data_dir
        cache_file = data_dir / "log_cache.json"

        if not cache_file.exists():
            logger.warning("本地缓存不存在，请等待数据同步")
            return []

        try:
            async with self._cache_lock:
                loop = asyncio.get_event_loop()
                cache_data = await loop.run_in_executor(None, self._read_cache_file, cache_file)
            cached_logs = cache_data.get("logs", [])
            cache_end_ts = cache_data.get("end_ts", 0)
            logger.info(f"读取本地缓存: {len(cached_logs)} 条, 缓存截止 {time.strftime('%m-%d %H:%M', time.localtime(cache_end_ts))}")
        except Exception as e:
            logger.warning(f"读取缓存失败: {e}")
            return []

        # 过滤时间范围
        result = [
            log
            for log in cached_logs
            if isinstance(log, dict)
            and start_ts <= normalize_timestamp(log.get("created_at")) < end_ts
        ]
        logger.info(f"日志获取完成: {len(result)} 条 (时间范围 {time.strftime('%m-%d %H:%M', time.localtime(start_ts))} ~ {time.strftime('%m-%d %H:%M', time.localtime(end_ts))})")
        return result

    async def _fetch_logs_from_api(self, start_ts: int, end_ts: int) -> list:
        """从 New-API REST API 严格拉全日志；任一分页失败则整轮失败。"""
        all_logs = []
        per_page = 100
        headers = {"Authorization": f"Bearer {self.newapi_token}"}
        if self.newapi_user_id:
            headers["New-Api-User"] = self.newapi_user_id

        # 按每30分钟一段拆分
        chunk_sec = 1800
        chunks = []
        t = start_ts
        while t < end_ts:
            chunk_end = min(t + chunk_sec, end_ts)
            chunks.append((t, chunk_end))
            t = chunk_end

        async with aiohttp.ClientSession() as session:
            for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
                page = 1
                chunk_fetched = 0
                chunk_total = None
                seen_page_signatures = set()
                while True:
                    params = {
                        "p": page,
                        "page_size": per_page,
                        "type": 2,
                        "start_timestamp": chunk_start,
                        "end_timestamp": chunk_end,
                    }
                    try:
                        data = await self._api_request(
                            session, f"{self.newapi_url}/api/log", params, headers
                        )
                    except Exception as e:
                        raise RuntimeError(
                            f"获取段 {chunk_idx+1}/{len(chunks)} page={page} 失败"
                        ) from e
                    resp_data = data.get("data", {})
                    if isinstance(resp_data, dict):
                        if "items" not in resp_data:
                            raise RuntimeError(
                                f"段 {chunk_idx+1}/{len(chunks)} page={page} "
                                "返回数据缺少 items"
                            )
                        logs = resp_data.get("items", [])
                        if (
                            chunk_total is None
                            and "total" in resp_data
                            and resp_data["total"] is not None
                        ):
                            try:
                                chunk_total = int(resp_data["total"])
                            except (TypeError, ValueError) as e:
                                raise RuntimeError(
                                    f"段 {chunk_idx+1}/{len(chunks)} page={page} "
                                    "返回的 total 格式异常"
                                ) from e
                            if chunk_total < 0:
                                raise RuntimeError(
                                    f"段 {chunk_idx+1}/{len(chunks)} page={page} "
                                    "返回的 total 小于 0"
                                )
                    else:
                        logs = resp_data if isinstance(resp_data, list) else []
                        chunk_total = None
                    if not isinstance(logs, list):
                        raise RuntimeError(
                            f"段 {chunk_idx+1}/{len(chunks)} page={page} 返回格式异常"
                        )
                    page_signature = tuple(
                        log.get("id")
                        or log.get("request_id")
                        or json.dumps(
                            log,
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        )
                        if isinstance(log, dict)
                        else repr(log)
                        for log in logs
                    )
                    if logs and page_signature in seen_page_signatures:
                        raise RuntimeError(
                            f"段 {chunk_idx+1}/{len(chunks)} page={page} "
                            "返回了重复分页，停止拉取以避免死循环"
                        )
                    seen_page_signatures.add(page_signature)
                    all_logs.extend(
                        log
                        for log in logs
                        if isinstance(log, dict)
                        and chunk_start
                        <= normalize_timestamp(log.get("created_at"))
                        < chunk_end
                    )
                    chunk_fetched += len(logs)
                    # 该段拉完：不足一页 或 已达到该段 total
                    if len(logs) < per_page or (chunk_total is not None and chunk_fetched >= chunk_total):
                        break
                    page += 1
                    if page > 10_000:
                        raise RuntimeError(
                            f"段 {chunk_idx+1}/{len(chunks)} 分页超过安全上限"
                        )
                    await asyncio.sleep(0.2)
                if chunk_total is not None and chunk_fetched < chunk_total:
                    raise RuntimeError(
                        f"段 {chunk_idx+1}/{len(chunks)} 数据不完整: "
                        f"拉取 {chunk_fetched}/{chunk_total}"
                    )
                await asyncio.sleep(0.1)
        logger.info(f"API 拉取完成: {len(all_logs)} 条")
        return all_logs

    async def _check_newapi_status(self) -> str:
        """检查 New-API 服务状态"""
        headers = {"Authorization": f"Bearer {self.newapi_token}"}
        if self.newapi_user_id:
            headers["New-Api-User"] = self.newapi_user_id
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.newapi_url}/api/status",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        return "ok"
        except Exception:
            pass
        return "down"

    # ────────── 数据处理 ──────────

    @staticmethod
    def _get_hour():
        now = time.time()
        end_ts = int(now)
        current_hour = int(now // 3600) * 3600
        hour_label = (
            time.strftime("%H:00", time.localtime(current_hour))
            + " – "
            + time.strftime("%H:%M %m/%d", time.localtime(end_ts))
        )
        return end_ts, hour_label

    @staticmethod
    def _process_data(logs, start_ts, end_ts, time_range_hours=24):
        # 严格使用调用方传入的滚动窗口结束时间，每档固定 30 分钟。
        slot_end = to_int(end_ts)
        num_slots = time_range_hours * 2
        range_start = slot_end - num_slots * 1800
        slots = [0] * num_slots
        slot_labels = []
        for i in range(num_slots):
            slot_start = range_start + i * 1800
            h = time.strftime("%H:%M", time.localtime(slot_start))
            slot_labels.append(h)

        model_stats = {}
        user_stats = {}
        total_tok_all = 0
        for log in logs:
            if not isinstance(log, dict) or to_int(log.get("type"), 2) != 2:
                continue
            created_at = normalize_timestamp(log.get("created_at"))
            if created_at < range_start or created_at >= slot_end:
                continue
            # 计算落在哪个slot
            slot_idx = int((created_at - range_start) // 1800)
            if 0 <= slot_idx < num_slots:
                slots[slot_idx] += 1
            model = str(log.get("model_name") or "unknown").strip() or "unknown"
            prompt_tok = max(0, to_int(log.get("prompt_tokens")))
            comp_tok = max(0, to_int(log.get("completion_tokens")))
            total_tok = prompt_tok + comp_tok
            total_tok_all += total_tok
            if model not in model_stats:
                model_stats[model] = {"cnt": 0, "tok": 0}
            model_stats[model]["cnt"] += 1
            model_stats[model]["tok"] += total_tok
            ip = str(log.get("ip") or "").strip()
            username = str(log.get("username") or "").strip()
            if ip or username:
                user_id = to_int(log.get("user_id"))
                key = f"id:{user_id}" if user_id else username or f"ip:{ip}"
                if key not in user_stats:
                    user_stats[key] = {
                        "name": username or "匿名用户",
                        "cnt": 0,
                        "tok": 0,
                        "ips": {},
                    }
                user_stats[key]["cnt"] += 1
                user_stats[key]["tok"] += total_tok
                if ip:
                    user_stats[key]["ips"][ip] = (
                        user_stats[key]["ips"].get(ip, 0) + total_tok
                    )
        models_sorted = sorted(model_stats.items(), key=lambda x: x[1]["tok"], reverse=True)[:10]
        users_sorted = sorted(user_stats.values(), key=lambda x: x["tok"], reverse=True)[:10]
        for user in users_sorted:
            user["ips_display"] = (
                [
                    ip
                    for ip, _ in sorted(
                        user["ips"].items(),
                        key=lambda item: item[1],
                        reverse=True,
                    )
                ]
                if user["ips"]
                else ["未记录"]
            )
        return slots, slot_labels, models_sorted, users_sorted, total_tok_all

    # ────────── 系统状态 ──────────

    @staticmethod
    def _get_system_status():
        cpu_pct = psutil.cpu_percent(interval=0.5)
        cpu_str = f"{cpu_pct:.1f}%"
        mem = psutil.virtual_memory()
        mem_pct = mem.percent
        mem_str = f"{mem.used / 1073741824:.1f}/{mem.total / 1073741824:.1f}G ({mem_pct}%)"
        swap = psutil.swap_memory()
        swap_str = f"{swap.used / 1073741824:.1f}/{swap.total / 1073741824:.1f}G ({swap.percent}%)"
        disk = psutil.disk_usage("/")
        disk_pct = disk.percent
        disk_str = f"{disk.used / 1073741824:.1f}/{disk.total / 1073741824:.1f}G ({disk_pct}%)"
        disk_warn = "warn" if disk_pct > 90 else ""
        net = psutil.net_io_counters()
        rx_str = f"{net.bytes_recv / 1048576:.1f}MB"
        tx_str = f"{net.bytes_sent / 1048576:.1f}MB"
        load_avg = "N/A"
        if hasattr(os, "getloadavg"):
            try:
                load_avg = ", ".join(f"{x:.2f}" for x in os.getloadavg())
            except Exception:
                pass
        uptime_sec = time.time() - psutil.boot_time()
        days = int(uptime_sec // 86400)
        hours = int((uptime_sec % 86400) // 3600)
        uptime_str = f"{days}天{hours}小时"
        # IO 统计
        io_str = "N/A"
        try:
            io = psutil.disk_io_counters()
            if io:
                io_str = f"R:{io.read_bytes/1048576:.0f}MB W:{io.write_bytes/1048576:.0f}MB"
        except Exception:
            pass
        # CPU 温度
        temp_str = "N/A"
        try:
            temps = psutil.sensors_temperatures()
            if temps:
                for name, entries in temps.items():
                    if entries:
                        temp_str = f"{entries[0].current:.0f}°C"
                        break
        except Exception:
            pass
        procs = []
        for proc in sorted(
            psutil.process_iter(["name", "memory_percent"]),
            key=lambda p: p.info.get("memory_percent", 0) or 0,
            reverse=True,
        )[:7]:
            try:
                cpu_pct_p = proc.cpu_percent(interval=0)
                procs.append(
                    {
                        "name": (proc.info.get("name") or "unknown")[:14],
                        "cpu": round(cpu_pct_p, 1),
                        "mem": round(proc.info.get("memory_percent", 0) or 0, 1),
                    }
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return {
            "cpu": cpu_str,
            "cpu_pct": cpu_pct,
            "mem": mem_str,
            "mem_pct": mem_pct,
            "swap": swap_str,
            "disk": disk_str,
            "disk_warn": disk_warn,
            "load": load_avg,
            "rx": rx_str,
            "tx": tx_str,
            "io": io_str,
            "temp": temp_str,
            "uptime": uptime_str,
            "procs_n": sum(1 for _ in psutil.process_iter()),
            "procs": procs,
        }

    # ────────── HTML 生成 ──────────

    @staticmethod
    def _build_html(
        slots,
        slot_labels,
        models_sorted,
        users_sorted,
        total_tok,
        sys_status,
        hour_label,
        newapi_status,
        time_range_hours=24,
        bg_image_path=None,
        show_ip_addresses=True,
        show_ipv4_addresses=True,
        show_ipv6_addresses=True,
        background_fit_mode="repeat",
        background_position="top",
    ):
        total_req = sum(slots)
        peak = max(slots) if slots else 0
        slots_json = json_for_script(slots)
        labels_json = json_for_script(slot_labels)
        models_json = json_for_script(
            [{"name": m[0], "cnt": m[1]["cnt"], "tok": m[1]["tok"]} for m in models_sorted]
        )
        def visible_ips(user):
            if not show_ip_addresses:
                return []
            result = []
            for raw_ip in user["ips_display"]:
                if raw_ip == "未记录":
                    result.append(raw_ip)
                    continue
                try:
                    version = ipaddress.ip_address(raw_ip).version
                except ValueError:
                    continue
                if version == 4 and show_ipv4_addresses:
                    result.append(raw_ip)
                elif version == 6 and show_ipv6_addresses:
                    result.append(raw_ip)
            return result or ["未记录"]

        effective_show_ip = bool(
            show_ip_addresses and (show_ipv4_addresses or show_ipv6_addresses)
        )
        ip_type_label = (
            "IPv4 / IPv6"
            if show_ipv4_addresses and show_ipv6_addresses
            else "IPv4"
            if show_ipv4_addresses
            else "IPv6"
        )
        users_json = json_for_script(
            [
                {
                    "name": user["name"],
                    "ips": visible_ips(user),
                    "cnt": user["cnt"],
                    "tok": user["tok"],
                }
                for user in users_sorted
            ]
        )
        procs_json = json_for_script(sys_status["procs"])
        newapi_cls = "ok" if newapi_status == "ok" else "warn"
        newapi_label = "Active" if newapi_status == "ok" else "Down"
        # 背景图样式
        bg_style = ""
        if bg_image_path:
            bg_uri = Path(bg_image_path).as_uri()
            position = (
                background_position
                if background_position in {"top", "center", "bottom"}
                else "top"
            )
            if background_fit_mode == "cover":
                bg_style = (
                    f"background-image:url('{bg_uri}');"
                    "background-size:cover;background-repeat:no-repeat;"
                    f"background-position:center {position};"
                )
            elif background_fit_mode == "contain":
                bg_style = (
                    f"background-image:url('{bg_uri}');"
                    "background-size:100% auto;background-repeat:no-repeat;"
                    f"background-position:center {position};"
                )
            else:
                # repeat 模式传入的是原图与上下翻转图组成的无缝背景单元。
                bg_style = (
                    "background-image:"
                    "linear-gradient(135deg,rgba(255,240,245,0.14),"
                    "rgba(240,248,255,0.10),rgba(255,245,245,0.14)),"
                    f"url('{bg_uri}');"
                    "background-size:cover,100% auto;"
                    "background-repeat:no-repeat,repeat-y;"
                    "background-position:center,center top;"
                )
        user_rank_title = "用户 / IP Token 排行" if effective_show_ip else "用户 Token 排行"
        user_rank_sub = (
            f"近{time_range_hours}小时 Token 消耗 Top 10 · 完整 IP 见下方明细"
            if effective_show_ip
            else f"近{time_range_hours}小时 Token 消耗 Top 10"
        )
        ip_column_header = '<span class="ch-ip">IP 数量</span>' if effective_show_ip else ""
        ip_detail_card = (
            f"""<div class="card ip-detail-card">
  <h3><span class="dot"></span>账号完整 IP 明细</h3>
  <div class="sub">近{time_range_hours}小时内 Top 10 用户出现过的全部完整 {ip_type_label}，不截断</div>
  <div class="ip-groups" id="ip-details"></div>
</div>"""
            if effective_show_ip
            else ""
        )
        show_ip_json = "true" if effective_show_ip else "false"

        html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{
    background:linear-gradient(135deg,#FFF0F5 0%,#F0F8FF 50%,#FFF5F5 100%);
    {bg_style}
    color:#334155;
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif;
    padding:16px 20px;
    display:flex;gap:14px;
    flex-wrap:wrap;
    align-items:flex-start;
    text-rendering:geometricPrecision;
    -webkit-font-smoothing:antialiased;
  }}
  .left{{flex:1;min-width:0;display:flex;flex-direction:column;gap:10px}}
  .right{{width:440px;flex-shrink:0;display:flex;flex-direction:column;gap:10px}}
  .card{{
    background:rgba(255,255,255,0.70);
    border:1px solid rgba(255,182,193,0.25);
    border-radius:18px;
    padding:12px 15px;
    box-shadow:0 2px 12px rgba(255,182,193,0.12),0 1px 3px rgba(167,199,231,0.15);
    backdrop-filter:blur(8px);
    -webkit-backdrop-filter:blur(8px);
  }}
  h3{{font-size:16px;font-weight:800;margin-bottom:3px;letter-spacing:0.3px;color:#1e293b}}
  h3 .dot{{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;background:linear-gradient(135deg,#FFB6C1,#A7C7E7)}}
  h3 span.accent{{color:#FF9EBB;font-weight:700}}
  .sub{{font-size:12px;font-weight:500;color:#475569;margin-bottom:7px}}
  .chart{{display:flex;align-items:flex-end;gap:1px;height:140px;padding:2px 0;overflow:visible}}
  .bar-col{{flex:1;display:flex;flex-direction:column;align-items:center;min-width:0;height:100%}}
  .bar-fill{{
    width:100%;max-width:18px;
    border-radius:3px 3px 1px 1px;
    margin-top:auto;min-height:0;
  }}
  .bar-label{{font-size:8.5px;font-family:'Consolas','Courier New',monospace;font-weight:600;color:#475569;margin-top:2px}}
  .rank-list{{display:flex;flex-direction:column;gap:6px}}
  .rank-row{{display:flex;align-items:center;gap:7px;font-size:13px;padding:3px 0}}
  .rank-num{{
    width:20px;height:20px;border-radius:7px;
    font-size:11px;font-weight:800;color:#fff;
    display:flex;align-items:center;justify-content:center;flex-shrink:0;
    box-shadow:0 1px 4px rgba(0,0,0,0.10);
  }}
  .rn1{{background:linear-gradient(135deg,#FF9EBB,#FFB6C1);box-shadow:0 0 8px rgba(255,158,187,0.45)}}
  .rn2{{background:linear-gradient(135deg,#A7C7E7,#C9E4DE);box-shadow:0 0 6px rgba(167,199,231,0.4)}}
  .rn3{{background:linear-gradient(135deg,#FFD700,#FFA500);box-shadow:0 0 5px rgba(255,215,0,0.35)}}
  .rnn{{background:#E8ECF1;color:#94a3b8}}
  .rank-name{{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;font-size:12px;color:#1e293b}}
  .rank-user{{width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;font-size:11.5px;color:#1e293b}}
  .rank-ip-count{{width:118px;font-family:'Consolas','Courier New',monospace;font-weight:700;font-size:10.5px;color:#334155}}
  .rank-stat{{font-family:'Consolas','Courier New',monospace;font-size:11.5px;font-weight:600;color:#475569;white-space:nowrap}}
  .rank-stat b{{color:#FF9EBB;font-weight:700}}
  .col-header{{display:flex;align-items:center;gap:7px;font-size:11px;font-weight:600;color:#475569;padding-bottom:5px;border-bottom:1px solid rgba(255,182,193,0.3);margin-bottom:3px}}
  .ch-name{{flex:1}}.ch-user{{width:110px}}.ch-ip{{width:118px}}.ch-stat{{width:68px;text-align:right}}
  .status-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px 12px;font-size:12px}}
  .status-item{{display:flex;justify-content:space-between;align-items:center}}
  .status-key{{color:#475569;font-size:11.5px;font-weight:600}}
  .status-val{{font-family:'Consolas','Courier New',monospace;font-weight:700;color:#1e293b;font-size:11.5px}}
  .status-val.ok{{color:#16a34a}}.status-val.warn{{color:#f59e0b}}.status-val.danger{{color:#ef4444}}
  .proc-list{{display:flex;flex-direction:column;gap:6px}}
  .proc-row{{display:flex;align-items:center;gap:6px;font-size:12px}}
  .proc-name{{width:90px;font-weight:700;color:#1e293b;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}}
  .proc-bar{{flex:1;height:5px;background:rgba(167,199,231,0.18);border-radius:3px;overflow:hidden}}
  .pcpu{{height:100%;border-radius:3px;background:linear-gradient(90deg,#FFB6C1,#FF9EBB)}}
  .pmem{{height:100%;border-radius:3px;background:linear-gradient(90deg,#A7C7E7,#7EB8DA)}}
  .pstat{{font-family:'Consolas','Courier New',monospace;font-size:11px;font-weight:600;color:#475569;width:68px;text-align:right}}
  .summary{{display:flex;gap:14px;margin-top:8px;font-size:10px;color:#64748b;flex-wrap:wrap}}
  .summary b{{color:#FF9EBB;font-family:'Consolas','Courier New',monospace;font-weight:700}}
  .footer{{margin-top:auto;text-align:right;font-size:9px;color:#cbd5e1}}
  .footer span{{color:#FFB6C1;margin:0 2px}}
  .overview-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;margin-top:7px}}
  .overview-item{{background:rgba(255,255,255,0.50);border:1px solid rgba(148,163,184,0.16);border-radius:9px;padding:7px 9px}}
  .overview-key{{font-size:10px;font-weight:700;color:#64748b;margin-bottom:2px}}
  .overview-val{{font-family:'Consolas','Courier New',monospace;font-size:14px;font-weight:800;color:#1e293b;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .share-title{{font-size:11px;font-weight:800;color:#475569;margin:9px 0 5px}}
  .share-list{{display:flex;flex-direction:column;gap:5px}}
  .share-row{{display:grid;grid-template-columns:125px 1fr 54px;gap:7px;align-items:center}}
  .share-name{{font-size:10px;font-weight:700;color:#334155;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .share-track{{height:6px;background:rgba(167,199,231,0.20);border-radius:4px;overflow:hidden}}
  .share-fill{{height:100%;border-radius:4px;background:linear-gradient(90deg,#FF9EBB,#A7C7E7)}}
  .share-pct{{font-family:'Consolas','Courier New',monospace;font-size:10px;font-weight:700;color:#475569;text-align:right}}
  .focus-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px;margin-top:6px}}
  .focus-item{{background:rgba(255,255,255,0.50);border:1px solid rgba(148,163,184,0.16);border-radius:8px;padding:6px 8px}}
  .focus-key{{font-size:9.5px;font-weight:700;color:#64748b;margin-bottom:2px}}
  .focus-val{{font-family:'Consolas','Courier New',monospace;font-size:12px;font-weight:800;color:#1e293b}}
  .ip-detail-card{{width:100%;padding:14px 16px}}
  .ip-groups{{display:flex;flex-direction:column;gap:12px}}
  .ip-group{{border-top:1px solid rgba(255,182,193,0.25);padding-top:9px}}
  .ip-group:first-child{{border-top:0;padding-top:0}}
  .ip-group-title{{font-size:12px;font-weight:800;color:#1e293b;margin-bottom:6px}}
  .ip-group-title span{{font-family:'Consolas','Courier New',monospace;color:#FF7FA5;margin-left:6px}}
  .ip-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:4px 8px}}
  .ip-address{{font-family:'Consolas','Courier New',monospace;font-size:9.5px;font-weight:700;line-height:1.25;color:#334155;background:rgba(255,255,255,0.56);border:1px solid rgba(148,163,184,0.20);border-radius:5px;padding:3px 5px;overflow-wrap:anywhere}}
</style>
</head>
<body>
<div class="left">
  <div class="card">
    <h3><span class="dot"></span><span class="accent">{hour_label}</span> 请求统计（近{time_range_hours}小时）</h3>
    <div class="sub">总 {fmt_tok(total_tok)} tokens · 请求 {total_req} · 峰值 {peak}/30min</div>
    <div class="chart" id="chart"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>服务器状态</h3>
    <div class="status-grid" id="status"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>进程占用 Top 7</h3>
    <div class="proc-list" id="procs"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>近{time_range_hours}小时概览</h3>
    <div class="sub">请求效率、活跃范围与模型 Token 分布</div>
    <div class="overview-grid" id="overview"></div>
    <div class="share-title">模型 Token 占比 Top 5</div>
    <div class="share-list" id="token-share"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>用量集中度</h3>
    <div class="focus-grid" id="focus"></div>
  </div>
</div>
<div class="right">
  <div class="card">
    <h3><span class="dot"></span>模型 Token 排行</h3>
    <div class="sub">近{time_range_hours}小时 Token 消耗 Top 10</div>
    <div class="col-header"><span class="ch-name">模型</span><span class="ch-stat">Token</span><span class="ch-stat">调用</span></div>
    <div class="rank-list" id="model-rank"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>{user_rank_title}</h3>
    <div class="sub">{user_rank_sub}</div>
    <div class="col-header"><span class="ch-user">用户</span>{ip_column_header}<span class="ch-stat">Token</span><span class="ch-stat">调用</span></div>
    <div class="rank-list" id="ip-rank"></div>
  </div>
</div>
{ip_detail_card}
<script>
const SLOTS={slots_json};
const LABELS={labels_json};
const MODELS={models_json};
const USERS={users_json};
const PROCS={procs_json};
const TOTAL_TOK={int(total_tok)};
const SHOW_IP={show_ip_json};
const maxS=Math.max(...SLOTS,1);
const gradientColors=[
  '#FFB6C1','#FFA5B9','#FF94B0','#FF83A8','#FF729F',
  '#FF6197','#FF508E','#FF3F86','#FF2E7D','#FF1D75',
  '#A7C7E7','#98C0E4','#89B9E1','#7AB2DE','#6BABDB',
  '#5CA4D8','#4D9DD5','#3E96D2','#2F8FCF','#2588CC'
];
function esc(v){{
  return String(v).replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
}}
document.getElementById('chart').innerHTML=SLOTS.map((c,i)=>{{
  const pct=(c/maxS*100).toFixed(1);
  const level=Math.min(Math.floor(c/maxS*20),19);
  const color=c===0?'#f1f5f9':gradientColors[level];
  const hasGlow=c>=maxS*0.5;
  // 智能标签：根据总柱数决定显示间隔
  const step=SLOTS.length>36?4:SLOTS.length>20?2:1;
  const label=i%step===0?LABELS[i]:'';
  return `<div class="bar-col"><div class="bar-fill" style="height:${{pct}}%;background:${{color}}${{hasGlow?';box-shadow:0 0 6px '+color+'66':''}}"></div><div class="bar-label">${{label}}</div></div>`;
}}).join('');
function ft(v){{if(v>=1e6)return(v/1e6).toFixed(2)+'M';if(v>=1e3)return(v/1e3).toFixed(1)+'K';return v;}}
document.getElementById('model-rank').innerHTML=MODELS.map((m,i)=>{{
  const rc=i===0?'rn1':i===1?'rn2':i===2?'rn3':'rnn';
  return `<div class="rank-row"><div class="rank-num ${{rc}}">${{i+1}}</div><div class="rank-name" title="${{esc(m.name)}}">${{esc(m.name)}}</div><div class="rank-stat"><b>${{ft(m.tok)}}</b></div><div class="rank-stat">${{m.cnt}}次</div></div>`;
}}).join('');
document.getElementById('ip-rank').innerHTML=USERS.map((user,i)=>{{
  const rc=i===0?'rn1':i===1?'rn2':i===2?'rn3':'rnn';
  const realIps=user.ips.filter(ip=>ip!=='未记录');
  const ipSummary=realIps.length?`${{realIps.length}} 个完整 IP`:'未记录';
  const ipCell=SHOW_IP?`<div class="rank-ip-count">${{ipSummary}}</div>`:'';
  const userClass=SHOW_IP?'rank-user':'rank-name';
  return `<div class="rank-row"><div class="rank-num ${{rc}}">${{i+1}}</div><div class="${{userClass}}" title="${{esc(user.name)}}">${{esc(user.name)}}</div>${{ipCell}}<div class="rank-stat"><b>${{ft(user.tok)}}</b></div><div class="rank-stat">${{user.cnt}}次</div></div>`;
}}).join('');
if(SHOW_IP){{
  const usersWithIps=USERS.filter(user=>user.ips.some(ip=>ip!=='未记录'));
  document.getElementById('ip-details').innerHTML=usersWithIps.length?usersWithIps.map(user=>{{
    const realIps=user.ips.filter(ip=>ip!=='未记录');
    return `<div class="ip-group"><div class="ip-group-title">${{esc(user.name)}}<span>${{realIps.length}} 个</span></div><div class="ip-grid">${{realIps.map(ip=>`<div class="ip-address">${{esc(ip)}}</div>`).join('')}}</div></div>`;
  }}).join(''):'<div class="sub">当前 Top 10 用户的日志均未记录 IP</div>';
}}
document.getElementById('status').innerHTML=[
  {{k:'CPU',v:'{sys_status["cpu"]}',cls:{sys_status["cpu_pct"]}>=80?'danger':{sys_status["cpu_pct"]}>=60?'warn':''}},
  {{k:'内存',v:'{sys_status["mem"]}',cls:{sys_status["mem_pct"]}>=90?'danger':{sys_status["mem_pct"]}>=75?'warn':''}},
  {{k:'Swap',v:'{sys_status["swap"]}'}},
  {{k:'磁盘',v:'{sys_status["disk"]}',cls:'{sys_status["disk_warn"]}'}},
  {{k:'负载',v:'{sys_status["load"]}'}},
  {{k:'温度',v:'{sys_status["temp"]}'}},
  {{k:'网络RX',v:'{sys_status["rx"]}'}},
  {{k:'网络TX',v:'{sys_status["tx"]}'}},
  {{k:'磁盘IO',v:'{sys_status["io"]}'}},
  {{k:'运行时间',v:'{sys_status["uptime"]}'}},
  {{k:'进程数',v:'{sys_status["procs_n"]}'}},
  {{k:'New-API',v:'{newapi_label}',cls:'{newapi_cls}'}},
].map(s=>`<div class="status-item"><span class="status-key">${{s.k}}</span><span class="status-val ${{s.cls||''}}">${{s.v}}</span></div>`).join('');
const maxPC=Math.max(1,...PROCS.map(p=>p.cpu));
const maxPM=Math.max(1,...PROCS.map(p=>p.mem));
document.getElementById('procs').innerHTML=PROCS.map(p=>{{
  const cw=(p.cpu/maxPC*100).toFixed(1);
  const mw=(p.mem/maxPM*100).toFixed(1);
  return `<div class="proc-row"><div class="proc-name">${{esc(p.name)}}</div><div class="pstat">CPU ${{p.cpu}}%</div><div class="proc-bar"><div class="pcpu" style="width:${{cw}}%"></div></div><div class="pstat">MEM ${{p.mem}}%</div><div class="proc-bar"><div class="pmem" style="width:${{mw}}%"></div></div></div>`;
}}).join('');
const totalReq=SLOTS.reduce((sum,value)=>sum+value,0);
const peakValue=Math.max(...SLOTS,0);
const peakIndex=SLOTS.indexOf(peakValue);
const uniqueIps=new Set(USERS.flatMap(user=>user.ips).filter(ip=>ip!=='未记录'));
const overviewItems=[
  ['总请求',totalReq.toLocaleString()],
  ['平均请求 / 30min',(totalReq/Math.max(SLOTS.length,1)).toFixed(1)],
  ['平均 Token / 请求',totalReq?ft(Math.round(TOTAL_TOK/totalReq)):'0'],
  ['上榜账号',USERS.length.toLocaleString()],
  ['上榜完整 IP',SHOW_IP?uniqueIps.size.toLocaleString():'已隐藏'],
  ['峰值时段',peakValue?`${{LABELS[peakIndex]}} · ${{peakValue}}次`:'暂无请求'],
];
document.getElementById('overview').innerHTML=overviewItems.map(item=>
  `<div class="overview-item"><div class="overview-key">${{item[0]}}</div><div class="overview-val" title="${{item[1]}}">${{item[1]}}</div></div>`
).join('');
document.getElementById('token-share').innerHTML=MODELS.slice(0,5).map(model=>{{
  const pct=TOTAL_TOK?Math.min(100,model.tok/TOTAL_TOK*100):0;
  return `<div class="share-row"><div class="share-name" title="${{esc(model.name)}}">${{esc(model.name)}}</div><div class="share-track"><div class="share-fill" style="width:${{pct.toFixed(2)}}%"></div></div><div class="share-pct">${{pct.toFixed(1)}}%</div></div>`;
}}).join('')||'<div class="sub">暂无模型调用数据</div>';
const modelTop3=MODELS.slice(0,3).reduce((sum,model)=>sum+model.tok,0);
const userTop3=USERS.slice(0,3).reduce((sum,user)=>sum+user.tok,0);
const ipv4Count=[...uniqueIps].filter(ip=>!ip.includes(':')).length;
const ipv6Count=[...uniqueIps].filter(ip=>ip.includes(':')).length;
const focusItems=[
  ['Top 3 模型',TOTAL_TOK?`${{(modelTop3/TOTAL_TOK*100).toFixed(1)}}%`:'0%'],
  ['Top 3 账号',TOTAL_TOK?`${{(userTop3/TOTAL_TOK*100).toFixed(1)}}%`:'0%'],
  ['上榜 IPv4',SHOW_IP?ipv4Count.toLocaleString():'已隐藏'],
  ['上榜 IPv6',SHOW_IP?ipv6Count.toLocaleString():'已隐藏'],
];
document.getElementById('focus').innerHTML=focusItems.map(item=>
  `<div class="focus-item"><div class="focus-key">${{item[0]}}</div><div class="focus-val">${{item[1]}}</div></div>`
).join('');
</script>
</body>
</html>"""
        return html

    # ────────── 截图 ──────────

    async def _screenshot(self, html_path: str, output_path: str):
        from playwright.async_api import async_playwright

        last_err = None
        for attempt in range(1, 4):
            browser = None
            try:
                async with async_playwright() as p:
                    launch_opts = {
                        "headless": True,
                        "args": [
                            "--no-sandbox",
                            "--disable-gpu",
                            "--disable-dev-shm-usage",
                        ],
                    }
                    if self.browser_executable:
                        launch_opts["executable_path"] = self.browser_executable
                    browser = await p.chromium.launch(**launch_opts)
                    page = await browser.new_page(
                        viewport={"width": 1100, "height": 1200},
                        device_scale_factor=2,
                    )
                    await page.goto(
                        Path(html_path).as_uri(),
                        wait_until="commit",
                        timeout=15000,
                    )
                    await page.wait_for_timeout(2000)
                    await page.locator("body").screenshot(
                        path=output_path,
                        timeout=30000,
                    )
                    await browser.close()
                    return
            except Exception as e:
                last_err = e
                logger.warning(f"截图尝试 {attempt}/3 失败: {e}")
                try:
                    if browser:
                        await browser.close()
                except Exception:
                    pass
                await asyncio.sleep(2 * attempt)
        raise last_err

    # ────────── 报表生成 ──────────

    async def _generate_report(self) -> str:
        """生成报表并返回图片路径"""
        if self._background_enabled():
            await self._cache_background(force_refresh=True)
        report_background_path = await self._prepare_report_background()
        end_ts, _ = self._get_hour()
        # 按当前分钟结束，统计严格的滚动时间窗口，标题不会指向未来。
        slot_end = (to_int(end_ts) // 60) * 60
        start_ts_range = slot_end - self.time_range_hours * 3600
        hour_label = (
            time.strftime("%m/%d %H:%M", time.localtime(start_ts_range))
            + " – "
            + time.strftime("%m/%d %H:%M", time.localtime(slot_end))
        )
        # API 查询范围也用 slot_end，确保和 _process_data 的档位范围一致
        logs = await self._fetch_logs(start_ts_range, slot_end)
        logger.info(f"获取到 {len(logs)} 条日志（时间范围 {self.time_range_hours} 小时）")
        if logs:
            ca = logs[0].get("created_at", 0)
            if ca > 1e12:
                logger.warning("created_at 是毫秒级，需要转换！")
        slots, slot_labels, models_sorted, users_sorted, total_tok = self._process_data(
            logs, start_ts_range, slot_end, self.time_range_hours
        )
        sys_status = self._get_system_status()
        newapi_status = await self._check_newapi_status()
        html = self._build_html(
            slots, slot_labels, models_sorted, users_sorted, total_tok, sys_status,
            hour_label, newapi_status, self.time_range_hours, report_background_path,
            self.show_ip_addresses, self.show_ipv4_addresses, self.show_ipv6_addresses,
            self.background_fit_mode, self.background_position
        )
        data_dir = self._data_dir
        html_path = data_dir / "minute-report.html"
        png_path = data_dir / "minute-report.png"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        await self._screenshot(str(html_path), str(png_path))
        return str(png_path)

    async def _generate_and_send_to_group(self):
        """生成报表并发送到配置的 QQ 群"""
        self._generating = True
        try:
            png_path = await self._generate_report()
            if not self.group_ids:
                logger.warning("无法发送报表：群号未配置")
                return
            for gid in self.group_ids:
                try:
                    await StarTools.send_message_by_id(
                        type="GroupMessage",
                        id=gid,
                        message_chain=MessageChain().file_image(png_path),
                        platform="aiocqhttp",
                    )
                    logger.info(f"报表已发送到群 {gid}")
                except Exception as e:
                    logger.error(f"报表发送到群 {gid} 失败: {e}")
        finally:
            self._generating = False

    # ────────── 指令处理 ──────────

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("报表")
    async def report_cmd(self, event: AstrMessageEvent):
        """手动生成 New-API 监控报表"""
        if not self.newapi_url or not self.newapi_token:
            yield event.plain_result("请先在插件配置中填写 New-API 地址和系统访问令牌")
            return
        if self._generating:
            yield event.plain_result("报表正在生成，请稍后再试")
            return
        self._generating = True
        try:
            png_path = await self._generate_report()
            yield event.image_result(png_path)
        except Exception as e:
            logger.error(f"报表生成失败: {e}", exc_info=True)
            yield event.plain_result(f"报表生成失败: {e}")
        finally:
            self._generating = False

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("报表底图", alias={"报表原图"})
    async def report_background_cmd(self, event: AstrMessageEvent):
        """发送最近一次报表实际使用的背景原图。"""
        if not self._background_enabled():
            yield event.plain_result("报表背景图已禁用")
            return
        cache_path = Path(self._bg_cache_path) if self._bg_cache_path else None
        if not cache_path or not cache_path.exists():
            await self._cache_background(force_refresh=True)
            cache_path = Path(self._bg_cache_path) if self._bg_cache_path else None
        if not cache_path or not cache_path.exists():
            yield event.plain_result("暂时没有可发送的报表底图，请稍后再试")
            return
        yield event.image_result(str(cache_path))
