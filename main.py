"""New-API 每小时监控报表 · AstrBot 插件版"""
import asyncio
import base64
import json
import os
import time
from pathlib import Path

import aiohttp
import psutil
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.all import Star, Context, AstrBotConfig, logger
from astrbot.api.star import StarTools
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)


def fmt_tok(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return str(v)


def mask_ip(ip):
    parts = ip.split(".")
    return f"{parts[0]}.{parts[1]}.*.*" if len(parts) == 4 else ip


class NewApiHourlyReport(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.newapi_url = config.get("newapi_url", "").rstrip("/")
        self.newapi_token = config.get("newapi_token", "")
        self.newapi_user_id = str(config.get("newapi_user_id", ""))
        # 多群支持：逗号分隔
        group_ids_raw = str(config.get("group_ids", "") or config.get("group_id", ""))
        self.group_ids = [g.strip() for g in group_ids_raw.split(",") if g.strip()]
        self.cron_minute = max(0, min(59, config.get("cron_minute", 5)))
        self.enable_cron = config.get("enable_cron", True)
        self.time_range_hours = max(1, min(48, config.get("time_range_hours", 9)))
        self.browser_executable = config.get("browser_executable", "")
        self.background_url = config.get("background_url", "")
        self._task = None
        self._running = False
        self._generating = False
        self._bot = None
        self._bg_cache_path = None

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
        # 预缓存背景图
        if self.background_url:
            await self._cache_background()
        if self.enable_cron:
            self._running = True
            self._task = asyncio.create_task(self._cron_loop())
            logger.info(f"New-API 定时报表已启动，每小时第 {self.cron_minute} 分钟执行，目标群: {self.group_ids}")

    async def terminate(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

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

    async def _cache_background(self):
        """下载并缓存背景图到本地，避免每次生成报表重新下载"""
        data_dir = StarTools.get_data_dir()
        cache_path = data_dir / "bg_cache.jpg"
        # 已缓存则跳过
        if cache_path.exists() and cache_path.stat().st_size > 0:
            self._bg_cache_path = str(cache_path)
            logger.debug(f"背景图已缓存: {cache_path}")
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.background_url,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        with open(cache_path, "wb") as f:
                            f.write(await resp.read())
                        self._bg_cache_path = str(cache_path)
                        logger.info(f"背景图缓存成功: {cache_path}")
                    else:
                        logger.warning(f"背景图下载失败: HTTP {resp.status}")
        except Exception as e:
            logger.warning(f"背景图缓存失败: {e}")

    # ────────── New-API 数据获取 ──────────

    async def _api_request(self, session, url, params, headers, max_retries=3):
        """带重试的 API 请求"""
        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    data = await resp.json()
                if not data.get("success"):
                    err_msg = data.get("message", "unknown")
                    if attempt < max_retries and "rate" not in err_msg.lower():
                        raise Exception(f"New-API 请求失败: {err_msg}")
                    raise Exception(f"New-API 请求失败: {err_msg}")
                return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                logger.warning(f"API 请求尝试 {attempt}/{max_retries} 失败: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
        raise last_err

    async def _fetch_logs(self, start_ts: int, end_ts: int) -> list:
        """通过 New-API REST API 分页获取日志"""
        all_logs = []
        page = 0
        per_page = 100
        headers = {"Authorization": f"Bearer {self.newapi_token}"}
        if self.newapi_user_id:
            headers["New-Api-User"] = self.newapi_user_id
        async with aiohttp.ClientSession() as session:
            while True:
                params = {
                    "p": page,
                    "per_page": per_page,
                    "start_timestamp": start_ts,
                    "end_timestamp": end_ts,
                }
                data = await self._api_request(
                    session, f"{self.newapi_url}/api/log", params, headers
                )
                resp_data = data.get("data", {})
                # New-API 返回分页对象: {page, page_size, total, items}
                if isinstance(resp_data, dict) and "items" in resp_data:
                    logs = resp_data.get("items", [])
                    total = resp_data.get("total", 0)
                else:
                    logs = resp_data if isinstance(resp_data, list) else []
                    total = data.get("total", len(logs))
                all_logs.extend(logs)
                logger.debug(f"分页获取: page={page}, 本页={len(logs)}, 累计={len(all_logs)}/{total}")
                if len(all_logs) >= total or len(logs) < per_page:
                    break
                page += 1
        logger.info(f"日志获取完成: 共 {len(all_logs)} 条")
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
        current_hour = int(now // 3600) * 3600
        start_ts = current_hour
        end_ts = int(now)
        hour_label = (
            time.strftime("%H:00", time.localtime(start_ts))
            + " – "
            + time.strftime("%H:%M %m/%d", time.localtime(end_ts))
        )
        return start_ts, end_ts, hour_label

    @staticmethod
    def _process_data(logs, start_ts, end_ts, time_range_hours=9):
        # 30分钟一档，按配置的时间范围计算档数
        num_slots = time_range_hours * 2
        current_hour = int(end_ts // 3600) * 3600
        range_start = current_hour - time_range_hours * 3600
        slots = [0] * num_slots
        slot_labels = []
        for i in range(num_slots):
            slot_start = range_start + i * 1800
            h = time.strftime("%H:%M", time.localtime(slot_start))
            slot_labels.append(h)

        model_stats = {}
        ip_stats = {}
        for log in logs:
            created_at = log.get("created_at", 0)
            if created_at < range_start or created_at >= end_ts:
                continue
            # 计算落在哪个slot
            slot_idx = int((created_at - range_start) // 1800)
            if 0 <= slot_idx < num_slots:
                slots[slot_idx] += 1
            model = log.get("model_name", "unknown")
            prompt_tok = int(log.get("prompt_tokens", 0) or 0)
            comp_tok = int(log.get("completion_tokens", 0) or 0)
            total_tok = prompt_tok + comp_tok
            if model not in model_stats:
                model_stats[model] = {"cnt": 0, "tok": 0}
            model_stats[model]["cnt"] += 1
            model_stats[model]["tok"] += total_tok
            ip = log.get("ip", "")
            username = log.get("username", "")
            if ip or username:
                label = f"{username}@{ip}" if username and ip else (username or ip)
                if label not in ip_stats:
                    ip_stats[label] = {"cnt": 0, "tok": 0}
                ip_stats[label]["cnt"] += 1
                ip_stats[label]["tok"] += total_tok
        models_sorted = sorted(model_stats.items(), key=lambda x: x[1]["tok"], reverse=True)[:10]
        ips_sorted = sorted(ip_stats.items(), key=lambda x: x[1]["tok"], reverse=True)[:10]
        return slots, slot_labels, models_sorted, ips_sorted

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
    def _build_html(slots, slot_labels, models_sorted, ips_sorted, sys_status, hour_label, newapi_status, time_range_hours=9, bg_image_path=None):
        total_req = sum(slots)
        total_tok = sum(m[1]["tok"] for m in models_sorted)
        peak = max(slots) if slots else 0
        slots_json = json.dumps(slots)
        labels_json = json.dumps(slot_labels)
        models_json = json.dumps(
            [{"name": m[0], "cnt": m[1]["cnt"], "tok": m[1]["tok"]} for m in models_sorted]
        )
        ips_json = json.dumps(
            [{"ip": ip[0], "cnt": ip[1]["cnt"], "tok": ip[1]["tok"]} for ip in ips_sorted]
        )
        procs_json = json.dumps(sys_status["procs"])
        newapi_cls = "ok" if newapi_status == "ok" else "warn"
        newapi_label = "Active" if newapi_status == "ok" else "Down"
        # 背景图样式
        bg_style = ""
        if bg_image_path:
            bg_uri = Path(bg_image_path).as_uri()
            bg_style = f"background-image:url('{bg_uri}');background-size:cover;background-position:center;"

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
    min-height:100vh;
  }}
  .left{{flex:1;min-width:0;display:flex;flex-direction:column;gap:10px}}
  .right{{width:390px;flex-shrink:0;display:flex;flex-direction:column;gap:10px}}
  .card{{
    background:rgba(255,255,255,0.78);
    border:1px solid rgba(255,182,193,0.25);
    border-radius:18px;
    padding:12px 15px;
    box-shadow:0 2px 12px rgba(255,182,193,0.12),0 1px 3px rgba(167,199,231,0.15);
    backdrop-filter:blur(8px);
    -webkit-backdrop-filter:blur(8px);
  }}
  h3{{font-size:13px;font-weight:700;margin-bottom:2px;letter-spacing:0.3px}}
  h3 .dot{{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;background:linear-gradient(135deg,#FFB6C1,#A7C7E7)}}
  h3 span.accent{{color:#FF9EBB;font-weight:700}}
  .sub{{font-size:10px;color:#94a3b8;margin-bottom:6px}}
  .chart{{display:flex;align-items:flex-end;gap:2px;height:130px;padding:2px 0}}
  .bar-col{{flex:1;display:flex;flex-direction:column;align-items:center;min-width:0;height:100%}}
  .bar-fill{{
    width:100%;max-width:14px;
    border-radius:4px 4px 2px 2px;
    margin-top:auto;min-height:2px;
    transition:height 0.3s ease;
  }}
  .bar-label{{font-size:6.5px;font-family:'Consolas','Courier New',monospace;color:#94a3b8;margin-top:2px}}
  .rank-list{{display:flex;flex-direction:column;gap:5px}}
  .rank-row{{display:flex;align-items:center;gap:6px;font-size:11px;padding:2px 0}}
  .rank-num{{
    width:20px;height:20px;border-radius:7px;
    font-size:9.5px;font-weight:700;color:#fff;
    display:flex;align-items:center;justify-content:center;flex-shrink:0;
    box-shadow:0 1px 4px rgba(0,0,0,0.10);
  }}
  .rn1{{background:linear-gradient(135deg,#FF9EBB,#FFB6C1);box-shadow:0 0 8px rgba(255,158,187,0.45)}}
  .rn2{{background:linear-gradient(135deg,#A7C7E7,#C9E4DE);box-shadow:0 0 6px rgba(167,199,231,0.4)}}
  .rn3{{background:linear-gradient(135deg,#FFD700,#FFA500);box-shadow:0 0 5px rgba(255,215,0,0.35)}}
  .rnn{{background:#E8ECF1;color:#94a3b8}}
  .rank-name{{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600;font-size:10px;color:#334155}}
  .rank-stat{{font-family:'Consolas','Courier New',monospace;font-size:10px;color:#64748b;white-space:nowrap}}
  .rank-stat b{{color:#FF9EBB;font-weight:700}}
  .col-header{{display:flex;align-items:center;gap:6px;font-size:9px;color:#94a3b8;padding-bottom:4px;border-bottom:1px solid rgba(255,182,193,0.2);margin-bottom:3px}}
  .ch-name{{flex:1}}.ch-stat{{width:68px;text-align:right}}
  .status-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px 10px;font-size:10.5px}}
  .status-item{{display:flex;justify-content:space-between;align-items:center}}
  .status-key{{color:#94a3b8;font-size:9.5px}}
  .status-val{{font-family:'Consolas','Courier New',monospace;font-weight:600;color:#334155;font-size:10px}}
  .status-val.ok{{color:#16a34a}}.status-val.warn{{color:#f59e0b}}.status-val.danger{{color:#ef4444}}
  .proc-list{{display:flex;flex-direction:column;gap:4px}}
  .proc-row{{display:flex;align-items:center;gap:5px;font-size:10px}}
  .proc-name{{width:80px;font-weight:600;color:#334155;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:10px}}
  .proc-bar{{flex:1;height:5px;background:rgba(167,199,231,0.18);border-radius:3px;overflow:hidden}}
  .pcpu{{height:100%;border-radius:3px;background:linear-gradient(90deg,#FFB6C1,#FF9EBB)}}
  .pmem{{height:100%;border-radius:3px;background:linear-gradient(90deg,#A7C7E7,#7EB8DA)}}
  .pstat{{font-family:'Consolas','Courier New',monospace;font-size:9px;color:#64748b;width:58px;text-align:right}}
  .summary{{display:flex;gap:14px;margin-top:8px;font-size:10px;color:#64748b;flex-wrap:wrap}}
  .summary b{{color:#FF9EBB;font-family:'Consolas','Courier New',monospace;font-weight:700}}
  .footer{{margin-top:auto;text-align:right;font-size:9px;color:#cbd5e1}}
  .footer span{{color:#FFB6C1;margin:0 2px}}
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
</div>
<div class="right">
  <div class="card">
    <h3><span class="dot"></span>模型 Token 排行</h3>
    <div class="sub">近{time_range_hours}小时 Token 消耗 Top 10</div>
    <div class="col-header"><span class="ch-name">模型</span><span class="ch-stat">Token</span><span class="ch-stat">调用</span></div>
    <div class="rank-list" id="model-rank"></div>
  </div>
  <div class="card">
    <h3><span class="dot"></span>用户 Token 排行</h3>
    <div class="sub">近{time_range_hours}小时 Token 消耗 Top 10</div>
    <div class="col-header"><span class="ch-name">用户</span><span class="ch-stat">Token</span><span class="ch-stat">调用</span></div>
    <div class="rank-list" id="ip-rank"></div>
  </div>
</div>
<script>
const SLOTS={slots_json};
const LABELS={labels_json};
const MODELS={models_json};
const IPS={ips_json};
const PROCS={procs_json};
const maxS=Math.max(...SLOTS,1);
const gradientColors=[
  '#FFB6C1','#FFA5B9','#FF94B0','#FF83A8','#FF729F',
  '#FF6197','#FF508E','#FF3F86','#FF2E7D','#FF1D75',
  '#A7C7E7','#98C0E4','#89B9E1','#7AB2DE','#6BABDB',
  '#5CA4D8','#4D9DD5','#3E96D2','#2F8FCF','#2588CC'
];
document.getElementById('chart').innerHTML=SLOTS.map((c,i)=>{{
  const pct=(c/maxS*100).toFixed(1);
  const level=Math.min(Math.floor(c/maxS*20),19);
  const color=c===0?'#f1f5f9':gradientColors[level];
  const hasGlow=c>=maxS*0.5;
  const label=i%2===0?LABELS[i]:'';
  return `<div class="bar-col"><div class="bar-fill" style="height:${{pct}}%;background:${{color}}${{hasGlow?';box-shadow:0 0 6px '+color+'66':''}}"></div><div class="bar-label">${{label}}</div></div>`;
}}).join('');
function ft(v){{if(v>=1e6)return(v/1e6).toFixed(2)+'M';if(v>=1e3)return(v/1e3).toFixed(1)+'K';return v;}}
document.getElementById('model-rank').innerHTML=MODELS.map((m,i)=>{{
  const rc=i===0?'rn1':i===1?'rn2':i===2?'rn3':'rnn';
  return `<div class="rank-row"><div class="rank-num ${{rc}}">${{i+1}}</div><div class="rank-name" title="${{m.name}}">${{m.name}}</div><div class="rank-stat"><b>${{ft(m.tok)}}</b></div><div class="rank-stat">${{m.cnt}}次</div></div>`;
}}).join('');
document.getElementById('ip-rank').innerHTML=IPS.map((ip,i)=>{{
  const rc=i===0?'rn1':i===1?'rn2':i===2?'rn3':'rnn';
  return `<div class="rank-row"><div class="rank-num ${{rc}}">${{i+1}}</div><div class="rank-name">${{ip.ip}}</div><div class="rank-stat"><b>${{ft(ip.tok)}}</b></div><div class="rank-stat">${{ip.cnt}}次</div></div>`;
}}).join('');
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
const maxPC=PROCS.length?Math.max(...PROCS.map(p=>p.cpu)):1;
const maxPM=PROCS.length?Math.max(...PROCS.map(p=>p.mem)):1;
document.getElementById('procs').innerHTML=PROCS.map(p=>{{
  const cw=(p.cpu/maxPC*100).toFixed(1);
  const mw=(p.mem/maxPM*100).toFixed(1);
  return `<div class="proc-row"><div class="proc-name">${{p.name}}</div><div class="pstat">CPU ${{p.cpu}}%</div><div class="proc-bar"><div class="pcpu" style="width:${{cw}}%"></div></div><div class="pstat">MEM ${{p.mem}}%</div><div class="proc-bar"><div class="pmem" style="width:${{mw}}%"></div></div></div>`;
}}).join('');
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
                        device_scale_factor=1,
                    )
                    await page.goto(
                        Path(html_path).as_uri(),
                        wait_until="commit",
                        timeout=15000,
                    )
                    await page.wait_for_timeout(2000)
                    await page.screenshot(
                        path=output_path, full_page=True, timeout=30000
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
        _, end_ts, hour_label = self._get_hour()
        # 按配置的时间范围拉取数据
        start_ts_range = int(end_ts // 3600) * 3600 - self.time_range_hours * 3600
        logs = await self._fetch_logs(start_ts_range, end_ts)
        logger.info(f"获取到 {len(logs)} 条日志（时间范围 {self.time_range_hours} 小时）")
        if logs:
            ca = logs[0].get('created_at', 0)
            if ca > 1e12:
                logger.warning("created_at 是毫秒级，需要转换！")
        slots, slot_labels, models_sorted, ips_sorted = self._process_data(
            logs, start_ts_range, end_ts, self.time_range_hours
        )
        sys_status = self._get_system_status()
        newapi_status = await self._check_newapi_status()
        html = self._build_html(
            slots, slot_labels, models_sorted, ips_sorted, sys_status,
            hour_label, newapi_status, self.time_range_hours, self._bg_cache_path
        )
        data_dir = StarTools.get_data_dir()
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
            if not self._bot or not self.group_ids:
                logger.warning("无法发送报表：bot 未初始化或群号未配置")
                return
            # 用 base64 发送，避免跨容器文件路径问题
            with open(png_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
            for gid in self.group_ids:
                try:
                    await self._bot.call_action(
                        "send_group_msg",
                        group_id=int(gid),
                        message=f"[CQ:image,file=base64://{img_b64}]",
                    )
                    logger.info(f"报表已发送到群 {gid}")
                except Exception as e:
                    logger.error(f"报表发送到群 {gid} 失败: {e}")
        finally:
            self._generating = False

    # ────────── 指令处理 ──────────

    @filter.command("报表")
    async def report_cmd(self, event: AstrMessageEvent):
        """手动生成 New-API 监控报表"""
        if isinstance(event, AiocqhttpMessageEvent) and self._bot is None:
            self._bot = event.bot
        if not self.newapi_url or not self.newapi_token:
            yield event.plain_result("请先在插件配置中填写 New-API 地址和系统访问令牌")
            return
        try:
            png_path = await self._generate_report()
            yield event.image_result(png_path)
        except Exception as e:
            logger.error(f"报表生成失败: {e}", exc_info=True)
            yield event.plain_result(f"报表生成失败: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_group(self, event: AiocqhttpMessageEvent):
        """存储 bot 引用，供定时报表使用"""
        if self._bot is None:
            self._bot = event.bot
