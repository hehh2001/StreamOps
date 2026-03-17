#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import posixpath
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = Path(os.environ.get('STREAM_WATCH_CONFIG', str(BASE_DIR / 'config.yml')))
ASSET_DIR = Path(os.environ.get('STREAM_WATCH_ASSETS_DIR', str(BASE_DIR / 'assets')))
JOBS_FILE = Path(os.environ.get('STREAM_WATCH_JOBS', str(BASE_DIR / 'runtime' / 'jobs.json')))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('stream-watch')


def load_config() -> dict:
    cfg = {}
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    cfg.setdefault('playback', {})
    cfg.setdefault('srs', {})
    cfg['playback'].setdefault('port', 7000)
    cfg['playback'].setdefault('watch_path_prefix', 'watch')
    cfg['playback'].setdefault('flv_path_prefix', 'flv')
    cfg['playback'].setdefault('hls_path_prefix', 'hls')
    cfg['playback'].setdefault('flv_app', cfg['srs'].get('origin_app', 'live'))
    cfg['playback'].setdefault('hls_app', cfg['srs'].get('origin_app', 'live'))
    cfg['playback'].setdefault('hls_edge_host', '')
    cfg['playback'].setdefault('hls_edge_port', 18080)
    cfg['playback'].setdefault('hls_edges', [])
    cfg['playback'].setdefault('control_api_base', 'http://127.0.0.1:18080')
    cfg['srs'].setdefault('edges', [])
    return cfg


CONFIG = load_config()
EDGES = []
for edge in CONFIG['srs']['edges']:
    parsed = urllib.parse.urlsplit(str(edge))
    host = parsed.hostname or str(edge).replace('http://', '').replace('https://', '').split(':', 1)[0]
    EDGES.append(f'{host}:8080')
# Local-only monitor backend. Keep this on loopback so the raw FLV service is not internet-exposed.
LISTEN = ('127.0.0.1', 17070)
WATCH_PATH_PREFIX = '/' + str(CONFIG['playback']['watch_path_prefix']).strip('/')
FLV_PATH_PREFIX = '/' + str(CONFIG['playback']['flv_path_prefix']).strip('/')
HLS_PATH_PREFIX = '/' + str(CONFIG['playback']['hls_path_prefix']).strip('/')
FLV_APP = str(CONFIG['playback']['flv_app']).strip('/')
HLS_APP = str(CONFIG['playback']['hls_app']).strip('/')
EDGE_CACHE_TTL = 120
STREAM_EDGE_CACHE = {}
HLS_RESPONSE_CACHE = {}
ACTIVE_FLV_VIEWERS = {}
ACTIVE_HLS_VIEWERS = {}
LOCK = threading.Lock()
HLS_EDGE_PORT = int(CONFIG['playback'].get('hls_edge_port', 18080))
CONTROL_API_BASE = str(CONFIG['playback'].get('control_api_base', 'http://127.0.0.1:18080')).rstrip('/')
HLS_EDGES = []
for edge in CONFIG['playback'].get('hls_edges', []) or []:
    parsed = urllib.parse.urlsplit(str(edge))
    host = parsed.hostname or str(edge).replace('http://', '').replace('https://', '').split(':', 1)[0]
    HLS_EDGES.append(f'{host}:{HLS_EDGE_PORT}')
if not HLS_EDGES:
    HLS_EDGES = [edge.rsplit(':', 1)[0] + f':{HLS_EDGE_PORT}' for edge in EDGES]
HLS_EDGE_HOST = str(CONFIG['playback'].get('hls_edge_host', '')).strip()
if HLS_EDGE_HOST and f'{HLS_EDGE_HOST}:{HLS_EDGE_PORT}' not in HLS_EDGES:
    HLS_EDGES.append(f'{HLS_EDGE_HOST}:{HLS_EDGE_PORT}')

PLAYBACK_SCHEME = str(CONFIG['playback'].get('scheme', 'http')).strip() or 'http'
PLAYBACK_HOST = str(CONFIG['playback'].get('host', '')).strip()
PLAYBACK_PORT = int(CONFIG['playback'].get('port', 7000))

def playback_public_base() -> str:
    if not PLAYBACK_HOST:
        return ''
    default_port = 80 if PLAYBACK_SCHEME == 'http' else 443
    if PLAYBACK_PORT == default_port:
        return f'{PLAYBACK_SCHEME}://{PLAYBACK_HOST}'
    return f'{PLAYBACK_SCHEME}://{PLAYBACK_HOST}:{PLAYBACK_PORT}'

PUBLIC_PLAYBACK_BASE = playback_public_base()


def load_jobs() -> dict:
    if not JOBS_FILE.exists():
        return {}
    try:
        with open(JOBS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning('load jobs failed: %s', exc)
        return {}


def watch_stats(stream_name: str) -> dict:
    stream_name = (stream_name or '').strip()
    stats = {
        'stream_name': stream_name,
        'viewer_count': 0,
        'rtmp_users': 0,
        'flv_viewers': 0,
        'mobile_hls_viewers': 0,
        'web_viewers': 0,
        'viewer_updated_at': None,
        'status': 'missing',
        'found': False,
    }
    if not stream_name:
        return stats
    stats['flv_viewers'] = count_active_flv_viewers(stream_name)
    stats['mobile_hls_viewers'] = count_active_mobile_hls_viewers(stream_name)
    stats['web_viewers'] = stats['flv_viewers'] + stats['mobile_hls_viewers']
    for job in load_jobs().values():
        if job.get('pull_name') != stream_name:
            continue
        edge_stats = job.get('edge_stats') or []
        rtmp_users = sum(int((item or {}).get('rtmp_users', 0) or 0) for item in edge_stats)
        stats.update({
            'rtmp_users': rtmp_users,
            'viewer_updated_at': job.get('viewer_updated_at'),
            'status': job.get('status', ''),
            'found': True,
        })
        stats['viewer_count'] = stats['rtmp_users'] + stats['web_viewers']
        return stats
    return stats


def viewer_identity(handler: BaseHTTPRequestHandler) -> str:
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(handler.path).query, keep_blank_values=False)
    sid = (query.get('sid') or [''])[0].strip()
    if sid:
        return sid[:80]
    return client_ip(handler) or 'unknown'


def mark_viewer(bucket: dict, stream_name: str, handler: BaseHTTPRequestHandler) -> None:
    if not stream_name:
        return
    identity = viewer_identity(handler)
    if not identity:
        return
    key = f'{stream_name}|{identity}'
    with LOCK:
        bucket[key] = now_ts()


def count_active_viewers(bucket: dict, stream_name: str, ttl: int = 35) -> int:
    if not stream_name:
        return 0
    deadline = now_ts() - ttl
    active = 0
    prefix = f'{stream_name}|'
    with LOCK:
        stale = [key for key, ts in bucket.items() if ts < deadline]
        for key in stale:
            bucket.pop(key, None)
        for key, ts in bucket.items():
            if key.startswith(prefix) and ts >= deadline:
                active += 1
    return active


def mark_web_viewer(stream_name: str, handler: BaseHTTPRequestHandler) -> None:
    mark_viewer(ACTIVE_FLV_VIEWERS, stream_name, handler)


def mark_mobile_hls_viewer(stream_name: str, handler: BaseHTTPRequestHandler) -> None:
    mark_viewer(ACTIVE_HLS_VIEWERS, stream_name, handler)


def count_active_flv_viewers(stream_name: str, ttl: int = 35) -> int:
    return count_active_viewers(ACTIVE_FLV_VIEWERS, stream_name, ttl)


def count_active_mobile_hls_viewers(stream_name: str, ttl: int = 35) -> int:
    return count_active_viewers(ACTIVE_HLS_VIEWERS, stream_name, ttl)


def request_mobile_hls(stream_name: str) -> None:
    if not stream_name or not CONTROL_API_BASE:
        return
    try:
        req = urllib.request.Request(
            f'{CONTROL_API_BASE}/internal/mobile-hls/ensure/{urllib.parse.quote(stream_name)}',
            method='POST',
        )
        urllib.request.urlopen(req, timeout=2).read(0)
    except Exception:
        return


def is_mobile_user_agent(ua: str) -> bool:
    ua = ua or ''
    return any(token in ua for token in (
        'Android', 'webOS', 'iPhone', 'iPad', 'iPod', 'BlackBerry', 'IEMobile', 'Opera Mini'
    ))

WATCH_HTML = '''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>直播监看</title>
<style>
:root{--bg:#06111d;--bg2:#0b1e34;--panel:rgba(8,17,30,.78);--panel-strong:rgba(5,11,20,.9);--line:rgba(255,255,255,.08);--text:#eef5ff;--muted:#9db0c9;--accent:#79e3c3;--accent-2:#ffd680;--ok:#9af0d2;--warn:#ffd27a;--err:#ffb7b7}
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;min-height:100%;color:var(--text);font-family:"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;background:radial-gradient(circle at 18% 18%, rgba(75,146,255,.18), transparent 26%),radial-gradient(circle at 82% 12%, rgba(121,227,195,.16), transparent 22%),linear-gradient(135deg,var(--bg2),#06111d 56%, #03070d 100%)}
body::before{content:"";position:fixed;inset:18px;border:1px solid rgba(255,255,255,.05);border-radius:30px;pointer-events:none}
.page{min-height:100vh;padding:20px}
.layout{max-width:1480px;height:calc(100vh - 40px);margin:0 auto;display:grid;grid-template-columns:minmax(0,1.72fr) minmax(320px,.78fr);gap:18px;align-items:stretch}
.panel{min-height:0;border:1px solid var(--line);border-radius:28px;background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.025));box-shadow:0 26px 80px rgba(0,0,0,.32);backdrop-filter:blur(14px)}
.stage{display:grid;grid-template-rows:auto auto 1fr auto;gap:14px;padding:20px 20px 18px}
.stage-head{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap}
.badge{display:inline-flex;align-items:center;gap:10px;padding:9px 14px;border-radius:999px;background:rgba(121,227,195,.12);border:1px solid rgba(121,227,195,.18);color:var(--accent);font-size:12px;font-weight:700;letter-spacing:.14em;text-transform:uppercase}
.badge-dot{width:8px;height:8px;border-radius:50%;background:var(--accent);box-shadow:0 0 16px rgba(121,227,195,.9)}
.view-pill{padding:9px 12px;border-radius:999px;background:rgba(3,8,14,.6);border:1px solid rgba(255,255,255,.08);color:#d6e5fa;font-size:12px}
.hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:14px;align-items:end}
.title{margin:0;font-size:clamp(22px,2.3vw,32px);line-height:1.14;font-weight:700;letter-spacing:.02em}
.title-main,.title-sub{display:block}
.title-sub{margin-top:4px;font-size:.82em;color:#d9e7fb}
.subtitle{margin:8px 0 0;color:var(--muted);font-size:14px;line-height:1.75;max-width:60ch}
.kpis{display:flex;gap:12px;flex-wrap:wrap;justify-content:flex-end}
.kpi{min-width:132px;padding:12px 14px;border-radius:18px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06)}
.kpi-label{display:block;margin-bottom:6px;color:#89a4c6;font-size:11px;letter-spacing:.12em;text-transform:uppercase}
.kpi-value{display:block;color:var(--text);font-size:14px;font-weight:600}
.kpi-value strong{display:block;font-size:22px;line-height:1.1}
.player-shell{position:relative;min-height:0;border-radius:24px;overflow:hidden;background:#000;border:1px solid rgba(255,255,255,.08)}
.player-shell::before{content:"";display:block;padding-top:56.25%}
video{position:absolute;inset:0;width:100%;height:100%;display:block;background:#000;object-fit:contain}
.overlay{position:absolute;left:16px;right:88px;bottom:64px;display:flex;justify-content:flex-start;pointer-events:none;opacity:0;transform:translateY(6px);transition:opacity .18s ease,transform .18s ease}
.overlay.visible{opacity:1;transform:translateY(0)}
.overlay-chip{max-width:min(60%,420px);padding:6px 10px;border-radius:999px;background:rgba(3,8,14,.52);border:1px solid rgba(255,255,255,.08);backdrop-filter:blur(10px);color:#d7e4f8;font-size:11px;line-height:1.35;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.meta{display:grid;grid-template-columns:1.05fr 1.25fr .9fr;gap:12px}
.meta-card{min-width:0;padding:14px 16px;border-radius:20px;background:rgba(6,12,20,.54);border:1px solid rgba(255,255,255,.06)}
.meta-label{display:block;margin-bottom:6px;font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#86a1c0}
.meta-value{font-size:14px;line-height:1.6;color:var(--text);word-break:break-all}
.mono{font-family:"SFMono-Regular","Consolas","Liberation Mono",monospace}
.status{display:flex;align-items:center;gap:10px}
.status-dot{width:10px;height:10px;border-radius:50%;flex:0 0 auto;background:var(--warn);box-shadow:0 0 12px rgba(255,210,122,.75)}
.status-dot.ok{background:var(--ok);box-shadow:0 0 12px rgba(154,240,210,.85)}
.status-dot.err{background:var(--err);box-shadow:0 0 12px rgba(255,183,183,.85)}
.status-text{font-size:13px;line-height:1.5;color:#dbe8fb}
.error{min-height:20px;color:var(--err);font-size:13px;line-height:1.55;white-space:pre-wrap}
.side{display:grid;grid-template-rows:auto auto auto 1fr auto auto;gap:14px;padding:20px;background:linear-gradient(180deg,rgba(6,12,20,.88),rgba(8,16,28,.84))}
.side h2{margin:0;font-size:22px;line-height:1.1}
.side-copy{margin:0;color:var(--muted);font-size:14px;line-height:1.75}
.info-grid,.service-list{display:grid;gap:10px}
.info-card{padding:14px 15px;border-radius:18px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06)}
.info-card strong{display:block;margin-bottom:6px;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent-2)}
.info-card span{display:block;font-size:14px;line-height:1.65;color:#d7e3f7}
.service-item{padding:13px 14px;border-radius:18px;background:linear-gradient(180deg,rgba(121,227,195,.10),rgba(121,227,195,.04));border:1px solid rgba(121,227,195,.12);font-size:13px;line-height:1.65;color:#dcf7ef}
.actions{display:flex;gap:10px;flex-wrap:wrap}
.action{display:inline-flex;align-items:center;justify-content:center;padding:11px 14px;border-radius:999px;text-decoration:none;font-size:13px;font-weight:600;border:1px solid rgba(255,255,255,.1);color:var(--text);background:rgba(255,255,255,.04)}
.action.primary{background:rgba(121,227,195,.12);border-color:rgba(121,227,195,.2);color:var(--accent)}
.contact{padding:16px 18px;border-radius:22px;background:linear-gradient(135deg,rgba(255,214,128,.12),rgba(255,255,255,.04));border:1px solid rgba(255,214,128,.16)}
.contact-label{display:block;margin-bottom:8px;font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--accent-2)}
.contact-phone{font-size:23px;font-weight:700;letter-spacing:.02em}
.footer{color:#8ea5c2;font-size:12px;line-height:1.7}
@media (max-width: 1180px){
  body::before{display:none}
  .page{padding:14px}
  .layout{height:auto;min-height:calc(100vh - 28px);grid-template-columns:1fr}
  .hero{grid-template-columns:1fr}
  .kpis{justify-content:flex-start}
}
@media (max-width: 760px){
  .stage,.side{padding:16px}
  .panel{border-radius:22px}
  .meta{grid-template-columns:1fr}
  .overlay{left:10px;right:10px;bottom:56px}
  .title{font-size:25px}
  .contact-phone{font-size:20px}
  .action{width:100%}
}
</style>
</head>
<body>
<div class="page">
  <div class="layout">
    <section class="panel stage">
      <div class="stage-head">
        <div class="badge"><span class="badge-dot"></span>Live Monitor</div>
        <div class="view-pill" id="view-mode">统一监看入口</div>
      </div>
      <div class="hero">
        <div>
          <h1 class="title"><span class="title-main">Live Operations</span><span class="title-sub">Monitor Console</span></h1>
          <p class="subtitle">A reference monitor UI for live delivery workflows. It combines edge-aware playback, viewer counters, and automatic client-side protocol selection for desktop and mobile devices.</p>
        </div>
        <div class="kpis">
          <div class="kpi">
            <span class="kpi-label">总在线</span>
            <span class="kpi-value"><strong id="viewer-count">--</strong><span id="viewer-updated">等待统计</span></span>
          </div>
          <div class="kpi">
            <span class="kpi-label">播放优先级</span>
            <span class="kpi-value" id="protocol-prefer">桌面端与安卓端 FLV</span>
          </div>
          <div class="kpi">
            <span class="kpi-label">恢复策略</span>
            <span class="kpi-value">自动重试已启用</span>
          </div>
        </div>
      </div>
      <div class="player-shell">
        <video id="video" controls playsinline webkit-playsinline x5-playsinline x-webkit-airplay="allow" preload="auto" muted></video>
        <div class="overlay" id="overlay">
          <div class="overlay-chip" id="overlay-tip">等待播放器初始化</div>
        </div>
      </div>
      <div class="meta">
        <div class="meta-card">
          <span class="meta-label">Service Scope</span>
          <div class="meta-value" id="stream-name"></div>
        </div>
        <div class="meta-card">
          <span class="meta-label">Playback Entry</span>
          <div class="meta-value mono" id="play-url"></div>
        </div>
        <div class="meta-card">
          <span class="meta-label">当前状态</span>
          <div class="status"><span class="status-dot" id="status-dot"></span><div class="status-text" id="status-text">等待播放</div></div>
          <div class="error" id="error"></div>
        </div>
      </div>
    </section>
    <aside class="panel side">
      <h2>Overview</h2>
      <p class="side-copy">This template is intended for broadcasters, event operators, and managed streaming teams that need a single monitor URL with adaptive playback behavior.</p>
      <div class="info-grid">
        <div class="info-card"><strong>Use Cases</strong><span>Suitable for multi-edge distribution, low-latency monitoring, mobile fallback playback, and operations dashboards that aggregate viewer counts from separate delivery planes.</span></div>
      </div>
      <div class="service-list">
        <div class="service-item">Supports RTMP ingest, HTTP-FLV monitoring, mobile HLS fallback, and edge-aware playback routing from one watch page.</div>
        <div class="service-item">Designed to plug into an external control plane for stream metadata, viewer aggregation, edge selection, and custom transport workflows.</div>
      </div>
      <div class="actions">
        <a class="action primary" id="open-hls" href="#">打开监看地址</a>
      </div>
      <div class="contact">
        <span class="contact-label">Deployment</span>
        <div class="contact-phone">See README for configuration and rollout steps</div>
      </div>
      <div class="footer">Sanitized reference build</div>
    </aside>
  </div>
</div>
<script src="/assets/flv.js"></script>
<script>
(function(){
const watchPathPrefix='__WATCH_PATH_PREFIX__'
const flvPathPrefix='__FLV_PATH_PREFIX__'
const hlsPathPrefix='__HLS_PATH_PREFIX__'
const flvApp='__FLV_APP__'
const hlsApp='__HLS_APP__'
const playbackBase=('__PUBLIC_PLAYBACK_BASE__'.endsWith('/') ? '__PUBLIC_PLAYBACK_BASE__'.slice(0, -1) : '__PUBLIC_PLAYBACK_BASE__')
const video=document.getElementById('video')
const errorBox=document.getElementById('error')
const streamNameBox=document.getElementById('stream-name')
const playUrlBox=document.getElementById('play-url')
const statusDot=document.getElementById('status-dot')
const statusText=document.getElementById('status-text')
const overlay=document.getElementById('overlay')
const overlayTip=document.getElementById('overlay-tip')
const viewMode=document.getElementById('view-mode')
const preferBox=document.getElementById('protocol-prefer')
const openHls=document.getElementById('open-hls')
const viewerCountBox=document.getElementById('viewer-count')
const viewerUpdatedBox=document.getElementById('viewer-updated')
const statsBase=watchPathPrefix.endsWith('/') ? watchPathPrefix.slice(0, -1) : watchPathPrefix
const sessionId=(function(){
  try{
    const buf=new Uint32Array(4)
    crypto.getRandomValues(buf)
    return Array.from(buf, v => v.toString(16)).join('')
  }catch(e){
    return 'sid-' + Date.now().toString(16) + '-' + Math.random().toString(16).slice(2)
  }
})()
const segs=location.pathname.replace(/[/]+$/, '').split('/').filter(Boolean)
const stream=decodeURIComponent(segs.length>1 ? segs[1] : '')
if(!stream){
  errorBox.textContent=`地址格式应为 ${watchPathPrefix}/<stream_name>`
  statusText.textContent='地址格式错误'
  statusDot.className='status-dot err'
  overlayTip.textContent='无法解析流名'
  return
}
const relativeFlvPath=`${flvPathPrefix}/${flvApp}/${encodeURIComponent(stream)}.flv`
const relativeHlsPath=`${hlsPathPrefix}/${hlsApp}/${encodeURIComponent(stream)}.m3u8`
const displayBase=((playbackBase || location.origin || ''));
const displayFlv=`${displayBase}${relativeFlvPath}`
const displayHls=`${displayBase}${relativeHlsPath}`
streamNameBox.textContent='承接广告活动直播、品牌发布会、论坛峰会、赛事演出及媒体协同制作保障服务'
openHls.href=displayBase ? `${displayBase}${watchPathPrefix}/${encodeURIComponent(stream)}` : '#';
viewMode.textContent='桌面端监看'
preferBox.textContent='桌面端 HTTP-FLV'
playUrlBox.textContent=displayFlv
let flvPlayer=null
let hlsPlayer=null
let retryTimer=null
let healthTimer=null
let statsTimer=null
let retryCount=0
let isStarting=false
let lastHealthyAt=0
let currentMode='idle'
function formatViewerUpdated(ts){
  if(!ts){ return '等待统计' }
  try{
    return '更新于 ' + new Date(Number(ts) * 1000).toLocaleTimeString()
  }catch(e){
    return '等待统计'
  }
}
function formatViewerBreakdown(data){
  const rtmp=Number(data.rtmp_users||0)
  const web=Number(data.web_viewers||0)
  return `RTMP拉流 ${rtmp} / 网页监看 ${web}`
}
async function refreshViewerStats(){
  try{
    const r=await fetch(`${statsBase}/stats/${encodeURIComponent(stream)}?ts=${Date.now()}`, {cache:'no-store'})
    if(!r.ok) throw new Error('stats-http-' + r.status)
    const data=await r.json()
    viewerCountBox.textContent=`${Number(data.viewer_count||0)} 人`
    viewerUpdatedBox.textContent=`${formatViewerBreakdown(data)} · ${formatViewerUpdated(data.viewer_updated_at)}`
  }catch(e){
    viewerUpdatedBox.textContent='统计暂不可用'
  }
}
function setStatus(text, kind, detail){
  statusText.textContent=text
  const tip=detail || text
  overlayTip.textContent=tip
  const shouldShow=kind === 'err' || text === '播放恢复中' || text === '缓冲恢复中' || text === '缓冲中' || text === '等待用户操作' || text === '浏览器不支持'
  overlay.classList.toggle('visible', shouldShow)
  statusDot.className='status-dot' + (kind ? ' ' + kind : '')
}
function clearPlayers(){
  if(healthTimer){ clearInterval(healthTimer); healthTimer=null }
  if(flvPlayer){
    try{flvPlayer.pause()}catch(e){}
    try{flvPlayer.unload()}catch(e){}
    try{flvPlayer.detachMediaElement()}catch(e){}
    try{flvPlayer.destroy()}catch(e){}
    flvPlayer=null
  }
  if(hlsPlayer){
    try{hlsPlayer.stopLoad()}catch(e){}
    try{hlsPlayer.detachMedia()}catch(e){}
    try{hlsPlayer.destroy()}catch(e){}
    hlsPlayer=null
  }
}
function resetMedia(){
  clearPlayers()
  try{video.pause()}catch(e){}
  video.removeAttribute('src')
  try{video.load()}catch(e){}
}
function relativeHls(){
  const q=`ts=${Date.now()}&sid=${encodeURIComponent(sessionId)}`
  return relativeHlsPath + (relativeHlsPath.includes('?') ? '&' : '?') + q
}
function relativeFlv(){
  const q=`ts=${Date.now()}&sid=${encodeURIComponent(sessionId)}`
  return relativeFlvPath + (relativeFlvPath.includes('?') ? '&' : '?') + q
}
function armHealthCheck(){
  if(healthTimer) clearInterval(healthTimer)
  healthTimer=setInterval(()=>{
    if(document.visibilityState !== 'visible') return
    if(video.readyState >= 3 && !video.paused){
      lastHealthyAt=Date.now()
      return
    }
    if(lastHealthyAt && Date.now() - lastHealthyAt > 10000 && !isStarting){
      scheduleRetry(currentMode + '-timeout')
    }
  }, 2500)
}
function onPlaying(modeLabel){
  retryCount=0
  isStarting=false
  lastHealthyAt=Date.now()
  errorBox.textContent=''
  setStatus('正在播放', 'ok', modeLabel)
  overlay.classList.remove('visible')
  armHealthCheck()
}
function scheduleRetry(reason){
  if(retryTimer || isStarting) return
  retryCount += 1
  const delay=Math.min(9000, 1600 + Math.min(retryCount, 8) * 650)
  errorBox.textContent='正在重试播放: ' + reason + ' (第' + retryCount + '次)'
  setStatus('播放恢复中', '', '正在尝试重新建立监看连接')
  retryTimer=setTimeout(()=>{
    retryTimer=null
    startPlayback()
  }, delay)
}
function playVideo(modeLabel){
  return video.play().then(()=>{
    onPlaying(modeLabel)
  }).catch(()=>{
    isStarting=false
    setStatus('等待用户操作', '', '浏览器阻止自动播放，点击播放器继续')
    errorBox.textContent='监看地址已就绪，点击播放按钮即可开始观看'
  })
}
function startFlv(preferred){
  if(!(window.flvjs && flvjs.isSupported())) return false
  currentMode='http-flv'
  const desktopConfig={enableStashBuffer:true,stashInitialSize:384,autoCleanupSourceBuffer:true,autoCleanupMaxBackwardDuration:15,autoCleanupMinBackwardDuration:8,fixAudioTimestampGap:false}
  setStatus('正在载入 FLV', '', preferred ? '桌面端正在建立低延迟监看连接' : '正在尝试恢复 FLV 播放')
  flvPlayer=flvjs.createPlayer({type:'flv',url:relativeFlv(),isLive:true,hasAudio:true,hasVideo:true},desktopConfig)
  flvPlayer.attachMediaElement(video)
  flvPlayer.load()
  flvPlayer.play().then(()=>{ onPlaying(preferred ? '桌面端 HTTP-FLV' : 'FLV 回退播放') }).catch(()=>{
    isStarting=false
    scheduleRetry(preferred ? 'flv-autoplay' : 'flv-fallback-autoplay')
  })
  flvPlayer.on(flvjs.Events.ERROR, (type, detail)=>{
    isStarting=false
    scheduleRetry(type + ' / ' + detail)
  })
  flvPlayer.on(flvjs.Events.LOADING_COMPLETE, ()=>scheduleRetry('flv-loading-complete'))
  return true
}
function startPlayback(){
  if(isStarting) return
  isStarting=true
  if(retryTimer){ clearTimeout(retryTimer); retryTimer=null }
  resetMedia()
  isStarting=true
  if(startFlv(true)){ return }
  isStarting=false
  setStatus('浏览器不支持', 'err', '当前终端无法建立网页内播放')
  errorBox.textContent='当前浏览器不支持网页内监看，请改用系统播放器或更换浏览器'
}
video.addEventListener('loadedmetadata', ()=>{ lastHealthyAt=Date.now() })
video.addEventListener('playing', ()=>{ lastHealthyAt=Date.now() })
video.addEventListener('timeupdate', ()=>{ lastHealthyAt=Date.now() })
video.addEventListener('error', ()=>{
  if(!isStarting) scheduleRetry(currentMode || 'video-error')
})
video.addEventListener('stalled', ()=>{
  if(!isStarting) scheduleRetry(currentMode || 'video-stalled')
})
video.addEventListener('waiting', ()=>{
  if(video.readyState === 0 && !isStarting) scheduleRetry(currentMode || 'video-waiting')
})
document.addEventListener('visibilitychange', ()=>{
  if(document.visibilityState === 'visible' && video.paused && !isStarting){
    startPlayback()
  }
})
setStatus('等待播放', '', '监看页已加载，准备初始化播放器')
refreshViewerStats()
statsTimer=setInterval(refreshViewerStats, 5000)
startPlayback()
})()
</script>
</body>
</html>
'''
MOBILE_WATCH_HTML = '''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>手机监看</title>
<style>
:root{--bg:#08111f;--bg-soft:#0d1d35;--panel:#10213d;--line:rgba(255,255,255,.10);--text:#f3f5fb;--muted:#c3d0e6;--accent:#ffd48a;--accent-soft:#f29b75}
*{box-sizing:border-box}
body{margin:0;color:var(--text);font-family:"Noto Serif SC","Source Han Serif SC","Songti SC",serif;background:radial-gradient(circle at top,#17345a 0,#08111f 48%,#050b14 100%) fixed}
.wrap{max-width:760px;margin:0 auto;padding:16px}
.shell{display:grid;grid-template-columns:1fr;gap:16px}
.card{background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));border:1px solid var(--line);border-radius:24px;box-shadow:0 18px 60px rgba(0,0,0,.30);backdrop-filter:blur(10px)}
.stage,.side{padding:16px}
.brand{display:inline-flex;align-items:center;gap:10px;padding:9px 16px;border-radius:999px;background:rgba(255,212,138,.12);color:var(--accent);font-size:20px;font-weight:600;letter-spacing:.06em}
h1{margin:14px 0 8px;font-size:20px;line-height:1.18;font-weight:600}
.lede,.copy{margin:0 0 12px;color:var(--muted);font-size:14px;line-height:1.72}
.meta{display:grid;grid-template-columns:1fr;gap:10px;margin-bottom:14px}
.meta-item{padding:12px 14px;border-radius:18px;background:rgba(5,11,20,.38);border:1px solid rgba(255,255,255,.06)}
.meta-label{display:block;margin-bottom:6px;font-size:12px;color:#97aac8;letter-spacing:.08em;text-transform:uppercase}
.meta-value{font-size:14px;color:var(--text);word-break:break-all}
.metric{display:flex;align-items:flex-end;gap:10px;flex-wrap:wrap}
.metric strong{font-size:30px;line-height:1;font-weight:600}
.metric span{font-size:13px;color:var(--muted)}
video{display:block;width:100%;background:#000;border-radius:20px;min-height:240px;box-shadow:0 10px 30px rgba(0,0,0,.28)}
.action{display:inline-flex;align-items:center;justify-content:center;margin-top:10px;padding:10px 14px;border-radius:999px;background:rgba(255,212,138,.14);border:1px solid rgba(255,212,138,.25);color:var(--accent);text-decoration:none}
.tip{margin-top:10px;color:#ffcfb0;font-size:13px;line-height:1.7}
.side h2{margin:0 0 10px;font-size:22px;font-weight:600}
.list{display:grid;gap:10px;margin:12px 0}
.pill{padding:12px 14px;border-radius:18px;background:linear-gradient(180deg,rgba(242,155,117,.12),rgba(255,212,138,.06));border:1px solid rgba(255,212,138,.18);font-size:13px;line-height:1.62}
.contact{margin-top:14px;padding:16px;border-radius:20px;background:rgba(5,11,20,.42);border:1px solid rgba(255,255,255,.08)}
.contact-title{margin:0 0 8px;color:var(--accent);font-size:13px;letter-spacing:.08em;text-transform:uppercase}
.contact-phone{font-size:26px;font-weight:600;letter-spacing:.04em}
.foot{margin-top:14px;color:#93a5c2;font-size:13px;line-height:1.8}
</style>
</head>
<body>
<div class="wrap">
  <div class="shell">
    <section class="card stage">
      <div class="brand">Live Operations Toolkit</div>
      <h1>Mobile Monitor Playback</h1>
      <p class="lede">A compact mobile watch page for live operations teams, with long-running HLS playback, aggregated viewer counts, and a direct handoff to the system player when required.</p>
      <div class="meta">
        <div class="meta-item">
          <span class="meta-label">总在线</span>
          <div class="meta-value metric"><strong id="viewer-count">--</strong><span id="viewer-updated">等待统计</span></div>
        </div>
      </div>
      <video controls autoplay muted playsinline webkit-playsinline preload="metadata"></video>
      <a class="action" id="open-native-player" href="__HLS_SRC__">打开系统播放器</a>
      <div class="tip" id="play-tip">正在准备移动端 HLS 监看链路，请稍候。</div>
    </section>
    <aside class="card side">
      <h2>Overview</h2>
      <p class="copy">Built for production teams that need a stable mobile monitoring surface while keeping desktop playback on lower-latency protocols.</p>
      <div class="list">
        <div class="pill">Combines RTMP ingest visibility, mobile-safe HLS playback, and edge-aware monitor delivery behind a single watch URL.</div>
        <div class="pill">Intended to integrate with custom control APIs for stream state, viewer statistics, edge selection, and operational automation.</div>
      </div>
      <div class="contact">
        <div class="contact-title">Deployment</div>
        <div class="contact-phone">See README for integration details</div>
      </div>
      <div class="foot">Sanitized reference build</div>
    </aside>
  </div>
</div>
<script src="/assets/hls.js"></script>
<script>
(function(){
const stream='__STREAM_NAME__'
const hlsSrc='__HLS_SRC__'
const viewerCountBox=document.getElementById('viewer-count')
const viewerUpdatedBox=document.getElementById('viewer-updated')
const video=document.querySelector('video')
const openNativePlayer=document.getElementById('open-native-player')
const playTip=document.getElementById('play-tip')
const sessionId=(function(){
  try{
    const buf=new Uint32Array(4)
    crypto.getRandomValues(buf)
    return Array.from(buf, v => v.toString(16)).join('')
  }catch(e){
    return 'sid-' + Date.now().toString(16) + '-' + Math.random().toString(16).slice(2)
  }
})()
const hlsWithSid=hlsSrc + (hlsSrc.includes('?') ? '&' : '?') + 'sid=' + encodeURIComponent(sessionId)
const canNativeHls=!!video.canPlayType('application/vnd.apple.mpegurl')
let hlsPlayer=null
let retryTimer=null
let retryCount=0
function formatViewerUpdated(ts){
  if(!ts){ return '等待统计' }
  try{
    return '更新于 ' + new Date(Number(ts) * 1000).toLocaleTimeString()
  }catch(e){
    return '等待统计'
  }
}
async function refreshViewerStats(){
  try{
    const r=await fetch(`/watch/stats/${encodeURIComponent(stream)}?ts=${Date.now()}`, {cache:'no-store'})
    if(!r.ok) throw new Error('stats-http-' + r.status)
    const data=await r.json()
    viewerCountBox.textContent=`${Number(data.viewer_count||0)} 人`
    viewerUpdatedBox.textContent=`RTMP拉流 ${Number(data.rtmp_users||0)} / 网页监看 ${Number(data.web_viewers||0)} · ${formatViewerUpdated(data.viewer_updated_at)}`
  }catch(e){
    viewerUpdatedBox.textContent='统计暂不可用'
  }
}
function scheduleRetry(reason){
  if(retryTimer) return
  retryCount += 1
  playTip.textContent=`监看链路重试中：${reason}（第${retryCount}次）`
  retryTimer=setTimeout(()=>{
    retryTimer=null
    startPlayback()
  }, Math.min(8000, 1200 + retryCount * 700))
}
function attachNative(){
  playTip.textContent='正在使用原生 HLS 建立移动端监看连接。'
  video.src=hlsWithSid
  video.load()
  video.play().then(()=>{
    retryCount=0
    playTip.textContent=''
  }).catch(()=>{
    playTip.textContent='请轻触播放按钮继续观看。'
  })
}
function attachHlsJs(){
  if(!(window.Hls && Hls.isSupported())) return false
  playTip.textContent='正在使用 HLS.js 建立移动端监看连接。'
  if(hlsPlayer){
    try{hlsPlayer.destroy()}catch(e){}
    hlsPlayer=null
  }
  hlsPlayer = new Hls({
    lowLatencyMode: false,
    backBufferLength: 6,
    maxBufferLength: 10,
    liveSyncDurationCount: 3,
    liveMaxLatencyDurationCount: 6,
    manifestLoadingTimeOut: 8000,
    fragLoadingTimeOut: 10000,
  })
  hlsPlayer.loadSource(hlsWithSid)
  hlsPlayer.attachMedia(video)
  hlsPlayer.on(Hls.Events.MANIFEST_PARSED, ()=>{
    video.play().then(()=>{
      retryCount=0
      playTip.textContent=''
    }).catch(()=>{
      playTip.textContent='请轻触播放按钮继续观看。'
    })
  })
  hlsPlayer.on(Hls.Events.ERROR, (_, data)=>{
    if(data && data.fatal){
      scheduleRetry(data.type || 'hls-error')
    }
  })
  return true
}
function startPlayback(){
  if(canNativeHls){
    attachNative()
    return
  }
  if(attachHlsJs()){
    return
  }
  playTip.textContent='当前手机浏览器不支持网页内 HLS，请改用系统播放器。'
}
openNativePlayer.href=hlsWithSid
video.addEventListener('playing', ()=>{ retryCount=0; playTip.textContent='' })
video.addEventListener('stalled', ()=>scheduleRetry('buffering'))
video.addEventListener('error', ()=>scheduleRetry('video-error'))
refreshViewerStats()
setInterval(refreshViewerStats, 5000)
startPlayback()
})()
</script>
</body>
</html>
'''
WATCH_HTML = (WATCH_HTML
    .replace('__WATCH_PATH_PREFIX__', WATCH_PATH_PREFIX)
    .replace('__FLV_PATH_PREFIX__', FLV_PATH_PREFIX)
    .replace('__HLS_PATH_PREFIX__', HLS_PATH_PREFIX)
    .replace('__FLV_APP__', FLV_APP)
    .replace('__HLS_APP__', HLS_APP)
    .replace('__PUBLIC_PLAYBACK_BASE__', PUBLIC_PLAYBACK_BASE)
)

def mobile_watch_html(stream_name: str) -> str:
    hls_src = f'{HLS_PATH_PREFIX}/{HLS_APP}/{urllib.parse.quote(stream_name)}.m3u8'
    return (MOBILE_WATCH_HTML
        .replace('__STREAM_NAME__', stream_name)
        .replace('__PLAY_URL__', hls_src)
        .replace('__HLS_SRC__', hls_src))


def now_ts() -> float:
    return time.time()


def stream_key_from_path(path: str) -> str:
    clean_path = urllib.parse.urlsplit(path).path
    expected_prefix = f'{FLV_PATH_PREFIX}/{FLV_APP}/'
    if not clean_path.startswith(expected_prefix):
        return ''
    name = urllib.parse.unquote(clean_path.rsplit('/', 1)[-1])
    if name.endswith('.flv'):
        return name[:-4]
    return name


def hls_stream_key_from_path(path: str) -> str:
    clean_path = urllib.parse.urlsplit(path).path
    prefixes = (f'{HLS_PATH_PREFIX}/{HLS_APP}/', f'/{HLS_APP}/')
    for prefix in prefixes:
        if clean_path.startswith(prefix):
            name = urllib.parse.unquote(clean_path.rsplit('/', 1)[-1])
            if name.endswith('.m3u8'):
                return name[:-5]
            if '.ts' in name:
                return name.split('.ts', 1)[0].rsplit('-', 1)[0]
    return ''


def client_ip(handler: BaseHTTPRequestHandler) -> str:
    forwarded = handler.headers.get('X-Forwarded-For', '').split(',', 1)[0].strip()
    if forwarded:
        return forwarded
    return handler.client_address[0] if handler.client_address else ''


def trace_request(kind: str, handler: BaseHTTPRequestHandler, upstream: str, status: int, extra: str = '') -> None:
    logger.info(
        'kind=%s client=%s method=%s path=%s upstream=%s status=%s %s',
        kind,
        client_ip(handler),
        getattr(handler, 'command', ''),
        getattr(handler, 'path', ''),
        upstream,
        status,
        extra,
    )


def default_edge(stream_key: str) -> str:
    if not EDGES:
        return ''
    idx = int(hashlib.md5(stream_key.encode('utf-8')).hexdigest(), 16) % len(EDGES)
    return EDGES[idx]


def get_edge(stream_key: str) -> str:
    if not EDGES:
        return ''
    with LOCK:
        cached = STREAM_EDGE_CACHE.get(stream_key)
        if cached and now_ts() - cached[1] < EDGE_CACHE_TTL:
            return cached[0]
    return default_edge(stream_key)


def set_edge(stream_key: str, edge: str) -> None:
    if not stream_key or not edge:
        return
    with LOCK:
        STREAM_EDGE_CACHE[stream_key] = (edge, now_ts())


def hls_cache_ttl(path: str) -> int:
    clean_path = urllib.parse.urlsplit(path).path
    if clean_path.endswith('.m3u8'):
        return 0
    if clean_path.endswith('.ts'):
        return 20
    return 0

def normalize_content_type(path: str, content_type: str = '') -> str:
    clean_path = urllib.parse.urlsplit(path).path.lower()
    if clean_path.endswith('.m3u8'):
        return 'application/vnd.apple.mpegurl'
    if clean_path.endswith('.ts'):
        return 'video/mp2t'
    if clean_path.endswith('.flv'):
        return 'video/x-flv'
    return content_type or 'application/octet-stream'


def cache_hls_response(path: str, content_type: str, body: bytes, upstream: str) -> None:
    ttl = hls_cache_ttl(path)
    if ttl <= 0 or not body:
        return
    with LOCK:
        HLS_RESPONSE_CACHE[path] = {
            'ts': now_ts(),
            'ttl': ttl,
            'content_type': content_type,
            'body': body,
            'upstream': upstream,
        }


def get_cached_hls_response(path: str):
    with LOCK:
        cached = HLS_RESPONSE_CACHE.get(path)
        if not cached:
            return None
        if now_ts() - cached['ts'] > cached['ttl']:
            HLS_RESPONSE_CACHE.pop(path, None)
            return None
        return cached


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path == '/':
            self.respond_redirect(f'{WATCH_PATH_PREFIX}/')
            return
        if self.path.startswith(f'{WATCH_PATH_PREFIX}/stats/'):
            self.respond_watch_stats()
            return
        if self.path.startswith(f'{WATCH_PATH_PREFIX}/'):
            self.respond_html()
            return
        if self.path == '/assets/flv.js':
            self.serve_asset('flv.min.js', 'application/javascript')
            return
        if self.path == '/assets/hls.js':
            self.serve_asset('hls-1.4.14.min.js', 'application/javascript')
            return
        if self.path.startswith(f'{FLV_PATH_PREFIX}/'):
            self.proxy_flv('GET')
            return
        if self.path.startswith(f'{HLS_PATH_PREFIX}/'):
            self.proxy_hls('GET')
            return
        if self.path.startswith(f'/{HLS_APP}/'):
            self.proxy_hls('GET', raw_path=self.path)
            return
        if self.path == '/healthz':
            self.respond_json({'ok': True, 'edges': EDGES, 'hls_edges': HLS_EDGES})
            return
        self.send_error(404)

    def do_HEAD(self):
        if self.path == '/':
            self.respond_redirect(f'{WATCH_PATH_PREFIX}/')
            return
        if self.path.startswith(f'{WATCH_PATH_PREFIX}/stats/'):
            self.respond_watch_stats(head_only=True)
            return
        if self.path.startswith(f'{WATCH_PATH_PREFIX}/'):
            self.respond_html(head_only=True)
            return
        if self.path == '/assets/flv.js':
            self.serve_asset('flv.min.js', 'application/javascript', head_only=True)
            return
        if self.path == '/assets/hls.js':
            self.serve_asset('hls-1.4.14.min.js', 'application/javascript', head_only=True)
            return
        if self.path.startswith(f'{FLV_PATH_PREFIX}/'):
            self.proxy_flv('HEAD')
            return
        if self.path.startswith(f'{HLS_PATH_PREFIX}/'):
            self.proxy_hls('HEAD')
            return
        if self.path.startswith(f'/{HLS_APP}/'):
            self.proxy_hls('HEAD', raw_path=self.path)
            return
        if self.path == '/healthz':
            self.respond_json({'ok': True, 'edges': EDGES, 'hls_edges': HLS_EDGES}, head_only=True)
            return
        self.send_error(404)

    def respond_redirect(self, location: str):
        self.send_response(302)
        self.send_header('Location', location)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def respond_html(self, head_only: bool = False):
        ua = self.headers.get('User-Agent', '')
        body_html = WATCH_HTML
        if is_mobile_user_agent(ua):
            parts = urllib.parse.urlsplit(self.path)
            path_only = parts.path.rstrip('/')
            prefix = f'{WATCH_PATH_PREFIX}/'
            if path_only.startswith(prefix):
                stream_name = urllib.parse.unquote(path_only[len(prefix):])
                if stream_name:
                    body_html = mobile_watch_html(stream_name)
        body = body_html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def respond_json(self, payload: dict, head_only: bool = False):
        body = json.dumps(payload).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def respond_watch_stats(self, head_only: bool = False):
        parts = urllib.parse.urlsplit(self.path)
        prefix = f'{WATCH_PATH_PREFIX}/stats/'
        stream_name = urllib.parse.unquote(parts.path[len(prefix):]) if parts.path.startswith(prefix) else ''
        self.respond_json(watch_stats(stream_name), head_only=head_only)

    def serve_asset(self, name: str, content_type: str, head_only: bool = False):
        body = (ASSET_DIR / name).read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'public, max-age=86400')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def fetch(self, edge: str, method: str):
        upstream_url = f'http://{edge}{self.path[len(FLV_PATH_PREFIX):]}'
        req = urllib.request.Request(upstream_url, method=method)
        req.add_header('User-Agent', self.headers.get('User-Agent', 'stream-watch'))
        req.add_header('Host', edge)
        return urllib.request.urlopen(req, timeout=10)

    def fetch_hls(self, edge: str, method: str, raw_path: str = None):
        path = raw_path or self.path[len(HLS_PATH_PREFIX):]
        upstream_url = f'http://{edge}{path}'
        req = urllib.request.Request(upstream_url, method=method)
        req.add_header('User-Agent', self.headers.get('User-Agent', 'stream-watch'))
        req.add_header('Host', edge)
        if self.headers.get('Range'):
            req.add_header('Range', self.headers['Range'])
        return urllib.request.urlopen(req, timeout=10)

    def send_upstream(self, edge: str, resp, method: str):
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        content_type = normalize_content_type(self.path, headers.get('content-type', 'application/octet-stream'))
        self.send_response(resp.status)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('X-Stream-Watch-Edge', edge)
        if headers.get('accept-ranges'):
            self.send_header('Accept-Ranges', headers['accept-ranges'])
        if headers.get('content-range'):
            self.send_header('Content-Range', headers['content-range'])
        if headers.get('content-length'):
            self.send_header('Content-Length', headers['content-length'])
        if method == 'HEAD':
            self.end_headers()
            return
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        try:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                if self.path.startswith(f'{FLV_PATH_PREFIX}/'):
                    stream_name = stream_key_from_path(self.path)
                    mark_web_viewer(stream_name, self)
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def send_http_error(self, upstream: str, err: urllib.error.HTTPError, method: str):
        body = b'' if method == 'HEAD' else err.read()
        self.send_response(err.code)
        self.send_header('Content-Type', normalize_content_type(self.path, err.headers.get('Content-Type', 'text/plain; charset=utf-8')))
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('X-Stream-Watch-Edge', upstream)
        if err.headers.get('Accept-Ranges'):
            self.send_header('Accept-Ranges', err.headers.get('Accept-Ranges'))
        if err.headers.get('Content-Range'):
            self.send_header('Content-Range', err.headers.get('Content-Range'))
        if method != 'HEAD':
            self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if method != 'HEAD':
            self.wfile.write(body)
        trace_request('hls-error', self, upstream, err.code, f'bytes={len(body)}')

    def send_body(self, status: int, content_type: str, body: bytes, method: str, upstream: str):
        content_type = normalize_content_type(self.path, content_type)
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('X-Stream-Watch-Edge', upstream)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if method != 'HEAD':
            self.wfile.write(body)
        trace_request('hls-body', self, upstream, status, f'bytes={len(body)}')

    def send_cached_hls(self, path: str, method: str) -> bool:
        cached = get_cached_hls_response(path)
        if not cached:
            return False
        self.send_response(200)
        self.send_header('Content-Type', normalize_content_type(path, cached['content_type']))
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('X-Stream-Watch-Edge', cached['upstream'])
        self.send_header('X-Stream-Watch-Cache', 'hit')
        self.send_header('Content-Length', str(len(cached['body'])))
        self.end_headers()
        if method != 'HEAD':
            self.wfile.write(cached['body'])
        trace_request('hls-cache', self, cached['upstream'], 200, f'cache=hit bytes={len(cached["body"])}')
        return True

    def maybe_flatten_hls(self, upstream: str, method: str, raw_path: str = None) -> bool:
        path = raw_path or self.path
        path_only = urllib.parse.urlsplit(path).path
        if method != 'GET' or 'hls_ctx=' in path or not path_only.endswith('.m3u8'):
            return False
        with self.fetch_hls(upstream, method, raw_path=raw_path) as resp:
            content_type = dict((k.lower(), v) for k, v in resp.getheaders()).get('content-type', 'application/vnd.apple.mpegurl')
            body = resp.read()
        text = body.decode('utf-8', 'ignore')
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if '#EXT-X-STREAM-INF' not in text:
            self.send_body(200, content_type, body, method, upstream)
            return True
        variant = ''
        for idx, line in enumerate(lines):
            if line.startswith('#EXT-X-STREAM-INF') and idx + 1 < len(lines):
                variant = lines[idx + 1]
                break
        if not variant:
            self.send_body(200, content_type, body, method, upstream)
            return True
        base_path = urllib.parse.urlsplit(path).path
        parsed_variant = urllib.parse.urlsplit(variant)
        nested_path = posixpath.normpath(posixpath.join(posixpath.dirname(base_path), parsed_variant.path))
        if not nested_path.startswith('/'):
            nested_path = '/' + nested_path
        if parsed_variant.query:
            nested_path += '?' + parsed_variant.query
        with self.fetch_hls(upstream, method, raw_path=nested_path) as resp:
            nested_type = dict((k.lower(), v) for k, v in resp.getheaders()).get('content-type', content_type)
            nested_body = resp.read()
        cache_hls_response(path, nested_type, nested_body, upstream)
        trace_request('hls-flat', self, upstream, 200, f'bytes={len(nested_body)}')
        self.send_body(200, nested_type, nested_body, method, upstream)
        return True

    def proxy_flv(self, method: str):
        stream_key = stream_key_from_path(self.path)
        if method == 'GET':
            mark_web_viewer(stream_key, self)
        preferred = get_edge(stream_key)
        upstreams = []
        if preferred:
            upstreams.append(preferred)
        upstreams.extend(edge for edge in EDGES if edge != preferred)
        last_http_error = None
        for upstream in upstreams:
            try:
                with self.fetch(upstream, method) as resp:
                    set_edge(stream_key, upstream)
                    self.send_upstream(upstream, resp, method)
                    return
            except urllib.error.HTTPError as err:
                last_http_error = (upstream, err)
                continue
            except Exception:
                continue
        if last_http_error is not None:
            upstream, err = last_http_error
            self.send_http_error(upstream, err, method)
            return
        body = b'Not Found'
        self.send_response(404)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if method != 'HEAD':
            self.wfile.write(body)

    def proxy_hls(self, method: str, raw_path: str = None):
        if not HLS_EDGES:
            self.send_error(404)
            return
        request_path = raw_path or self.path
        request_path_only = urllib.parse.urlsplit(request_path).path
        stream_key = hls_stream_key_from_path(request_path)
        if stream_key and request_path_only.endswith('.m3u8'):
            mark_mobile_hls_viewer(stream_key, self)
            request_mobile_hls(stream_key)
        viewer_key = f'{stream_key}|{client_ip(self)}' if stream_key else client_ip(self)
        preferred = get_edge(viewer_key).rsplit(':', 1)[0] + f':{HLS_EDGE_PORT}' if viewer_key else ''
        upstreams = []
        if preferred and preferred in HLS_EDGES:
            upstreams.append(preferred)
        upstreams.extend(edge for edge in HLS_EDGES if edge != preferred)
        last_http_error = None
        direct_stream = method == 'GET' and (self.headers.get('Range') or request_path_only.endswith('.ts'))
        for upstream in upstreams:
            try:
                if not direct_stream and self.maybe_flatten_hls(upstream, method, raw_path=raw_path):
                    return
                with self.fetch_hls(upstream, method, raw_path=raw_path) as resp:
                    if direct_stream:
                        trace_request('hls-stream', self, upstream, resp.status)
                        self.send_upstream(upstream, resp, method)
                        return
                    if method == 'GET':
                        headers = dict((k.lower(), v) for k, v in resp.getheaders())
                        body = resp.read()
                        content_type = headers.get('content-type', 'application/octet-stream')
                        cache_hls_response(request_path, content_type, body, upstream)
                        trace_request('hls-get', self, upstream, resp.status, f'bytes={len(body)}')
                        self.send_body(resp.status, content_type, body, method, upstream)
                        return
                    trace_request('hls-head', self, upstream, resp.status)
                    self.send_upstream(upstream, resp, method)
                    return
            except urllib.error.HTTPError as err:
                last_http_error = (upstream, err)
                continue
            except Exception:
                continue
        if method == 'GET' and not direct_stream and self.send_cached_hls(request_path, method):
            return
        if last_http_error is not None:
            upstream, err = last_http_error
            if method == 'GET' and not direct_stream and self.send_cached_hls(request_path, method):
                return
            self.send_http_error(upstream, err, method)
            return
        body = b'Bad Gateway'
        self.send_response(502)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if method != 'HEAD':
            self.wfile.write(body)


if __name__ == '__main__':
    ThreadingHTTPServer(LISTEN, Handler).serve_forever()
