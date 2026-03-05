#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IDAP Daily Maps (refino final)
Saídas:
1) mapa_alertas_todos.png
2) mapa_alertas_chuva_temp_inund.png
3) mapa_alertas_deslizamento.png
4) mapa_alertas_outros.png
+ resumo.json e resumo.md

Regras:
- Varre o RSS completo a cada execução (sem filtrar por status).
- Cor por NIVEL calculado:
    Extremo, Severo, Alto, Médio, Baixo, Indefinido
- Mapas 2, 3 e 4 são filtros por EVENTO.
"""

import json
import os
import re
import unicodedata
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
import matplotlib.pyplot as plt


# ----------------------------
# Config / Env
# ----------------------------

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

DEFAULT_RSS_URL = "https://idapfile.mdr.gov.br/idap/api/rss/cap"
DEFAULT_UF_GEOJSON_PATH = "resources/br_uf.geojson"
DEFAULT_OUT_DIR = "out"
DEFAULT_STATE_PATH = ".cache/state.json"

# UF -> Região
UF_TO_REGION = {
    # Norte
    "AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "N", "TO": "N",
    # Nordeste
    "AL": "NE", "BA": "NE", "CE": "NE", "MA": "NE", "PB": "NE", "PE": "NE",
    "PI": "NE", "RN": "NE", "SE": "NE",
    # Centro-Oeste
    "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO",
    # Sudeste
    "ES": "SE", "MG": "SE", "RJ": "SE", "SP": "SE",
    # Sul
    "PR": "S", "RS": "S", "SC": "S",
}

# Cores por NÍVEL (não por severity)
NIVEL_COLORS = {
    "Extremo": "#6a0dad",     # 🟣
    "Severo":  "#d62728",     # 🔴
    "Alto":    "#ff7f0e",     # 🟠
    "Médio":   "#ffd92f",     # 🟡
    "Baixo":   "#2ca02c",     # 🟢
    "Indefinido": "#7f7f7f",  # ⚪ (cinza aqui no mapa)
}

ALERT_ALPHA = 0.35
BORDER_ALPHA = 0.9


# ----------------------------
# Regras de nível (as suas)
# ----------------------------

def calc_nivel(severity: str, urgency: str, certainty: str, response_type: str) -> str:
    s = (severity or "").strip()
    u = (urgency or "").strip()
    c = (certainty or "").strip()
    r = (response_type or "").strip()

    if s == "Extreme":
        return "Extremo"
    if s == "Moderate":
        return "Médio"
    if s == "Minor":
        return "Baixo"

    if s == "Severe":
        if (u == "Expected") and (c in {"Likely", "Observed"}) and (r in {"Execute", "Prepare"}):
            return "Severo"
        return "Alto"

    return "Indefinido"


def nivel_emoji(nivel: str) -> str:
    n = (nivel or "").strip()
    return {
        "Extremo": "🟣",
        "Severo": "🔴",
        "Alto": "🟠",
        "Médio": "🟡",
        "Baixo": "🟢",
        "Indefinido": "⚪",
    }.get(n, "⚪")


# ----------------------------
# Modelos
# ----------------------------

@dataclass
class AlertRecord:
    identifier: str
    sender: Optional[str]
    senderName: Optional[str]
    sent: Optional[str]
    status: Optional[str]
    msgType: Optional[str]

    category: Optional[str]
    event: Optional[str]
    responseType: Optional[str]
    urgency: Optional[str]
    severity: Optional[str]
    certainty: Optional[str]
    onset: Optional[str]
    expires: Optional[str]

    nivel: str  # calculado

    headline: Optional[str]
    description: Optional[str]
    instruction: Optional[str]
    web: Optional[str]
    contact: Optional[str]

    channel_list: Optional[str]

    areaDesc: Optional[str]
    polygon_raw: Optional[str]
    polygon_points: int
    has_geocode: bool
    uf_hint: Optional[str]
    region: Optional[str]

    geometry_wkt: Optional[str]


# ----------------------------
# Utilitários
# ----------------------------

def _now_sp() -> datetime:
    return datetime.now().astimezone()


def _safe_text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    txt = elem.text
    if txt is None:
        return None
    txt = txt.strip()
    return txt if txt != "" else None


def _first(elem: ET.Element, path: str, ns: Dict[str, str]) -> Optional[ET.Element]:
    try:
        return elem.find(path, ns)
    except Exception:
        return None


def _all(elem: ET.Element, path: str, ns: Dict[str, str]) -> List[ET.Element]:
    try:
        return elem.findall(path, ns) or []
    except Exception:
        return []


def _read_url(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "IDAP-Daily-Maps/1.2 (+github-actions)", "Accept": "*/*"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    if not s:
        return ""
    s2 = unicodedata.normalize("NFKD", s)
    s2 = "".join([c for c in s2 if not unicodedata.combining(c)])
    return s2.upper()


def _parse_polygon_str(poly_str: str) -> Optional[BaseGeometry]:
    """
    CAP polygon vem como "lat,lon lat,lon ..."
    Shapely espera (x,y) = (lon,lat)
    """
    if not poly_str:
        return None
    poly_str = poly_str.strip()
    if not poly_str:
        return None

    pts: List[Tuple[float, float]] = []
    for token in poly_str.split():
        if "," not in token:
            continue
        a, b = token.split(",", 1)
        try:
            lat = float(a)
            lon = float(b)
        except ValueError:
            continue
        pts.append((lon, lat))

    if len(pts) < 3:
        return None

    if pts[0] != pts[-1]:
        pts.append(pts[0])

    geom: BaseGeometry = Polygon(pts)
    if not geom.is_valid:
        geom = geom.buffer(0)

    if geom.is_empty:
        return None
    return geom


def _geom_points_count(geom: Optional[BaseGeometry]) -> int:
    try:
        if geom is None or geom.is_empty:
            return 0

        if geom.geom_type == "Polygon":
            return len(geom.exterior.coords) if geom.exterior else 0

        if geom.geom_type == "MultiPolygon":
            best = 0
            mp: MultiPolygon = geom  # type: ignore
            for g in mp.geoms:
                if g.exterior:
                    best = max(best, len(g.exterior.coords))
            return best

        return 0
    except Exception:
        return 0


def _guess_uf(area_desc: Optional[str]) -> Optional[str]:
    txt = (area_desc or "").strip().upper()
    m = re.search(r"/([A-Z]{2})\b", txt)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z]{2})\b", txt)
    if m:
        return m.group(1)

    return None


def _uf_to_region(uf: Optional[str]) -> Optional[str]:
    if not uf:
        return None
    uf2 = uf.strip().upper()
    return UF_TO_REGION.get(uf2)


def _cap_get_parameter(info_elem: ET.Element, value_name: str) -> Optional[str]:
    for p in _all(info_elem, "cap:parameter", CAP_NS):
        vn = _safe_text(_first(p, "cap:valueName", CAP_NS))
        if vn and vn.strip().upper() == value_name.strip().upper():
            return _safe_text(_first(p, "cap:value", CAP_NS))
    return None


def _extract_cap_xml_from_entry(entry: ET.Element) -> Optional[ET.Element]:
    content = _first(entry, "atom:content", ATOM_NS)
    if content is None:
        return None

    # caso venha como filho XML mesmo
    for child in list(content):
        if child.tag.endswith("alert"):
            return child

    raw = content.text
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None

    # tenta direto
    try:
        root = ET.fromstring(raw)
        if root.tag.endswith("alert"):
            return root
    except Exception:
        pass

    # tenta des-escapar
    raw2 = raw.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&amp;", "&")
    try:
        root = ET.fromstring(raw2)
        if root.tag.endswith("alert"):
            return root
    except Exception:
        return None

    return None


def _parse_cap_from_entry(entry: ET.Element) -> Tuple[Optional[AlertRecord], Optional[str]]:
    try:
        cap_alert = _extract_cap_xml_from_entry(entry)
        if cap_alert is None:
            return None, "entry sem CAP <alert>"

        identifier = _safe_text(_first(cap_alert, "cap:identifier", CAP_NS)) or "UNKNOWN"
        sender = _safe_text(_first(cap_alert, "cap:sender", CAP_NS))
        sent = _safe_text(_first(cap_alert, "cap:sent", CAP_NS))
        status = _safe_text(_first(cap_alert, "cap:status", CAP_NS))
        msgType = _safe_text(_first(cap_alert, "cap:msgType", CAP_NS))

        info = _first(cap_alert, "cap:info", CAP_NS)
        if info is None:
            infos = _all(cap_alert, "cap:info", CAP_NS)
            info = infos[0] if infos else None

        category = event = responseType = urgency = severity = certainty = onset = expires = None
        senderName = headline = description = instruction = web = contact = None
        channel_list = None
        areaDesc = None
        polygon_raw = None
        has_geocode = False
        geom: Optional[BaseGeometry] = None

        if info is not None:
            category = _safe_text(_first(info, "cap:category", CAP_NS))
            event = _safe_text(_first(info, "cap:event", CAP_NS))
            responseType = _safe_text(_first(info, "cap:responseType", CAP_NS))
            urgency = _safe_text(_first(info, "cap:urgency", CAP_NS))
            severity = _safe_text(_first(info, "cap:severity", CAP_NS))
            certainty = _safe_text(_first(info, "cap:certainty", CAP_NS))
            onset = _safe_text(_first(info, "cap:onset", CAP_NS))
            expires = _safe_text(_first(info, "cap:expires", CAP_NS))
            senderName = _safe_text(_first(info, "cap:senderName", CAP_NS))
            headline = _safe_text(_first(info, "cap:headline", CAP_NS))
            description = _safe_text(_first(info, "cap:description", CAP_NS))
            instruction = _safe_text(_first(info, "cap:instruction", CAP_NS))
            web = _safe_text(_first(info, "cap:web", CAP_NS))
            contact = _safe_text(_first(info, "cap:contact", CAP_NS))

            channel_list = _cap_get_parameter(info, "CHANNEL-LIST")

            area = _first(info, "cap:area", CAP_NS)
            if area is not None:
                areaDesc = _safe_text(_first(area, "cap:areaDesc", CAP_NS))
                polygon_raw = _safe_text(_first(area, "cap:polygon", CAP_NS))

                geocodes = _all(area, "cap:geocode", CAP_NS)
                has_geocode = len(geocodes) > 0

                if polygon_raw:
                    geom = _parse_polygon_str(polygon_raw)

        uf_hint = _guess_uf(areaDesc)
        region = _uf_to_region(uf_hint)

        nivel = calc_nivel(severity or "", urgency or "", certainty or "", responseType or "")

        rec = AlertRecord(
            identifier=identifier,
            sender=sender,
            senderName=senderName,
            sent=sent,
            status=status,
            msgType=msgType,
            category=category,
            event=event,
            responseType=responseType,
            urgency=urgency,
            severity=severity,
            certainty=certainty,
            onset=onset,
            expires=expires,
            nivel=nivel,
            headline=headline,
            description=description,
            instruction=instruction,
            web=web,
            contact=contact,
            channel_list=channel_list,
            areaDesc=areaDesc,
            polygon_raw=polygon_raw,
            polygon_points=_geom_points_count(geom),
            has_geocode=has_geocode,
            uf_hint=uf_hint,
            region=region,
            geometry_wkt=geom.wkt if geom is not None else None,
        )
        return rec, None

    except Exception as e:
        return None, f"erro parse CAP: {e}"


def _load_uf_gdf(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        try:
            gdf = gdf.to_crs("EPSG:4326")
        except Exception:
            pass
    return gdf


def _alerts_to_gdf(alerts: List[AlertRecord]) -> gpd.GeoDataFrame:
    geoms = []
    rows = []
    for a in alerts:
        if not a.geometry_wkt:
            continue
        try:
            geom = gpd.GeoSeries.from_wkt([a.geometry_wkt], crs="EPSG:4326").iloc[0]
        except Exception:
            continue
        geoms.append(geom)
        rows.append(a)

    if not rows:
        return gpd.GeoDataFrame(columns=["identifier"], geometry=[], crs="EPSG:4326")

    df = gpd.GeoDataFrame([asdict(r) for r in rows], geometry=geoms, crs="EPSG:4326")
    return df


def _count_by(alerts: List[AlertRecord], key_fn) -> Dict[str, int]:
    d: Dict[str, int] = {}
    for a in alerts:
        k = key_fn(a) or "N/A"
        d[k] = d.get(k, 0) + 1
    return dict(sorted(d.items(), key=lambda x: (-x[1], x[0])))


def _make_summary(alerts: List[AlertRecord]) -> Dict[str, Any]:
    by_nivel = _count_by(alerts, lambda a: a.nivel)
    by_channel = _count_by(alerts, lambda a: a.channel_list)
    by_region = _count_by(alerts, lambda a: a.region)

    return {
        "total_alerts": len(alerts),
        "by_nivel": by_nivel,
        "by_channel_list": by_channel,
        "by_region": by_region,
    }


def _plot_alerts_map(
    uf_gdf: gpd.GeoDataFrame,
    alerts_gdf: gpd.GeoDataFrame,
    out_path: str,
    title: str,
) -> None:
    fig = plt.figure(figsize=(12, 12))
    ax = plt.gca()

    uf_gdf.boundary.plot(ax=ax, linewidth=0.6, alpha=BORDER_ALPHA)

    if len(alerts_gdf) > 0:
        def nivel_color(n: str) -> str:
            n = (n or "").strip()
            return NIVEL_COLORS.get(n, NIVEL_COLORS["Indefinido"])

        alerts_gdf["_color"] = alerts_gdf["nivel"].apply(nivel_color)

        alerts_gdf.plot(
            ax=ax,
            color=alerts_gdf["_color"],
            edgecolor=alerts_gdf["_color"],
            linewidth=0.8,
            alpha=ALERT_ALPHA,
        )

    ax.set_title(title, fontsize=12)
    ax.set_axis_off()
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def _load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _write_resumo_md(path: str, resumo: Dict[str, Any]) -> None:
    lines = []
    lines.append("# Quadro geral")
    lines.append("")
    lines.append(f"Total de alertas (RSS considerados): **{resumo.get('total_alerts', 0)}**")
    lines.append("")

    def _block(title: str, d: Dict[str, int], emoji: bool = False):
        lines.append(f"## {title}")
        lines.append("")
        for k, v in d.items():
            if emoji:
                lines.append(f"- {nivel_emoji(k)} {k}: {v}")
            else:
                lines.append(f"- {k}: {v}")
        lines.append("")

    _block("Nível (calculado)", resumo.get("by_nivel", {}), emoji=True)
    _block("Tipo (CHANNEL-LIST)", resumo.get("by_channel_list", {}), emoji=False)
    _block("Alertas por regiões do Brasil", resumo.get("by_region", {}), emoji=False)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ----------------------------
# Filtros de mapa
# ----------------------------

def _is_chuva_temp_inund(event: Optional[str]) -> bool:
    n = _normalize_text(event)

    # compatibilidade com textos compostos tipo: "TEMPESTADE LOCAL CONVECTIVA - CHUVAS INTENSAS"
    if "CHUVA" in n and "INTENSA" in n:
        return True
    if "TEMPESTADE" in n and "CONVECT" in n:
        return True
    if "INUND" in n:
        return True

    return False


def _is_deslizamento(event: Optional[str]) -> bool:
    n = _normalize_text(event)
    # cobre DESLIZAMENTO(S), e variações que você usa no dia a dia
    return ("DESLIZ" in n) or ("MOVIMENTO DE MASSA" in n) or ("CORRIDA DE MASSA" in n)


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    rss_url = os.getenv("RSS_URL", DEFAULT_RSS_URL)
    uf_geojson_path = os.getenv("UF_GEOJSON_PATH", DEFAULT_UF_GEOJSON_PATH)
    out_dir = os.getenv("OUT_DIR", DEFAULT_OUT_DIR)
    state_path = os.getenv("STATE_PATH", DEFAULT_STATE_PATH)

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    print(f"[INFO] RSS_URL={rss_url}")
    print(f"[INFO] UF_GEOJSON_PATH={uf_geojson_path}")
    print(f"[INFO] OUT_DIR={out_dir}")

    run_ts = _now_sp().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(out_dir, f"run_{run_ts}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")

    _ensure_dirs(".cache", out_dir, run_dir)

    state = _load_state(state_path)

    # baixa RSS
    try:
        rss_bytes = _read_url(rss_url, timeout=40)
    except urllib.error.URLError as e:
        print(f"[ERROR] Falha ao baixar RSS: {e}")
        return 2

    try:
        root = ET.fromstring(rss_bytes)
    except Exception as e:
        print(f"[ERROR] RSS inválido (XML): {e}")
        return 3

    entries = _all(root, "atom:entry", ATOM_NS)
    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    alerts: List[AlertRecord] = []
    errors: List[Dict[str, Any]] = []

    for entry in entries:
        a, err = _parse_cap_from_entry(entry)
        if a is None:
            errors.append({"error": err or "desconhecido"})
            continue
        alerts.append(a)

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # salva dados brutos
    with open(os.path.join(run_dir, "alerts.json"), "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in alerts], f, ensure_ascii=False, indent=2)
    with open(os.path.join(run_dir, "errors.json"), "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    # resumo
    resumo = _make_summary(alerts)
    resumo_json_path = os.path.join(run_dir, "resumo.json")
    resumo_md_path = os.path.join(run_dir, "resumo.md")

    with open(resumo_json_path, "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)
    _write_resumo_md(resumo_md_path, resumo)

    # base brasil
    try:
        uf_gdf = _load_uf_gdf(uf_geojson_path)
    except Exception as e:
        print(f"[ERROR] Falha ao ler UF GeoJSON: {e}")
        return 4

    # prepara gdf geral com polygons
    alerts_gdf_all = _alerts_to_gdf(alerts)

    # Mapa 1: todos
    map1 = os.path.join(run_dir, "mapa_alertas_todos.png")
    if len(alerts_gdf_all) > 0:
        _plot_alerts_map(
            uf_gdf,
            alerts_gdf_all,
            map1,
            f"Alertas IDAP (todos) | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map1}")
    else:
        map1 = ""
        print("[WARN] Mapa 1 não gerado: nenhum alerta com polygon")

    # Mapa 2: chuva/temp/inund
    alerts_2 = [a for a in alerts if _is_chuva_temp_inund(a.event)]
    gdf_2 = _alerts_to_gdf(alerts_2)
    map2 = os.path.join(run_dir, "mapa_alertas_chuva_temp_inund.png")
    if len(gdf_2) > 0:
        _plot_alerts_map(
            uf_gdf,
            gdf_2,
            map2,
            f"Alertas: Chuvas Intensas, Tempestades Convectivas, Inundações | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map2}")
    else:
        map2 = ""
        print("[WARN] Mapa 2 não gerado: nenhum alerta (filtro) com polygon")

    # Mapa 3: deslizamento
    alerts_3 = [a for a in alerts if _is_deslizamento(a.event)]
    gdf_3 = _alerts_to_gdf(alerts_3)
    map3 = os.path.join(run_dir, "mapa_alertas_deslizamento.png")
    if len(gdf_3) > 0:
        _plot_alerts_map(
            uf_gdf,
            gdf_3,
            map3,
            f"Alertas: Deslizamento | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map3}")
    else:
        map3 = ""
        print("[WARN] Mapa 3 não gerado: nenhum alerta de deslizamento com polygon")

    # Mapa 4: demais alertas (exclui mapas 2 e 3)
    ids_2 = {a.identifier for a in alerts_2}
    ids_3 = {a.identifier for a in alerts_3}
    alerts_4 = [a for a in alerts if (a.identifier not in ids_2) and (a.identifier not in ids_3)]
    gdf_4 = _alerts_to_gdf(alerts_4)
    map4 = os.path.join(run_dir, "mapa_alertas_outros.png")
    if len(gdf_4) > 0:
        _plot_alerts_map(
            uf_gdf,
            gdf_4,
            map4,
            f"Alertas: Outros tipos | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map4}")
    else:
        map4 = ""
        print("[WARN] Mapa 4 não gerado: nenhum alerta (outros) com polygon")

    # Telegram (opcional): aqui eu só mando texto, e as imagens uma a uma
    if tg_token and tg_chat_id:
        by_nivel = resumo.get("by_nivel", {}) or {}
        by_reg = resumo.get("by_region", {}) or {}
        by_typ = resumo.get("by_channel_list", {}) or {}

        def _fmt_counts(d: Dict[str, int], with_emoji: bool = False) -> str:
            parts = []
            for k, v in list(d.items())[:10]:
                if with_emoji:
                    parts.append(f"{nivel_emoji(k)} {k}:{v}")
                else:
                    parts.append(f"{k}:{v}")
            return ", ".join(parts)

        msg = (
            f"IDAP Daily Maps\n"
            f"Rodada: {run_ts}\n"
            f"Entradas RSS: {len(entries)}\n"
            f"CAPs parseados: {len(alerts)} | erros: {len(errors)}\n"
            f"Nível: {_fmt_counts(by_nivel, with_emoji=True)}\n"
            f"Tipo: {_fmt_counts(by_typ, with_emoji=False)}\n"
            f"Regiões: {_fmt_counts(by_reg, with_emoji=False)}\n"
        )

        # sendMessage
        try:
            url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
            data = json.dumps({"chat_id": tg_chat_id, "text": msg}).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                _ = resp.read()
            print("[INFO] Telegram: mensagem enviada")
        except Exception as e:
            print(f"[WARN] Telegram: falha ao enviar mensagem: {e}")

        # sendPhoto
        def _send_photo(photo_path: str, caption: str) -> None:
            import uuid
            boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
            url = f"https://api.telegram.org/bot{tg_token}/sendPhoto"

            with open(photo_path, "rb") as f:
                photo_bytes = f.read()

            def _part(name: str, value: str) -> bytes:
                return (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")

            body = b""
            body += _part("chat_id", str(tg_chat_id))
            if caption:
                body += _part("caption", caption)

            filename = os.path.basename(photo_path)
            body += (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'
                f"Content-Type: image/png\r\n\r\n"
            ).encode("utf-8")
            body += photo_bytes
            body += b"\r\n"
            body += f"--{boundary}--\r\n".encode("utf-8")

            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                _ = resp.read()

        for pth, cap in [
            (map1, f"Mapa 1: todos | {run_ts}"),
            (map2, f"Mapa 2: chuva/temp/inund | {run_ts}"),
            (map3, f"Mapa 3: deslizamento | {run_ts}"),
            (map4, f"Mapa 4: outros | {run_ts}"),
        ]:
            if pth:
                try:
                    _send_photo(pth, cap)
                    print(f"[INFO] Telegram: enviado {os.path.basename(pth)}")
                except Exception as e:
                    print(f"[WARN] Telegram: falha ao enviar {os.path.basename(pth)}: {e}")
    else:
        print("[INFO] Telegram: não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID vazios)")

    # state.json (registro simples)
    state["last_run_ts"] = run_ts
    state["last_run_iso"] = datetime.now(timezone.utc).isoformat()
    state["last_counts"] = {"entries": len(entries), "alerts": len(alerts), "errors": len(errors)}
    _save_state(state_path, state)

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
