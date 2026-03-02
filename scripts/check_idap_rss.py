#!/usr/bin/env python3
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
import http.client
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo
import html

RSS_URL = os.environ.get("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/seen.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "45"))
HTTP_RETRIES = int(os.environ.get("HTTP_RETRIES", "5"))
HTTP_RETRY_SLEEP_SECONDS = float(os.environ.get("HTTP_RETRY_SLEEP_SECONDS", "2.0"))

BR_TZ = ZoneInfo("America/Sao_Paulo")

IBGE_JSON_PATH = os.environ.get("IBGE_JSON_PATH", "data/ibge_municipios.json").strip()
MAX_MUNICIPIOS_LISTAR = int(os.environ.get("MAX_MUNICIPIOS_LISTAR", "15"))


def http_get(url: str, timeout: int = HTTP_TIMEOUT) -> bytes:
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "github-actions-idap-atom-monitor/1.7"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()

        except http.client.IncompleteRead as e:
            last_err = e
            print(f"[http_get] IncompleteRead {attempt}/{HTTP_RETRIES}: {e}. Retentando...")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            last_err = RuntimeError(f"HTTPError {e.code} ao baixar {url}. Body: {body[:400]}")
            print(f"[http_get] HTTPError {attempt}/{HTTP_RETRIES}: {e.code}")
            if 400 <= e.code < 500:
                break
        except urllib.error.URLError as e:
            last_err = e
            print(f"[http_get] URLError {attempt}/{HTTP_RETRIES}: {e}. Retentando...")
        except Exception as e:
            last_err = e
            print(f"[http_get] Erro {attempt}/{HTTP_RETRIES}: {e}. Retentando...")

        if attempt < HTTP_RETRIES:
            time.sleep(HTTP_RETRY_SLEEP_SECONDS * attempt)

    raise RuntimeError(f"Falha ao baixar {url} após {HTTP_RETRIES} tentativas. Último erro: {last_err}") from last_err


def _tg_call_sendmessage(chat_id: str, text: str, parse_mode: str = "HTML") -> Tuple[bool, str]:
    if not TG_TOKEN:
        return False, "TG_TOKEN vazio"

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return True, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return False, body
    except Exception as e:
        return False, str(e)


def tg_send_message(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("Telegram não configurado. Vou só imprimir a mensagem.")
        print(text)
        return

    ok, body = _tg_call_sendmessage(TG_CHAT_ID, text)
    if ok:
        return

    try:
        j = json.loads(body) if body and body.strip().startswith("{") else None
    except Exception:
        j = None

    if isinstance(j, dict):
        params = j.get("parameters") or {}
        migrate_to = params.get("migrate_to_chat_id")
        if migrate_to:
            print(f"[Telegram] Atenção: chat foi migrado. Novo chat id sugerido: {migrate_to}")
            ok2, _ = _tg_call_sendmessage(str(migrate_to), text)
            if ok2:
                print("[Telegram] Mensagem enviada com sucesso usando migrate_to_chat_id.")
                print(f"[Telegram] Atualize seu TELEGRAM_CHAT_ID para: {migrate_to}")
                return

    raise RuntimeError(f"Telegram sendMessage falhou. Resposta: {body}")


def chunk_text(text: str, limit: int = 3600) -> List[str]:
    # HTML aumenta risco de cortar tag no meio, então deixei o limite menor
    parts: List[str] = []
    buf = ""

    def flush() -> None:
        nonlocal buf
        if buf:
            parts.append(buf.rstrip())
            buf = ""

    for line in text.splitlines(True):
        while len(line) > limit:
            piece = line[:limit]
            line = line[limit:]
            if len(buf) + len(piece) > limit:
                flush()
            buf += piece
            flush()

        if len(buf) + len(line) > limit:
            flush()
        buf += line

    flush()
    return parts if parts else [text]


def truncate(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def parse_any_iso(dt_str: str) -> Optional[datetime]:
    s = (dt_str or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def fmt_brasilia(dt_str: str) -> str:
    dt = parse_any_iso(dt_str)
    if not dt:
        return "-"
    br = dt.astimezone(BR_TZ)
    return br.strftime("%d/%m %H:%M")


def localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def first_child(elem: ET.Element, name: str) -> Optional[ET.Element]:
    for ch in list(elem):
        if localname(ch.tag) == name:
            return ch
    return None


def child_text(elem: ET.Element, name: str) -> str:
    ch = first_child(elem, name)
    return (ch.text or "").strip() if ch is not None else ""


def load_state() -> Dict:
    if not os.path.exists(STATE_PATH):
        return {"seen_ids": [], "last_run_utc": ""}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen_ids": [], "last_run_utc": ""}


def save_state(state: Dict) -> None:
    d = os.path.dirname(STATE_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_ibge_map() -> Dict[str, Dict[str, object]]:
    if not IBGE_JSON_PATH:
        return {}
    if not os.path.exists(IBGE_JSON_PATH):
        print(f"Atenção: não achei o arquivo IBGE em {IBGE_JSON_PATH}. Vou seguir sem nomes de municípios.")
        return {}
    try:
        with open(IBGE_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"Atenção: falha ao ler {IBGE_JSON_PATH}: {e}. Vou seguir sem nomes de municípios.")
        return {}


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


def event_emoji(event: str) -> str:
    e = (event or "").upper()
    if "DESLIZ" in e:
        return "⛰️"
    if "CHUVA" in e:
        return "🌧️"
    if "ALAG" in e or "INUN" in e or "ENXUR" in e:
        return "🌊"
    if "VENDAV" in e:
        return "🌬️"
    if "GRANIZ" in e:
        return "🧊"
    if "RAIO" in e:
        return "⚡"
    if "ONDA DE CALOR" in e:
        return "🥵"
    if "ONDA DE FRIO" in e or "GEADA" in e:
        return "🥶"
    return "📢"


def parse_atom_feed(feed_xml: bytes) -> List[Dict[str, object]]:
    root = ET.fromstring(feed_xml)
    out: List[Dict[str, object]] = []

    for entry in list(root):
        if localname(entry.tag) != "entry":
            continue

        entry_id = child_text(entry, "id").strip()
        entry_updated = child_text(entry, "updated").strip()

        content = first_child(entry, "content")
        if content is None:
            continue

        alert = None
        for ch in list(content):
            if localname(ch.tag) == "alert":
                alert = ch
                break
        if alert is None:
            continue

        info = None
        for ch in list(alert):
            if localname(ch.tag) == "info":
                info = ch
                break

        def info_text(name: str) -> str:
            if info is None:
                return ""
            for ch in list(info):
                if localname(ch.tag) == name:
                    return (ch.text or "").strip()
            return ""

        ibges: List[str] = []
        area_descs: List[str] = []

        if info is not None:
            for info_child in list(info):
                if localname(info_child.tag) != "area":
                    continue
                area = info_child

                for area_child in list(area):
                    if localname(area_child.tag) == "areaDesc":
                        ad = (area_child.text or "").strip()
                        if ad:
                            area_descs.append(ad)

                for area_child in list(area):
                    if localname(area_child.tag) != "geocode":
                        continue
                    vn = ""
                    vv = ""
                    for gc_child in list(area_child):
                        if localname(gc_child.tag) == "valueName":
                            vn = (gc_child.text or "").strip()
                        elif localname(gc_child.tag) == "value":
                            vv = (gc_child.text or "").strip()
                    if vn.upper() == "IBGE" and vv:
                        vv_norm = vv.strip()
                        if vv_norm.isdigit():
                            vv_norm = vv_norm.zfill(7)
                        ibges.append(vv_norm)

        seen_codes: Set[str] = set()
        ibges_unique: List[str] = []
        for code in ibges:
            if code and code not in seen_codes:
                seen_codes.add(code)
                ibges_unique.append(code)

        seen_ad: Set[str] = set()
        area_descs_unique: List[str] = []
        for ad in area_descs:
            ad2 = ad.strip()
            if ad2 and ad2 not in seen_ad:
                seen_ad.add(ad2)
                area_descs_unique.append(ad2)

        out.append(
            {
                "entry_id": entry_id,
                "entry_updated": entry_updated,
                "onset": info_text("onset"),
                "senderName": info_text("senderName"),
                "event": info_text("event"),
                "headline": info_text("headline"),
                "severity": info_text("severity"),
                "urgency": info_text("urgency"),
                "certainty": info_text("certainty"),
                "responseType": info_text("responseType"),
                "ibge_codes": ibges_unique,
                "area_descs": area_descs_unique,
            }
        )

    return out


def ibge_codes_to_names(codes: List[str], ibge_map: Dict[str, Dict[str, object]]) -> List[str]:
    names: List[str] = []
    for code in codes or []:
        item = ibge_map.get(code)
        if not item:
            names.append(code)
            continue
        nome = str(item.get("nome", "")).strip()
        uf = str(item.get("uf", "")).strip()
        if nome and uf:
            names.append(f"{nome}/{uf}")
        elif nome:
            names.append(nome)
        else:
            names.append(code)
    return names


def format_municipios_list(muns: List[str]) -> str:
    if not muns:
        return "-"
    total = len(muns)
    if total <= MAX_MUNICIPIOS_LISTAR:
        return "; ".join(muns)
    primeiros = "; ".join(muns[:MAX_MUNICIPIOS_LISTAR])
    resto = total - MAX_MUNICIPIOS_LISTAR
    return f"{primeiros}; +{resto} outros"


def format_area_desc(area_descs: List[str]) -> str:
    ads = [a.strip() for a in (area_descs or []) if a and a.strip()]
    if not ads:
        return "-"
    if len(ads) == 1:
        return ads[0]
    return "; ".join(ads)


def esc(s: str) -> str:
    return html.escape((s or "").strip())


def main() -> int:
    if not RSS_URL:
        print("RSS_URL vazio.")
        return 2

    ibge_map = load_ibge_map()

    now_utc = datetime.now(timezone.utc)
    state = load_state()
    seen: Set[str] = set(state.get("seen_ids", []) or [])

    print(f"Baixando feed: {RSS_URL}")
    feed_xml = http_get(RSS_URL)
    entries = parse_atom_feed(feed_xml)

    print(f"Entries no feed: {len(entries)}")

    new_entries = [e for e in entries if e.get("entry_id") and str(e["entry_id"]) not in seen]
    print(f"Novos desde a última execução: {len(new_entries)}")

    if new_entries:
        def sort_key(e: Dict[str, object]) -> str:
            return str(e.get("onset") or e.get("entry_updated") or "")

        new_entries_sorted = sorted(new_entries, key=sort_key)

        tg_send_message(f"✅ <b>Novos alertas desde a última checagem:</b> {len(new_entries_sorted)}")

        for e in new_entries_sorted:
            nivel = calc_nivel(
                str(e.get("severity", "")),
                str(e.get("urgency", "")),
                str(e.get("certainty", "")),
                str(e.get("responseType", "")),
            )

            onset_raw = str(e.get("onset") or e.get("entry_updated") or "").strip()
            onset_br = fmt_brasilia(onset_raw)

            sender = truncate(str(e.get("senderName") or "-"), 180)
            event = truncate(str(e.get("event") or "-"), 90)
            headline = truncate(str(e.get("headline") or "-"), 800)

            ibge_codes = e.get("ibge_codes") or []
            if not isinstance(ibge_codes, list):
                ibge_codes = []
            municipios = ibge_codes_to_names([str(x) for x in ibge_codes], ibge_map)

            area_descs = e.get("area_descs") or []
            if not isinstance(area_descs, list):
                area_descs = []
            area_desc_txt = format_area_desc([str(x) for x in area_descs])

            lvl_emo = nivel_emoji(nivel)
            evt_emo = event_emoji(event)

            # monta mensagem "bonita"
            title_line = f"🕒 <b>{esc(onset_br)}</b>  |  {lvl_emo} <b>{esc(nivel)}</b>  |  {evt_emo} <b>{esc(event)}</b>"
            sender_line = f"<b>Emissor:</b> {esc(sender)}"
            aviso_line = f"<b>Mensagem:</b> {esc(headline)}"

            if municipios:
                mun_txt = format_municipios_list(municipios)
                footer = f"<b>Municípios ({len(municipios)}):</b> {esc(mun_txt)}"
            else:
                footer = f"<b>Área:</b> Polígono: {esc(area_desc_txt)}"

            msg = "\n".join([title_line, sender_line, aviso_line, footer])

            for part in chunk_text(msg):
                tg_send_message(part)

            time.sleep(0.25)

    for e in entries:
        eid = str(e.get("entry_id") or "").strip()
        if eid:
            seen.add(eid)

    seen_list = list(seen)
    if len(seen_list) > 5000:
        seen_list = seen_list[-5000:]

    state["seen_ids"] = seen_list
    state["last_run_utc"] = now_utc.isoformat(timespec="seconds")

    save_state(state)
    print(f"Estado salvo em {STATE_PATH}. Total vistos: {len(state['seen_ids'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
