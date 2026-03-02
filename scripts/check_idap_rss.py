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
from typing import Dict, List, Optional, Set


RSS_URL = os.environ.get("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap").strip()
STATE_PATH = os.environ.get("STATE_PATH", "state/seen.json").strip()

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "45"))
HTTP_RETRIES = int(os.environ.get("HTTP_RETRIES", "5"))
HTTP_RETRY_SLEEP_SECONDS = float(os.environ.get("HTTP_RETRY_SLEEP_SECONDS", "2.0"))


def http_get(url: str, timeout: int = HTTP_TIMEOUT) -> bytes:
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "github-actions-idap-atom-monitor/1.2"},
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


def tg_send_message(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("Telegram não configurado. Vou só imprimir a mensagem.")
        print(text)
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _ = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Telegram sendMessage falhou: HTTP {e.code}: {body}") from e


def chunk_text(text: str, limit: int = 3900) -> List[str]:
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
        # Severo: combinação exata
        if (u == "Expected") and (c in {"Likely", "Observed"}) and (r in {"Execute", "Prepare"}):
            return "Severo"
        return "Alto"

    return "Indefinido"


def parse_atom_feed(feed_xml: bytes) -> List[Dict[str, str]]:
    root = ET.fromstring(feed_xml)
    out: List[Dict[str, str]] = []

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
            }
        )

    return out


def main() -> int:
    if not RSS_URL:
        print("RSS_URL vazio.")
        return 2

    now_utc = datetime.now(timezone.utc)
    state = load_state()
    seen: Set[str] = set(state.get("seen_ids", []) or [])

    print(f"Baixando feed: {RSS_URL}")
    feed_xml = http_get(RSS_URL)
    entries = parse_atom_feed(feed_xml)

    print(f"Entries no feed: {len(entries)}")

    new_entries = [e for e in entries if e.get("entry_id") and e["entry_id"] not in seen]
    print(f"Novos desde a última execução: {len(new_entries)}")

    if new_entries:
        def sort_key(e: Dict[str, str]) -> str:
            return e.get("onset") or e.get("entry_updated") or ""

        new_entries_sorted = sorted(new_entries, key=sort_key)

        lines: List[str] = []
        lines.append(f"Alertas enviados: {len(new_entries_sorted)}")

        for e in new_entries_sorted:
            nivel = calc_nivel(
                e.get("severity", ""),
                e.get("urgency", ""),
                e.get("certainty", ""),
                e.get("responseType", ""),
            )

            onset = (e.get("onset") or e.get("entry_updated") or "-").strip()
            sender = truncate(e.get("senderName") or "-", 120)
            event = truncate(e.get("event") or "-", 60)
            headline = truncate(e.get("headline") or "-", 280)

            lines.append(f"[{onset}][Nível {nivel}][{sender}][{event}][{headline}]")

        msg = "\n".join(lines).strip()
        for part in chunk_text(msg):
            tg_send_message(part)

    for e in entries:
        eid = (e.get("entry_id") or "").strip()
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
