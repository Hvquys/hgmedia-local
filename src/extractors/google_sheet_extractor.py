"""
extractors/google_sheet_extractor.py
Đọc dữ liệu từ Google Sheet (thay cho ExcelExtractor cũ).
Dùng gspread + service account credential.

Config mẫu (xem config/google_sheet_sources.yaml):
  source_id: dim_partners
  source_type: google_sheet
  spreadsheet_id: "1AbCdEfGhIjKlMnOpQrStUvWxYz_PLACEHOLDER"
  worksheet_name: "Thông tin các kho nhạc BD"
  header_row: 1
  data_start_row: 2
  field_mapping: [...]
"""
import hashlib
import json
from typing import Optional
import logging
log = logging.getLogger(__name__)
import pandas as pd

from src.extractors.base import BaseExtractor, ExtractResult

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]


class GoogleSheetExtractor(BaseExtractor):

    def __init__(self, source_config: dict, connection_config: dict):
        super().__init__(source_config)
        self.connection_config = connection_config
        self._client = None

    def _get_client(self):
        if self._client is None:
            if gspread is None:
                raise ImportError("Cần cài gspread + google-auth: pip install gspread google-auth")
            creds_path = self.connection_config["credentials_json_path"]
            creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
            self._client = gspread.authorize(creds)
        return self._client

    def _drive_service(self):
        from googleapiclient.discovery import build
        creds = Credentials.from_service_account_file(
            self.connection_config["credentials_json_path"], scopes=SCOPES)
        return build("drive", "v3", credentials=creds)

    def _read_raw_values(self):
        import io, openpyxl
        from googleapiclient.http import MediaIoBaseDownload
        cfg = self.source_config

        # CHẾ ĐỘ 1: 1 Google Sheet native theo spreadsheet_id -> dùng gspread
        if cfg.get("spreadsheet_id"):
            import re
            client = self._get_client()
            sh = client.open_by_key(cfg["spreadsheet_id"])
            pat = cfg.get("worksheet_pattern")
            tabs = [w for w in sh.worksheets() if pat in w.title] if pat \
                else [sh.worksheet(cfg["worksheet_name"]) if cfg.get("worksheet_name") else sh.sheet1]

            WANT = cfg.get("columns")
            hidx = cfg.get("header_row", 1) - 1
            out = []
            for ws in tabs:
                mt = re.search(r"(20\d{2})", ws.title)
                yr = mt.group(1) if mt else ""
                vals = ws.get_all_values()
                if len(vals) <= hidx:
                    continue
                hdr = [str(h).replace("\n", " ").strip() for h in vals[hidx]]
                if WANT:
                    pos = {c: hdr.index(c) for c in WANT if c in hdr}
                    if not out:
                        out.append(list(WANT) + ["_year"])
                    for r in vals[hidx + 1:]:
                        row = [r[pos[c]] if c in pos and pos[c] < len(r) else "" for c in WANT]
                        if any(x.strip() for x in row[:5]):
                            out.append(row + [yr])
                else:
                    if not out:
                        out.append(hdr + ["_year"])
                    for r in vals[hidx + 1:]:
                        out.append(r + [yr])
            return out

        # CHẾ ĐỘ 2: folder .xlsm/.xlsx -> download qua Drive
        XLSX = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-excel.sheet.macroenabled.12"]
        drive = self._drive_service()
        resp = drive.files().list(
            q=f"'{cfg['folder_id']}' in parents and trashed=false",
            fields="files(id,name,mimeType)", pageSize=1000,
            supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        # thêm log để xem tất cả file trong folder kể cả không phải xlsx
        resp = drive.files().list(
            q=f"'{cfg['folder_id']}' in parents and trashed=false",
            fields="files(id,name,mimeType)", pageSize=1000,
            supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        log.info(f"Tất cả file trong folder: {[(f['name'], f['mimeType']) for f in resp['files']]}")
        files = [f for f in resp["files"] if f["mimeType"] in XLSX]
        log.info(f"[{cfg['source_id']}] Tìm thấy {len(files)} file trong folder")
        for f in files:
            log.info(f"[{cfg['source_id']}] File: {f['name']}")

        # filter theo file_name nếu có
        if cfg.get("file_name"):
            files = [f for f in files if f["name"] == cfg["file_name"]]

        # đọc columns từ config, fallback về hardcode cũ
        WANT = cfg.get("columns") or ["Mã", "Tiêu đề", "Nghệ sĩ bài hát"]
        KEY = cfg.get("header_key", "mã")



        rows = []
        headers = None
        for idx, f in enumerate(files, 1):
            buf = io.BytesIO()
            dl = MediaIoBaseDownload(buf, drive.files().get_media(fileId=f["id"], supportsAllDrives=True))
            done = False
            while not done:
                _, done = dl.next_chunk()
            buf.seek(0)
            wb = openpyxl.load_workbook(buf, data_only=True, read_only=True)
            log.info(f"[{cfg['source_id']}] Các sheet trong {f['name']}: {wb.sheetnames}")
            # chọn sheet theo worksheet_name nếu có, không thì lấy sheet đầu
            if cfg.get("worksheet_name") and cfg["worksheet_name"] in wb.sheetnames:
                ws = wb[cfg["worksheet_name"]]
            else:
                ws = wb[wb.sheetnames[0]]

            vals = [[("" if c is None else str(c).strip()) for c in r] for r in ws.iter_rows(values_only=True)]
            # thêm trước dòng hidx = next(...)
            log.info(f"[{cfg['source_id']}] Các dòng đầu của {f['name']}: {[vals[i][:5] for i in range(min(5, len(vals)))]}")
            # tìm dòng header theo KEY
            hidx = next((i for i, r in enumerate(vals) if KEY in [c.lower() for c in r]), None)
            if hidx is None:
                log.warning(f"[{cfg['source_id']}] SKIP {f['name']} (không thấy header '{KEY}')")
                continue

            hdr = [c for c in vals[hidx]]
            # thêm sau dòng hdr = [c for c in vals[hidx]]
            log.info(f"[{cfg['source_id']}] Headers trong {f['name']}: {hdr[:10]}")
            hdr_lower = [c.lower() for c in hdr]
            pos = {w: hdr_lower.index(w.lower()) for w in WANT if w.lower() in hdr_lower}

            repo = f["name"].rsplit(".", 1)[0]

            if headers is None:
                headers = WANT + ["Repository"]

            for r in vals[hidx + 1:]:
                if not any((r[i].strip() if i < len(r) else "") for i in pos.values()):
                    continue
                d = {w: (r[pos[w]] if w in pos and pos[w] < len(r) else "") for w in WANT}
                d["Repository"] = repo
                rows.append(d)

            log.info(f"[{cfg['source_id']}] ({idx}/{len(files)}) {f['name']}: +{len(vals)-hidx-1} dòng")

        if headers is None:
            headers = WANT + ["Repository"]

        return [headers] + [[d.get(h, "") for h in headers] for d in rows]

    def extract(self) -> ExtractResult:
        cfg = self.source_config

        raw_values = self._read_raw_values()
        if cfg.get("worksheet_pattern") or cfg.get("folder_id") or cfg.get("columns"):
            headers, data_rows = raw_values[0], raw_values[1:]
        else:
            headers = raw_values[cfg.get("header_row",1)-1]
            data_rows = raw_values[cfg.get("data_start_row",2)-1:]

        headers = [str(h).replace("\n", " ").strip() for h in headers]
        seen = {}
        for i, h in enumerate(headers):
            h = h or f"col_{i}"
            if h in seen: seen[h] += 1; headers[i] = f"{h}_{seen[h]}"
            else: seen[h] = 0
        n = len(headers)
        data_rows = [(r + [""] * n)[:n] for r in data_rows]
        df = pd.DataFrame(data_rows, columns=headers)

        # Áp field_mapping nếu có khai báo (đổi tên cột source -> target + ép kiểu cơ bản)
        field_mapping = cfg.get("field_mapping") or []
        if field_mapping:
            rename_map = {fm["source"]: fm["target"] for fm in field_mapping}
            keep_cols = [c for c in rename_map if c in df.columns]
            df = df[keep_cols].rename(columns=rename_map)

        # Thêm metadata chuẩn
        df["_source_id"] = cfg["source_id"]
        df["_source_sheet"] = cfg.get("worksheet_name") or cfg.get("folder_id")

        checksum = self._compute_checksum(raw_values)

        return ExtractResult(
            dataframe=df,
            row_count=len(df),
            checksum=checksum,
            watermark_value=None,
            source_meta={
                "spreadsheet_id": cfg.get("spreadsheet_id") or cfg.get("folder_id"),
                "worksheet_name": cfg.get("worksheet_name"),
            },
        )

    def has_changed(self, last_checksum_or_watermark: Optional[str]) -> bool:
        if last_checksum_or_watermark is None:
            return True
        raw_values = self._read_raw_values()
        current_checksum = self._compute_checksum(raw_values)
        return current_checksum != last_checksum_or_watermark

    @staticmethod
    def _compute_checksum(raw_values: list) -> str:
        payload = json.dumps(raw_values, ensure_ascii=False, sort_keys=False)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()
