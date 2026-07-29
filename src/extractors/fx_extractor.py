import requests, pandas as pd
from datetime import date, timedelta
from typing import Optional
from src.extractors.base import BaseExtractor, ExtractResult

class FxExtractor(BaseExtractor):
    def __init__(self, source_config, connection_config=None):
        super().__init__(source_config)

    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        start = date.fromisoformat(cfg.get("start_date", "2025-06-01"))
        end = date.today()
        cur = cfg.get("currency", "vnd")
        batch_days = cfg.get("batch_days", 30)
        stream = cfg.get("stream_to_staging")

        if stream:
            from sqlalchemy import create_engine
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))
            schema, table = cfg["target_staging_table"].split(".")

        buf, total, page, d = [], 0, 0, start
        def flush():
            nonlocal buf, total, page
            if not buf: return
            df = pd.DataFrame(buf); df["_source_id"] = cfg["source_id"]
            df.to_sql(table, dwh, schema=schema, index=False,
                      if_exists=("replace" if page == 0 else "append"))
            page += 1; total += len(buf); buf = []
            print(f"[{cfg['source_id']}] batch {page}: tổng {total}", flush=True)

        while d <= end:
            ds = d.isoformat()
            url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{ds}/v1/currencies/usd.json"
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    v = r.json().get("usd", {}).get(cur)
                    if v: buf.append({"record_date": ds, "exchange_rate": v})
            except Exception:
                pass
            if stream and len(buf) >= batch_days:
                flush()
            d += timedelta(days=1)

        if stream:
            flush()
            return ExtractResult(dataframe=pd.DataFrame(), row_count=total,
                                 checksum=None, watermark_value=None,
                                 source_meta={"currency": cur, "streamed": True})
        df = pd.DataFrame(buf); df["_source_id"] = cfg["source_id"]
        return ExtractResult(dataframe=df, row_count=len(df), checksum=None,
                             watermark_value=None, source_meta={"currency": cur})

    def has_changed(self, last):
        return True