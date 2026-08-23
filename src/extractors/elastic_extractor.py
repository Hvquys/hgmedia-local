import logging, requests, pandas as pd, json, gc
from typing import Optional
from src.extractors.base import BaseExtractor, ExtractResult
log = logging.getLogger(__name__)

class ElasticExtractor(BaseExtractor):
    def __init__(self, source_config, connection_config):
        super().__init__(source_config)
        self.cc = connection_config

    def _auth(self):
        u, p = self.cc.get("user"), self.cc.get("password")
        return (u, p) if u else None

    def extract(self, watermark_filter=None):
        cfg = self.source_config
        index = cfg["es_index"]
        size = cfg.get("page_size", 5000)
        base = self.cc["host"].rstrip("/")
        url = f"{base}/{index}/_search"
        q = {"match_all": {}}
        if cfg.get("date_from"):
            q = {"range": {cfg.get("date_field", "Date"): {"gte": cfg["date_from"]}}}
        sf = cfg.get("sort_field", "_id")
        body = {"size": size, "query": q, "sort": [{sf: "asc"}, {"_doc": "asc"}]}
        auth = self._auth()

        stream = cfg.get("stream_to_staging")
        if stream:
            from sqlalchemy import create_engine
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))
            schema, table = cfg["target_staging_table"].split(".")

        TIMESTAMP_SUFFIXES = ("At", "Date", "Time", "Utc", "UTC")

        def clean(df):
            for c in df.columns:
                # dict/list → json string
                if df[c].map(lambda v: isinstance(v, (dict, list))).any():
                    df[c] = df[c].map(
                        lambda v: json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (dict, list)) else v
                    )
                # suffix gợi ý datetime → ép kiểu
                if any(c.endswith(s) for s in TIMESTAMP_SUFFIXES):
                    df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
            df["_source_id"] = cfg["source_id"]
            return df

        def get_dtype_map(df):
            from sqlalchemy import Text
            from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TS
            dtype = {}
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    dtype[c] = PG_TS(timezone=True)
                elif df[c].dtype == object:
                    dtype[c] = Text()
            return dtype

        rows, after, total, page = [], None, 0, 0
        dtype_map = None  # tính 1 lần ở page đầu, tái dùng cho các page sau

        while True:
            if after:
                body["search_after"] = after
            r = requests.post(url, json=body, auth=auth, timeout=120)
            r.raise_for_status()
            hits = r.json()["hits"]["hits"]
            if not hits:
                break
            page += 1
            batch = [{**h["_source"], "_es_id": h["_id"]} for h in hits]
            total += len(hits)
            after = hits[-1]["sort"]

            if stream:
                df = clean(pd.json_normalize(batch))
                if dtype_map is None:
                    dtype_map = get_dtype_map(df)  # tính 1 lần duy nhất từ page 1
                df.to_sql(
                    table, dwh, schema=schema, index=False,
                    if_exists=("replace" if page == 1 else "append"),
                    method="multi", chunksize=1000,
                    dtype=dtype_map,              # ← luôn truyền dtype_map
                )
                log.info(f"[{cfg['source_id']}] page {page}: +{len(hits)} (tổng {total}) -> staging")
                del df, batch, r
                gc.collect()
            else:
                rows += batch

            if len(hits) < size:
                break

        if stream:
            return ExtractResult(
                dataframe=pd.DataFrame(), row_count=total,
                checksum=None, watermark_value=None,
                source_meta={"index": index, "streamed": True}
            )
        df = clean(pd.json_normalize(rows)) if rows else pd.DataFrame()
        return ExtractResult(
            dataframe=df, row_count=len(df), checksum=None,
            watermark_value=None, source_meta={"index": index}
        )

    def has_changed(self, last):
        return True