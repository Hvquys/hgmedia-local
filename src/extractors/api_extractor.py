"""Extractor cho API phân trang, hỗ trợ chia khoảng theo tháng."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from src.extractors.base import BaseExtractor, ExtractResult


logger = logging.getLogger(__name__)


class ApiExtractor(BaseExtractor):
    """Đọc API về DataFrame để task chung lưu Bronze trước khi load Staging."""

    def extract(self, watermark_filter=None) -> ExtractResult:
        if self.source_config.get("stream_to_staging"):
            raise ValueError(
                "ApiExtractor không hỗ trợ stream_to_staging. "
                "Dữ liệu phải đi qua Bronze để có thể retry và đối soát."
            )

        if self.source_config.get("parallel_by") == "month":
            return self._extract_parallel_monthly()

        dataframe, page_count = self._fetch_pages(
            self.source_config.get("params", {}).copy()
        )
        return self._build_result(
            dataframe,
            {"parallel": False, "pages": page_count},
        )

    def _get_month_ranges(self) -> list[tuple[str, str]]:
        date_from = self.source_config.get("date_from", "2025-06-01")
        start = datetime.strptime(date_from, "%Y-%m-%d").replace(day=1)
        end = datetime.now().replace(day=1) + relativedelta(months=1)

        ranges = []
        current = start
        while current < end:
            following = current + relativedelta(months=1)
            ranges.append(
                (current.strftime("%Y-%m-%d"), following.strftime("%Y-%m-%d"))
            )
            current = following
        return ranges

    def _extract_parallel_monthly(self) -> ExtractResult:
        cfg = self.source_config
        ranges = self._get_month_ranges()
        from_param = cfg.get("api_date_from_param", "fromDate")
        to_param = cfg.get("api_date_to_param", "toDate")
        max_workers = max(1, int(cfg.get("parallel_workers", 3)))
        frames_by_month = {}
        pages_by_month = {}
        failures = []

        def fetch_month(date_from: str, date_to: str):
            params = cfg.get("params", {}).copy()
            params[from_param] = date_from
            params[to_param] = date_to
            dataframe, pages = self._fetch_pages(
                params,
                month_label=date_from[:7],
            )
            return date_from, dataframe, pages

        logger.info(
            "[%s] Đọc song song %s tháng với %s worker",
            cfg["source_id"],
            len(ranges),
            max_workers,
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_month, date_from, date_to): date_from
                for date_from, date_to in ranges
            }
            for future in as_completed(futures):
                month = futures[future][:7]
                try:
                    date_from, dataframe, pages = future.result()
                    frames_by_month[date_from] = dataframe
                    pages_by_month[date_from] = pages
                    logger.info(
                        "[%s] %s hoàn tất: %s dòng",
                        cfg["source_id"],
                        month,
                        len(dataframe),
                    )
                except Exception as error:
                    failures.append((month, error))
                    logger.exception(
                        "[%s] %s thất bại",
                        cfg["source_id"],
                        month,
                    )

        if failures:
            details = "; ".join(
                f"{month}: {type(error).__name__}: {error}"
                for month, error in sorted(failures)
            )
            raise RuntimeError(
                f"[{cfg['source_id']}] API extraction thất bại; "
                f"không tạo batch một phần. {details}"
            )

        ordered_frames = [
            frames_by_month[date_from]
            for date_from, _ in ranges
            if not frames_by_month[date_from].empty
        ]
        dataframe = (
            pd.concat(ordered_frames, ignore_index=True)
            if ordered_frames
            else pd.DataFrame()
        )
        return self._build_result(
            dataframe,
            {
                "parallel": True,
                "months": len(ranges),
                "pages": sum(pages_by_month.values()),
            },
        )

    def _fetch_pages(
        self,
        params: dict,
        month_label: str = "",
    ) -> tuple[pd.DataFrame, int]:
        cfg = self.source_config
        data_key = cfg.get("data_key")
        scroll_key = cfg.get("scroll_key")
        has_more_key = cfg.get("has_more_key")
        max_pages = max(1, int(cfg.get("max_pages", 10_000)))
        request_params = params.copy()
        rows = []
        seen_scroll_values = set()

        for page_number in range(1, max_pages + 1):
            response = requests.get(
                cfg["api_url"],
                params=request_params,
                headers=cfg.get("headers", {}),
                verify=cfg.get("verify_ssl", True),
                timeout=cfg.get("timeout_seconds", 120),
            )
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                if data_key:
                    if data_key not in payload:
                        raise KeyError(f"API response thiếu data_key '{data_key}'")
                    page_rows = payload[data_key]
                else:
                    raise ValueError(
                        "API response là object nhưng config chưa khai báo data_key"
                    )
            elif isinstance(payload, list):
                page_rows = payload
            else:
                raise TypeError(
                    f"API response phải là object hoặc list, nhận {type(payload).__name__}"
                )

            if page_rows is None:
                page_rows = []
            if not isinstance(page_rows, list):
                raise TypeError(
                    f"API field '{data_key}' phải là list, nhận "
                    f"{type(page_rows).__name__}"
                )
            rows.extend(page_rows)
            logger.info(
                "[%s] %s page %s: +%s dòng",
                cfg["source_id"],
                month_label,
                page_number,
                len(page_rows),
            )

            has_more = bool(
                has_more_key
                and isinstance(payload, dict)
                and payload.get(has_more_key, False)
            )
            if not has_more:
                return pd.json_normalize(rows), page_number
            if not scroll_key:
                raise ValueError("API báo còn trang nhưng config thiếu scroll_key")

            scroll_value = payload.get(scroll_key)
            if not scroll_value:
                raise ValueError(
                    f"API báo còn trang nhưng response thiếu scroll_key '{scroll_key}'"
                )
            if scroll_value in seen_scroll_values:
                raise RuntimeError(
                    f"API lặp scroll token tại page {page_number}: {scroll_value}"
                )
            seen_scroll_values.add(scroll_value)
            request_params[scroll_key] = scroll_value

        raise RuntimeError(f"API vượt giới hạn max_pages={max_pages}")

    def _build_result(self, dataframe: pd.DataFrame, source_meta: dict) -> ExtractResult:
        dataframe = dataframe.copy()
        dataframe["_source_id"] = self.source_config["source_id"]
        return ExtractResult(
            dataframe=dataframe,
            row_count=len(dataframe),
            checksum=None,
            watermark_value=None,
            source_meta=source_meta,
        )

    def has_changed(self, last_checksum_or_watermark) -> bool:
        return True
