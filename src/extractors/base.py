"""
extractors/base.py
Mọi nguồn (Google Sheet, SQL DB, sau này Elasticsearch...) đều implement interface này.
Task Load (load_task.py) chỉ làm việc qua interface chung, không cần biết nguồn gốc.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
import pandas as pd


@dataclass
class ExtractResult:
    """Kết quả trả về sau khi 1 Extractor đọc xong dữ liệu nguồn."""
    dataframe: pd.DataFrame
    row_count: int
    checksum: Optional[str] = None          # dùng cho nguồn không có watermark (vd Google Sheet)
    watermark_value: Optional[str] = None    # dùng cho nguồn SQL có cột updated_at/write_date
    source_meta: Optional[dict] = None       # thông tin phụ: tên sheet, tên file, query đã chạy...


class BaseExtractor(ABC):
    """Interface chung. Mỗi loại nguồn implement 2 method bắt buộc."""

    def __init__(self, source_config: dict):
        self.source_config = source_config
        self.source_id = source_config["source_id"]

    @abstractmethod
    def extract(self) -> ExtractResult:
        """Đọc dữ liệu thô từ nguồn, trả về DataFrame + metadata. KHÔNG áp business rule."""
        raise NotImplementedError

    @abstractmethod
    def has_changed(self, last_checksum_or_watermark: Optional[str]) -> bool:
        """
        Kiểm tra nguồn có thay đổi kể từ lần extract trước không.
        - Google Sheet/Excel: so sánh md5 nội dung.
        - SQL: so sánh có row nào mới hơn watermark_column hiện tại không.
        Trả về True nếu cần extract lại, False nếu skip.
        """
        raise NotImplementedError
