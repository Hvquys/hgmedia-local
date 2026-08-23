"""
extractors/factory.py
Chọn đúng Extractor theo source_type khai báo trong config.
Task Extract chỉ gọi get_extractor(), không cần biết chi tiết từng loại.
"""
from src.connections import get_connection
from src.extractors.google_sheet_extractor import GoogleSheetExtractor
from src.extractors.sql_extractor import SQLExtractor


def get_extractor(source_config: dict):
    source_type = source_config["source_type"]
    if source_type == "fx":
        from src.extractors.fx_extractor import FxExtractor
        return FxExtractor(source_config)
    
    if source_type == "csv":
        from src.extractors.csv_extractor import CsvExtractor
        return CsvExtractor(source_config)
    if source_type == "google_sheet":
        conn_cfg = get_connection("google_sheet")
        return GoogleSheetExtractor(source_config, conn_cfg)

    if source_type == "elastic":
        from src.extractors.elastic_extractor import ElasticExtractor
        return ElasticExtractor(source_config, get_connection(source_config["connection"]))
    
    if source_type == "sql":
        conn_cfg = get_connection(source_config["connection"])
        return SQLExtractor(source_config, conn_cfg)
    if source_type == "api":
        from src.extractors.api_extractor import ApiExtractor
        return ApiExtractor(source_config)

    # elasticsearch: tạm bỏ qua theo yêu cầu hiện tại
    raise ValueError(f"source_type '{source_type}' chưa được hỗ trợ trong giai đoạn này")
