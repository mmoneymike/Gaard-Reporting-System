import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SECTION_HEADER = "Header"
SECTION_DATA = "Data"


@dataclass(frozen=True)
class StatementSections:
    raw_sections: dict[str, pd.DataFrame]
    positions: pd.DataFrame
    dividends: pd.DataFrame
    trades: pd.DataFrame


def read_statement_csv(path: str | Path) -> dict[str, pd.DataFrame]:
    """
    Parse the statement CSV into section-based DataFrames.

    The statement uses:
      Column A = Section (e.g., Positions, Dividends)
      Column B = Record Type (Header/Data)
      Column C.. = Header fields or data values
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Statement CSV not found at {path}")

    headers: dict[str, list[str]] = {}
    rows: dict[str, list[dict[str, str]]] = defaultdict(list)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for raw_row in reader:
            if not raw_row:
                continue

            section = raw_row[0].strip()
            record_type = raw_row[1].strip() if len(raw_row) > 1 else ""

            if record_type == SECTION_HEADER:
                headers[section] = [cell.strip() for cell in raw_row[2:]]
                continue

            if record_type == SECTION_DATA:
                header = headers.get(section)
                values = [cell.strip() for cell in raw_row[2:]]
                if header:
                    padded = values + [""] * (len(header) - len(values))
                    row_dict = dict(zip(header, padded[: len(header)]))
                    rows[section].append(row_dict)
                else:
                    rows[section].append({f"col_{i}": v for i, v in enumerate(values)})

    frames: dict[str, pd.DataFrame] = {}
    for section, section_rows in rows.items():
        frames[section] = pd.DataFrame(section_rows)

    return frames


def extract_symbol_from_description(description: str) -> str | None:
    """
    Extract a symbol like 'ICSH' from a dividend description string.
    Example: 'ICSH(US46434V8789) Cash Dividend ...'
    """
    if not description:
        return None
    match = re.match(r"^([A-Z0-9\\.]+)\\(", description.strip())
    if not match:
        return None
    return match.group(1)


def normalize_model_bucket(model: str) -> str:
    if not model:
        return "Other"
    cleaned = model.strip().lower()
    if "domestic" in cleaned or "u.s" in cleaned or "us " in cleaned:
        return "U.S. Equities"
    if "international" in cleaned:
        return "International Equities"
    if "fixed" in cleaned or "income" in cleaned:
        return "Fixed Income"
    if "alternative" in cleaned:
        return "Alternative Assets"
    if "cash" in cleaned:
        return "Cash"
    return "Other"


def build_statement_sections(path: str | Path) -> StatementSections:
    sections = read_statement_csv(path)

    positions = sections.get("Positions", pd.DataFrame())
    dividends = sections.get("Dividends", pd.DataFrame())
    trades = sections.get("Trades", pd.DataFrame())

    if not positions.empty and "Model" in positions.columns:
        positions = positions.copy()
        positions["Bucket"] = positions["Model"].apply(normalize_model_bucket)

    if not dividends.empty and "Description" in dividends.columns:
        dividends = dividends.copy()
        dividends["Symbol"] = dividends["Description"].apply(extract_symbol_from_description)

    symbol_to_model = {}
    if not positions.empty and {"Symbol", "Model"}.issubset(positions.columns):
        symbol_to_model.update(
            positions.dropna(subset=["Symbol", "Model"])
            .drop_duplicates(subset=["Symbol"])
            .set_index("Symbol")["Model"]
            .to_dict()
        )
    if not trades.empty and {"Symbol", "Model"}.issubset(trades.columns):
        trade_map = (
            trades.dropna(subset=["Symbol", "Model"])
            .drop_duplicates(subset=["Symbol"])
            .set_index("Symbol")["Model"]
            .to_dict()
        )
        symbol_to_model.update(trade_map)

    if not dividends.empty and "Symbol" in dividends.columns:
        dividends["Model"] = dividends["Symbol"].map(symbol_to_model)
        dividends["Bucket"] = dividends["Model"].apply(normalize_model_bucket)

    return StatementSections(
        raw_sections=sections,
        positions=positions,
        dividends=dividends,
        trades=trades,
    )
