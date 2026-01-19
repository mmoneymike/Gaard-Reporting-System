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


@dataclass(frozen=True)
class CumulativeReturnResults:
    positions: pd.DataFrame
    buckets: pd.DataFrame


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


def _clean_symbol(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    cleaned = symbol.strip().upper()
    return cleaned or None


def _coerce_float(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace(",", "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return float(cleaned)
    except ValueError:
        return None


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


def fetch_benchmark_returns_wrds(
    connection,
    tickers: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Fetch daily total returns for benchmarks using WRDS (CRSP).

    Returns a DataFrame indexed by date with tickers as columns.
    """
    clean_tickers = sorted({t.strip().upper() for t in tickers if t and t.strip()})
    if not clean_tickers:
        return pd.DataFrame()

    ticker_list = ", ".join([f"'{ticker}'" for ticker in clean_tickers])
    date_filters = []
    if start_date:
        date_filters.append(f"dsf.date >= '{start_date}'")
    if end_date:
        date_filters.append(f"dsf.date <= '{end_date}'")
    date_clause = f"AND {' AND '.join(date_filters)}" if date_filters else ""

    query = f"""
        SELECT
            dsf.date,
            sn.ticker,
            dsf.ret
        FROM crsp.dsf AS dsf
        JOIN crsp.stocknames AS sn
          ON dsf.permno = sn.permno
         AND dsf.date BETWEEN sn.namedt AND sn.nameendt
        WHERE sn.ticker IN ({ticker_list})
        {date_clause}
        ORDER BY dsf.date, sn.ticker
    """

    raw = connection.raw_sql(query)
    if raw.empty:
        return pd.DataFrame()

    raw["ticker"] = raw["ticker"].str.upper()
    raw["date"] = pd.to_datetime(raw["date"])
    returns = raw.pivot(index="date", columns="ticker", values="ret").sort_index()
    return returns


def calculate_cumulative_returns_with_dividends(
    sections: StatementSections,
) -> CumulativeReturnResults:
    """
    Calculate cumulative returns for positions including dividends, plus bucket-level rollups.
    """
    if sections.positions.empty:
        return CumulativeReturnResults(positions=pd.DataFrame(), buckets=pd.DataFrame())

    positions = sections.positions.copy()
    symbol_series = (
        positions["Symbol"] if "Symbol" in positions.columns else pd.Series([None] * len(positions))
    )
    positions["Symbol"] = symbol_series.apply(_clean_symbol)
    positions = positions[positions["Symbol"].notna()].copy()
    quantity_series = (
        positions["Quantity"]
        if "Quantity" in positions.columns
        else pd.Series([None] * len(positions))
    )
    value_series = (
        positions["Value"] if "Value" in positions.columns else pd.Series([None] * len(positions))
    )
    positions["Quantity"] = quantity_series.apply(_coerce_float)
    positions["Value"] = value_series.apply(_coerce_float)

    trades = sections.trades.copy() if not sections.trades.empty else pd.DataFrame()
    if not trades.empty:
        trade_symbol_series = (
            trades["Symbol"] if "Symbol" in trades.columns else pd.Series([None] * len(trades))
        )
        trades["Symbol"] = trade_symbol_series.apply(_clean_symbol)
        trades = trades[trades["Symbol"].notna()].copy()
        buy_qty = (
            trades["Buy Quantity"]
            if "Buy Quantity" in trades.columns
            else pd.Series([None] * len(trades))
        )
        avg_buy_price = (
            trades["Avg. Buy Price"]
            if "Avg. Buy Price" in trades.columns
            else pd.Series([None] * len(trades))
        )
        buy_proceeds = (
            trades["Buy Proceeds"]
            if "Buy Proceeds" in trades.columns
            else pd.Series([None] * len(trades))
        )
        trades["Buy Quantity"] = buy_qty.apply(_coerce_float).fillna(0.0)
        trades["Avg. Buy Price"] = avg_buy_price.apply(_coerce_float).fillna(0.0)
        trades["Buy Proceeds"] = buy_proceeds.apply(_coerce_float).fillna(0.0)
        trades["trade_cost"] = trades["Buy Proceeds"].abs()
        trades["trade_cost"] = trades["trade_cost"].where(
            trades["trade_cost"] > 0,
            trades["Buy Quantity"] * trades["Avg. Buy Price"],
        )
        trade_summary = (
            trades.groupby("Symbol", dropna=True)["trade_cost"]
            .sum()
            .rename("cost_basis")
        )
    else:
        trade_summary = pd.Series(dtype="float64", name="cost_basis")

    dividends = sections.dividends.copy() if not sections.dividends.empty else pd.DataFrame()
    if not dividends.empty:
        if "Symbol" not in dividends.columns and "Description" in dividends.columns:
            dividends["Symbol"] = dividends["Description"].apply(extract_symbol_from_description)
        dividend_symbol_series = (
            dividends["Symbol"]
            if "Symbol" in dividends.columns
            else pd.Series([None] * len(dividends))
        )
        dividends["Symbol"] = dividend_symbol_series.apply(_clean_symbol)
        dividends = dividends[dividends["Symbol"].notna()].copy()
        amount_series = (
            dividends["Amount"]
            if "Amount" in dividends.columns
            else pd.Series([None] * len(dividends))
        )
        dividends["Amount"] = amount_series.apply(_coerce_float).fillna(0.0)
        dividend_summary = (
            dividends.groupby("Symbol", dropna=True)["Amount"].sum().rename("dividends")
        )
    else:
        dividend_summary = pd.Series(dtype="float64", name="dividends")

    positions = positions.merge(trade_summary, on="Symbol", how="left")
    positions = positions.merge(dividend_summary, on="Symbol", how="left")
    positions["cost_basis"] = positions["cost_basis"].fillna(0.0)
    positions["dividends"] = positions["dividends"].fillna(0.0)
    positions["Value"] = positions["Value"].fillna(0.0)
    positions["total_ending_value"] = positions["Value"] + positions["dividends"]
    positions["cumulative_return"] = positions.apply(
        lambda row: (row["total_ending_value"] / row["cost_basis"]) - 1.0
        if row["cost_basis"]
        else 0.0,
        axis=1,
    )

    bucket_summary = (
        positions.groupby("Bucket", dropna=True)[["cost_basis", "total_ending_value"]]
        .sum()
        .reset_index()
    )
    if not bucket_summary.empty:
        bucket_summary["cumulative_return"] = bucket_summary.apply(
            lambda row: (row["total_ending_value"] / row["cost_basis"]) - 1.0
            if row["cost_basis"]
            else 0.0,
            axis=1,
        )

    return CumulativeReturnResults(positions=positions, buckets=bucket_summary)
