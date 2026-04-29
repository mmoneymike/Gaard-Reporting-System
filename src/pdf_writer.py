import pandas as pd
import altair as alt
import re
from fpdf import FPDF
from fpdf.fonts import FontFace
import os
import datetime

# --- CONSTANTS & COLORS ---
C_BLUE_PRIMARY = (89, 120, 247)   # #5978F7
C_BLUE_LOGO    = (73, 106, 154)    # #496A94
C_LIGHT_BG     = (245, 247, 255)  # Light background color
C_GREY_BORDER  = (200, 200, 200)
C_TEXT_LIGHT_GREY = (180, 180, 180) # Footnotes, bottom-of-page disclosures
C_TEXT_GREY     = (100, 100, 100) # Benchmark rows, "Reportings as of …" lines
C_WHITE         = (255, 255, 255)  

# --- PAGE DISCLOSURES ---
# Page: Breakdown of Accounts
DISCLOSURE_BREAKDOWN = (
    "Portfolio and account performance are presented net of management fees and third-party commissions and are calculated using the Time-Weighted Return (TWR) method. " 
    "The total return reflects the consolidated net return across all underlying accounts. "
    "Account values and performance information are provided for informational purposes only and may be unreconciled, unaudited, and/or based on third-party sources. "
    "Please refer to monthly account statements from the custodian for finalized information. "
    "Past performance is no guarantee of future results."
)

# Page: Change in Portfolio Value
DISCLOSURE_NAV = (
    "Net Asset Value (NAV): Reflects Mark-to-Market (MTM) valuations, deposits, withdrawals, "
    "and dividends. MTM represents the current market value of assets. Interest includes earned "
    "income; Change in Interest Accruals reflects interest earned but not yet settled. Fees include "
    "management fees and third-party execution commissions via Interactive Brokers LLC. "
    "Please see disclosures for full descriptions. "
    "Account values and performance information are provided for informational purposes only and may be unreconciled, unaudited and/or based on third-party sources. "
    "Please refer to monthly account statements from the custodian for finalized information. "
    "Past performance is no guarantee of future results."
)

# Pages: Portfolio Performance by Allocation, Expanded Portfolio Performance by Allocation
DISCLOSURE_ALLOCATION = (
    "Portfolio and account performance are presented net of management fees and third-party commissions, calculated using the Time-Weighted Return (TWR) method. "
    "Asset class performance reflects weighted contributions to total portfolio return. "
    "Allocation percentages and market values reflect total portfolio positions. "
    "Benchmark returns reflect actual benchmark performance, net of fund expense and do not include trading costs. Please see disclosures for full descriptions. "
    "Account values and performance information are provided for informational purposes only and may be unreconciled, unaudited and/or based on third-party sources. "
    "Please refer to monthly account statements from the custodian for finalized information. "
    "Past performance is no guarantee of future results."
)

# Full ETF names for building dynamic benchmark descriptions in disclosures
_BENCHMARK_FULL_NAMES = {
    'SPY': 'SPDR S&P 500 ETF Trust',
    'AGG': 'iShares Core U.S. Aggregate Bond ETF',
}

def _build_benchmark_description(benchmark_key):
    """Converts '60% SPY / 40% AGG' into '60% SPDR S&P 500 ETF Trust / 40% iShares Core ...'."""
    parts = [p.strip() for p in benchmark_key.split('/')]
    expanded = []
    for part in parts:
        for ticker, full_name in _BENCHMARK_FULL_NAMES.items():
            part = part.replace(ticker, full_name)
        expanded.append(part)
    return ' / '.join(expanded)

# Page: Portfolio Overview
def _disclosure_performance(benchmark_key):
    bench_desc = _build_benchmark_description(benchmark_key)
    return (
        "Performance is presented net of management fees and third-party commissions and are calculated using the Time-Weighted Return (TWR) method. "
        f"The Benchmark is comprised of {bench_desc}. "
        "Benchmarks are net of fund expense and do not include trading costs. Please see disclosures for full descriptions. "
        "Account values and performance information are provided for informational purposes only and may be unreconciled, unaudited "
        "and/or provided from outside sources. Please refer to monthly account statements for finalized information. "
        "Returns greater than one year are annualized. Past performance is no guarantee of future results."
    )

# Page: Risk Analysis
def _disclosure_risk_metrics(benchmark_key):
    bench_desc = _build_benchmark_description(benchmark_key)
    return (
        "Portfolio and account performance are presented net of management fees and third-party commissions and are calculated using the Time-Weighted Return (TWR) method. "
        "Risk metrics are reported since inception. "
        f"The Benchmark is comprised of {bench_desc}. "
        "Benchmarks are net of fund expense and do not include trading costs. "
        "Relative risk and factor coefficients are statistical estimates based on historical data and should not be viewed as absolute predictors of future "
        "exposure or performance. Please see disclosures for full descriptions. "
        "Account values and performance information are provided for informational purposes only and may be unreconciled, unaudited "
        "and/or provided from outside sources. Please refer to monthly account statements for finalized information.Past performance is no guarantee of future results."
    )

BENCHMARK_DEFINITIONS = [
    ("SPDR S&P 500 ETF Trust (SPY)",
     "A market-capitalization-weighted index designed to track the 500 leading companies in leading "
     "industries of the U.S. economy. It represents approximately 80% coverage of available market "
     "capitalization. (Expense Ratio: 0.09%)"),

    ("iShares Core U.S. Aggregate Bond Index (AGG)",
     "A market-capitalization-weighted index representing a broad range of U.S. investment-grade "
     "fixed-income securities, including government, corporate, and mortgage-backed bonds. "
     "(Expense Ratio: 0.03%)"),

    ("iShares MSCI ACWI ex U.S. ETF (ACWX)",
     "Captures large- and mid-cap representation across 22 of 23 developed markets (excluding the "
     "U.S.) and 23 emerging markets countries. This index covers approximately 85% of the global "
     "equity opportunity set outside the United States. (Expense Ratio: 0.33%)"),

    ("IQ Hedge Multi-Strategy Tracker ETF (QAI)",
     "Designed to track the risk and return characteristics of multiple hedge fund investment "
     "styles -- including long/short equity, global macro, and fixed-income arbitrage -- using a "
     "rules-based, transparent methodology. (Expense Ratio: 0.76%)"),

    ("SPDR Bloomberg 1-3 Month T-Bill ETF (BIL)",
     "Tracks the market for U.S. Treasury Bills with a remaining maturity between one and three "
     "months. These securities are issued by the U.S. government and are considered among the "
     "highest-quality fixed-income assets. (Expense Ratio: 0.14%)"),
]

class PortfolioPDF(FPDF):
    def __init__(self, orientation='P', unit='mm', format='A4'):
        super().__init__(orientation, unit, format)
        # Flag to control if the standard blue header appears
        self.show_standard_header = False
        self.header_text = "PORTFOLIO REPORT" 
        self.suppress_footer = False
        self.text_logo_path = None
        self.logo_path = None

        # --- FONT LOADING ---
        self.main_font = 'Helvetica' # Default fallback
        try:
            self.add_font('Carlito', '', 'data/pdf_resources/fonts/Carlito-Regular.ttf', uni=True)  # Carlito is Metric-compatible with Calibri
            self.add_font('Carlito', 'B', 'data/pdf_resources/fonts/Carlito-Bold.ttf',uni=True)
            self.add_font('Carlito', 'I', 'data/pdf_resources/fonts/Carlito-Italic.ttf', uni=True)
            self.main_font = 'Carlito'
            print("   > Loaded custom Calibri/Carlito font from data/fonts folder.")
        except Exception as e:
            print(f"   > Warning: Could not load Calibri/Carlito fonts ({e}). Using Helvetica.")

    def header(self):
        if self.show_standard_header:
            # 1. Title Text
            self.set_y(10)
            self.set_font('Carlito', 'B', 16)
            self.set_text_color(*C_BLUE_LOGO)
            self.cell(0, 10, self.header_text, new_x="LMARGIN", new_y="NEXT", align='L')
            
            # 2. Thin Line Underneath
            self.set_draw_color(*C_BLUE_LOGO) 
            self.set_line_width(0.3)
            line_y = self.get_y()
            self.line(10, line_y, self.w - 12, line_y)
        
            self.ln(5)

    def footer(self):
        # Hide page # on fist and last page
        if self.page_no() == 1 or self.suppress_footer: 
            return
        
        self.set_y(-15)
        base_y = self.get_y()
        
        # 1. Thin Line
        self.set_draw_color(*C_BLUE_LOGO) 
        self.set_line_width(0.3)            
        line_y = base_y - 2
        self.line(10, line_y, self.w-12, line_y)

        # --- LOGO (RIGHT) ---
        logo_y = base_y + 2
        if self.text_logo_path and os.path.exists(self.text_logo_path):
            self.image(self.text_logo_path, x=self.w - 39, y=logo_y, h=8)

        # --- Confidential Text (LEFT) ---
        text_y = base_y + 3
        self.set_y(text_y)
        self.set_x(10) # Align to left margin
        self.set_font(self.main_font, "", 10)
        self.set_text_color(*C_BLUE_LOGO)
        self.cell(60, 5, "Proprietary & Confidential | Not for Distribution", align='L')

        # --- Page Number (CENTER) ---
        self.set_y(text_y)
        self.set_x(9) # Reset X to allow centering across entire page width
        self.set_font(self.main_font, 'I', 10)
        self.set_text_color(150, 150, 150)
        display_num = self.page_no()
        self.cell(0, 5, f'Page {display_num}', align='C')
        

def clean_display_name(name: str) -> str:
    """Strips custodian boilerplate from IBKR account names for PDF display."""
    n = str(name).strip()
    n = re.sub(r',?\s*Interactive Brokers LLC Custodian$', '', n, flags=re.IGNORECASE)
    n = re.sub(r'\s+of\s*$', '', n)
    return n.strip().rstrip(',').strip()


# Table uses f"{x:.1%}" (one decimal in percent). Raw ratios from MV/grand can sit
# slightly inside/outside the IPS band while still displaying the same as Min/Target
# (e.g. 9.995% prints 10.0%). Compare at that same 0.1% resolution so compliance
# matches what the PDF shows.
_IPS_BAND_COMPARE_DECIMALS = 3  # ratio rounded to 0.001 == 0.1%


def _ips_band_compliant(v_cur, v_min, v_max, ndigits=_IPS_BAND_COMPARE_DECIMALS):
    """True if current is within [min, max] after rounding to the PDF's one-decimal percent precision."""
    try:
        c = round(float(v_cur), ndigits)
        lo = round(float(v_min), ndigits)
        hi = round(float(v_max), ndigits)
    except (TypeError, ValueError):
        return False
    return lo <= c <= hi


#  === IPS COMPLIANCE TABLE DATA ===
def get_ips_table_data(pdf_info, summary_df):
    """Constructs data rows: (Category, Min, Max, Target, Current)"""
    rows = []
    
    def get_val(key, default=0.0):
        try: return float(pdf_info.get(key, default))
        except: return default

    def get_current(bucket_name):
        try:
            row = summary_df[summary_df['Name'] == bucket_name]
            if not row.empty: return row['Allocation'].iloc[0]
            return 0.0
        except: return 0.0

    # 1. US Equity
    us_min = get_val('page_4_ips_us_equity_range_min')
    us_max = get_val('page_4_ips_us_equity_range_max')
    us_tgt = get_val('page_4_ips_us_equity_target')
    us_cur = get_current('U.S. Equities')
    rows.append(('US Equities', us_min, us_max, us_tgt, us_cur))

    # 2. International Equities (single band from info_for_pdf.xlsx)
    intl_min = get_val('page_4_ips_non_us_equity_range_min')
    intl_max = get_val('page_4_ips_non_us_equity_range_max')
    intl_tgt = get_val('page_4_ips_non_us_equity_target')
    intl_cur = get_current('International Equities')
    rows.append(('International Equities', intl_min, intl_max, intl_tgt, intl_cur))

    # 3. Fixed Income (single band)
    fi_min = get_val('page_4_ips_fixed_income_range_min')
    fi_max = get_val('page_4_ips_fixed_income_range_max')
    fi_tgt = get_val('page_4_ips_fixed_income_target')
    fi_cur = get_current('Fixed Income')
    rows.append(('Fixed Income', fi_min, fi_max, fi_tgt, fi_cur))

    # 4. Alternatives
    alt_min = get_val('page_4_ips_alternatives_range_min')
    alt_max = get_val('page_4_ips_alternatives_range_max')
    alt_tgt = get_val('page_4_ips_alternatives_target')
    alt_cur = get_current('Alternative Assets')
    rows.append(('Alternatives', alt_min, alt_max, alt_tgt, alt_cur))

    # 5. Cash
    cash_min = get_val('page_4_ips_cash_range_min')
    cash_max = get_val('page_4_ips_cash_range_max')
    cash_tgt = get_val('page_4_ips_cash_target')
    cash_cur = get_current('Cash')
    rows.append(('Cash', cash_min, cash_max, cash_tgt, cash_cur))
    
    return rows


#  === IPS COMPLIANCE BOX & WHISKER PLOT ===
def generate_ips_chart(ips_rows):
    """Creates a 'Box and Whisker' style plot for IPS Compliance. Range (Grey Bar), Target (Black Tick), Current (Colored Point)."""
    if not ips_rows: return None
    
    # 1. Prepare Data
    data = []
    for cat, v_min, v_max, v_tgt, v_cur in ips_rows:
        is_compliant = _ips_band_compliant(v_cur, v_min, v_max)
        status_color = "#329632" if is_compliant else "#C83232" # Change to differentiate Compliance Status
        
        data.append({
            "Category": cat,
            "Min": v_min,
            "Max": v_max,
            "Target": v_tgt,
            "Current": v_cur
        })
    
    df = pd.DataFrame(data)
    
    # 2. Base Chart
    base = alt.Chart(df).encode(y=alt.Y("Category", title=None, sort=None))

    # A. The Range (Grey Bar)
    ranges = base.mark_rule(size=3, color="#D3D3D3").encode(
        x=alt.X("Min", title="Allocation", axis=alt.Axis(format="%")),
        x2="Max"
    )
    
    # B. The Target (Black Tick)
    targets = base.mark_tick(thickness=2, color="black", size=15).encode(
        x="Target"
    )
    
    # C. The Current (Colored Point)
    currents = base.mark_circle(size=120, opacity=1).encode(x="Current")

    # Combine Layers
    chart = (ranges + targets + currents).properties(
        width=800, height=200
    ).configure_axis(
        labelFont='Calibri', titleFont='Calibri', titleFontSize=15, labelFontSize=15
    ).configure_legend(
        labelFont='Calibri', titleFont='Calibri', titleFontSize=15, labelFontSize=15
    ).configure_text(
        font='Calibri', fontSize=15
    )

    chart_path = "temp_ips_chart.png"
    chart.save(chart_path, scale_factor=3.0, engine="vl-convert")
    return chart_path
    

# === PORTFOLIO RETURN CHART ===
def generate_line_chart(comparison_df):
    """Line Chart: Portfolio vs Benchmark (Dynamic Name)."""
    if comparison_df is None or comparison_df.empty: return None
    
    # 1. No Melt Needed
    # The new prepare_chart_data sends us data that is ALREADY in long format:
    # Columns: ['date', 'Cumulative Return', 'Series']
    source = comparison_df.copy()
    
    # 2. Determine Domain Dynamically
    # We expect 'Series' column to contain ['Portfolio', 'YOUR_BENCHMARK_NAME']
    series_names = source['Series'].unique().tolist()
    
    # Rename any non-Portfolio series to "Benchmark" for display
    if 'Portfolio' in series_names:
        series_names.remove('Portfolio')
        original_bench_name = series_names[0] if series_names else "Benchmark"
        source['Series'] = source['Series'].replace(original_bench_name, 'Benchmark')
        domain = ['Portfolio', 'Benchmark']
    else:
        domain = series_names

    range_colors = ['#5978F7', '#7F7F7F'] 
    
    chart = alt.Chart(source).mark_line(strokeWidth=3).encode(
        x=alt.X('date:T', 
                title=None, 
                axis=alt.Axis(format='%b %Y', labelAngle=0, tickCount=6, labelColor='black', grid=False)
        ), 
        y=alt.Y('Cumulative Return:Q', 
                title=None, 
                axis=alt.Axis(format='%', grid=True, labelColor='black')
        ), 
        color=alt.Color('Series:N', 
                        scale=alt.Scale(domain=domain, range=range_colors), 
                        legend=alt.Legend(title=None, orient='none', legendX=185, legendY=276, direction='horizontal', labelColor='black')
        )
    ).properties(
        width=500, 
        height=250
    ).configure_axis(
        labelFont='Calibri', titleFont='Calibri', labelFontSize=12
    ).configure_legend(
        labelFont='Calibri', titleFont='Calibri', labelFontSize=12
    ).configure_view(
        strokeWidth=0
    )
    
    chart.save("temp_line_chart.png", scale_factor=3.0, engine="vl-convert")
    return "temp_line_chart.png"
    
    
#  === ASSET ALLOCATION CHART CREATION ===
def generate_donut_chart(summary_df):
    """Generates a Donut chart with ~300 width, max 500 height, and large Legend labels."""
    source = summary_df[
        (summary_df['Type'] == 'Bucket') & 
        (summary_df['Name'] != 'Other')
    ].copy()
    
    if source.empty: return None

    # PREPARE LEGEND LABELS
    # Combine Name + Allocation % for the Legend
    source['LegendLabel'] = source.apply(
        lambda row: f"{row['Name']}: {row['Allocation']:.1%}", axis=1
    )
    
    # Sort Descending
    source = source.sort_values('MarketValue', ascending=False)
    
    # Colors
    domain = source['LegendLabel'].tolist()
    range_colors = ['#5978F7', '#0070C0', '#2F5597', '#BDD7EE', '#7F7F7F', '#D9D9D9']
    
    base = alt.Chart(source).encode(theta=alt.Theta("MarketValue", stack=True))

    # PIE CHART (No Labels on Chart)
    pie = base.mark_arc(innerRadius=60, outerRadius=100).encode(
        color=alt.Color(
            "LegendLabel", 
            scale=alt.Scale(domain=domain, range=range_colors),
            legend=alt.Legend(
                title="Asset Class", 
                orient="bottom", 
                columns=2,          
                labelLimit=0,       
                columnPadding=10,
                
                # --- FONT CONFIGURATION ---
                labelFont='Calibri',
                titleFont='Calibri',
                titleFontSize=14,   
                labelFontSize=14,   
                symbolSize=200,     
                rowPadding=10
            ) 
        ),
        order=alt.Order('MarketValue', sort="descending"),
        tooltip=["Name", "MarketValue", alt.Tooltip("Allocation", format=".2%")]
    )
    
    # CONFIGURE
    chart = pie.properties(
        width=320,   
        height=220
    ).configure_view(
        strokeWidth=0
    )

    chart_path = "temp_chart.png"
    chart.save(chart_path, scale_factor=3.0, engine="vl-convert")
    return chart_path


#  ==========================================
#   OVERALL PORTFOLIO REPORT
#  ==========================================
def write_portfolio_report(summary_df, holdings_df, key_statistics, total_metrics, risk_metrics, report_date, output_path, account_title="Total Portfolio",
                           performance_windows=None, benchmark_performance_windows=None, performance_chart_data=None, quarter_label="Quarter", main_benchmark_key="60% SPY / 40% AGG",
                           legal_notes=None, pdf_info=None, text_logo_path=None, logo_path=None, portfolio_inception_date=None,
                           consolidated_breakdown_rows=None, page_visibility=None):
    
    print(f"   > Generating PDF Report: {output_path}")
    if pdf_info is None: pdf_info = {}
    if page_visibility is None: page_visibility = {}
    vis = lambda key: page_visibility.get(key, True)

    if consolidated_breakdown_rows is None:
        consolidated_breakdown_rows = []
    else:
        consolidated_breakdown_rows = [
            r for r in consolidated_breakdown_rows
            if r is not None and getattr(r, "account_number", "").strip()
        ]
    
    # Build per-client disclosure text from the benchmark key
    disclosure_performance = _disclosure_performance(main_benchmark_key)
    disclosure_risk_metrics = _disclosure_risk_metrics(main_benchmark_key)

    # Pre-calculate IPS Data Rows
    ips_rows = get_ips_table_data(pdf_info, summary_df)
    
    # --- HELPER 1: CLEAN TEXT HELPER FUNCTION FOR PDF INFO FROM data/info_for_pdf.xlsx ---
    def clean_text(text):
        """Replaces 'smart' punctuation with standard ASCII."""
        if not isinstance(text, str):
            return str(text)
        
        replacements = {
            '\u2018': "'",  # Left single quote
            '\u2019': "'",  # Right single quote
            '\u201c': '"',  # Left double quote
            '\u201d': '"',  # Right double quote
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
            '\u2026': '...',# Ellipsis
        }
        for original, replacement in replacements.items():
            text = text.replace(original, replacement)
        
        # Final safety net: replace any remaining non-latin-1 chars with '?'
        return text.encode('latin-1', 'replace').decode('latin-1')
    
    # --- HELPER 2: FORMAT DATE (YYYY-MM-DD -> January 13th, 2026) ---
    def format_nice_date(date_str):
        try:
            # Convert string or datetime object to Timestamp
            dt = pd.to_datetime(date_str)
            
            # Determine ordinal suffix (st, nd, rd, th)
            day = dt.day
            if 4 <= day <= 20 or 24 <= day <= 30:
                suffix = "th"
            else:
                suffix = ["st", "nd", "rd"][day % 10 - 1]
            
            # Return format: "Month Day(suffix), Year"
            return dt.strftime(f"%B {day}{suffix}, %Y")
        except:
            return str(date_str) # Fallback if parsing fails
    # --------------------------------------------------------------------------------------
    
    # --- HELPER 3: DISCLOSURE AT BOTTOM OF PAGE ---
    def _disclosure_single_paragraph(text):
        """Turns disclosure copy into one flowing paragraph (no hard line breaks from source)."""
        if not isinstance(text, str):
            text = str(text)
        text = text.replace('\xa0', ' ')
        return re.sub(r'\s+', ' ', text).strip()

    def render_page_disclosure(pdf_obj, text):
        """Renders a small light-grey disclosure paragraph at the bottom of the current page."""
        pdf_obj.set_y(-33)
        pdf_obj.set_font('Carlito', '', 8)
        pdf_obj.set_text_color(C_TEXT_LIGHT_GREY)
        pdf_obj.multi_cell(w=pdf_obj.w - 24, h=3, text=_disclosure_single_paragraph(text), align='L')
    # --------------------------------------------------------------------------------------
    
    # 1. *** SETUP ***
    pdf = PortfolioPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.text_logo_path = text_logo_path
    pdf.logo_path = logo_path
    data_rep_date = format_nice_date(report_date) # Format WHEN GENERATED Report Date
    
    # COMMON STYLES
    header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
    
    
    # Clean the account title for PDF display (strip custodian boilerplate)
    account_title = clean_display_name(account_title)
    
    # Use statement-derived account title (Introduction -> Name) for front/back cover pages.
    # Keep pdf_info fallback only if statement data is missing or generic.
    cover_account_name_raw = account_title
    if not cover_account_name_raw or str(cover_account_name_raw).strip().lower() == "total portfolio":
        cover_account_name_raw = pdf_info.get('page_1_account_name', account_title)
    cover_account_name = clean_text(cover_account_name_raw)

    # --- PRE-COMPUTE COVER DATA (used by both cover and end-cover) ---
    account_name = cover_account_name
    report_title = f"{quarter_label} Portfolio Report"
    title_date_input = pdf_info.get('page_1_report_date', report_date)
    title_rep_date = format_nice_date(title_date_input)

    def _render_cover_page():
        """Shared layout for front and back cover pages."""
        if text_logo_path and os.path.exists(text_logo_path):
            logo_w = 46
            logo_x = (pdf.w - logo_w) / 2
            try: 
                pdf.image(text_logo_path, x=logo_x, y=55, w=logo_w)
            except Exception as e: 
                print(f"Warning: Could not load logo: {e}")
        page_center_y = pdf.h / 2
        content_height = 40
        start_y = page_center_y - (content_height / 2)
        line_width = 200
        line_start_x = (pdf.w - line_width) / 2
        line_end_x = line_start_x + line_width
        pdf.set_draw_color(*C_BLUE_LOGO)
        pdf.set_line_width(0.5)
        pdf.line(line_start_x, start_y, line_end_x, start_y)
        pdf.set_y(start_y + 6)
        pdf.set_font('Carlito', 'B', 24); pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 12, account_name, align='C', new_x="LMARGIN", new_y="NEXT")
        pdf.set_font('Carlito', 'B', 18); pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, report_title, align='C', new_x="LMARGIN", new_y="NEXT")
        pdf.set_font('Carlito', '', 12); pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 8, f"{title_rep_date}", align='C', new_x="LMARGIN", new_y="NEXT")
        final_y = pdf.get_y() + 6
        pdf.line(line_start_x, final_y, line_end_x, final_y)


    #  ==========================================
    #   COVER PAGE
    #  ==========================================
    if vis('cover'):
        pdf.show_standard_header = False
        pdf.add_page()
        _render_cover_page()
    
    
    #  ==========================================
    #   PAGE 2: TABLE OF CONTENTS
    #  ==========================================
    if vis('table_of_contents'):
        pdf.show_standard_header = True; pdf.header_text = "Table of Contents"; pdf.add_page()

    toc_items = []
    if vis('goals_and_objectives'):     toc_items.append("Goals and Objectives")
    if vis('target_allocations'):       toc_items.append("Target Allocations")
    if consolidated_breakdown_rows and vis('breakdown_of_accounts'):
        toc_items.append("Breakdown of Accounts")
    if vis('change_in_portfolio_value'):toc_items.append("Change in Portfolio Value")
    if vis('portfolio_overview'):       toc_items.append("Portfolio Overview")
    if vis('portfolio_performance'):    toc_items.append("Portfolio Performance by Allocation")
    if vis('expanded_performance'):     toc_items.append("Expanded Investment Performance")
    if vis('risk_analysis'):            toc_items.append("Risk Analysis")
    if vis('financial_statistics'):     toc_items.append("Financial Statistics")
    if vis('market_review'):            toc_items.append("Market Review")
    if vis('disclosures'):              toc_items.append("Important Information and Disclosures")
    
    if vis('table_of_contents') and toc_items:
        toc_width = 140
        start_x = (pdf.w - toc_width) / 2
        total_height = len(toc_items) * 12 
        start_y = (pdf.h - total_height) / 2
        if start_y < 35: start_y = 35
        pdf.set_y(start_y)
        
        for i, item in enumerate(toc_items, 1):
            num_str = f"{i:02d}."
            pdf.set_x(start_x)
            pdf.set_font('Carlito', 'B', 14); pdf.set_text_color(*C_BLUE_LOGO)
            pdf.cell(15, 10, num_str, align='R')
            pdf.set_font('Carlito', '', 14); pdf.set_text_color(0, 0, 0)
            pdf.cell(toc_width - 15, 10, item, new_x="LMARGIN", new_y="NEXT", align='L')
            curr_y = pdf.get_y()
            pdf.set_draw_color(240, 240, 240); pdf.set_line_width(0.2)
            pdf.line(start_x, curr_y, start_x + toc_width, curr_y)
            pdf.set_y(curr_y + 2)
        
        
    #  ==========================================
    #   PAGE 3: GOALS & OBJECTIVES
    #  ==========================================
    if vis('goals_and_objectives'):
        pdf.show_standard_header = True; pdf.header_text = "Goals and Objectives"; pdf.add_page()
        pdf.set_y(15); pdf.ln(10)
        ips_text = clean_text(pdf_info.get('page_3_ips_objectives_text', 'No IPS text provided.'))
        pdf.set_font('Carlito', '', 12)
        pdf.set_text_color(0, 0, 0)
        side_margin = 55
        text_block_width = pdf.w - (side_margin *2)
        line_height = 6
        lines = pdf.multi_cell(text_block_width, line_height, ips_text, dry_run=True, output="LINES")
        num_lines = len(lines)
        block_height = num_lines * line_height
        start_y = (pdf.h - block_height) / 2
        if start_y < 35: 
            start_y = 35
        pdf.set_y(start_y)
        pdf.set_x(side_margin)
        pdf.multi_cell(text_block_width, line_height, ips_text)
    
    
    #  ==========================================
    #   PAGE 4: TARGET ALLOCATIONS
    #  ==========================================
    reg_name_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    reg_data_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)

    if vis('target_allocations'):
        pdf.show_standard_header = True; pdf.header_text = "Target Allocations"; pdf.add_page()
        pdf.ln(2) 
        
        with pdf.table(col_widths=(55, 20, 20, 20, 20, 55), 
                       text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"), 
                       borders_layout="NONE",
                       align="CENTER", 
                       width=190, 
                       line_height=8) as table:
            h = table.row()
            h.cell("Category", style=header_style)
            for t in ["Min", "Max", "Target", "Current", "Compliance Status"]: 
                h.cell(t, style=header_style)
            pdf.set_draw_color(*C_GREY_BORDER)
            for cat, v_min, v_max, v_tgt, v_cur in ips_rows:
                r = table.row()
                r.cell(cat, style=reg_name_style)
                r.cell(f"{v_min:.1%}", style=reg_data_style, border="RIGHT")
                r.cell(f"{v_max:.1%}", style=reg_data_style, border="RIGHT")
                r.cell(f"{v_tgt:.1%}", style=reg_data_style, border="RIGHT")
                r.cell(f"{v_cur:.1%}", style=reg_data_style, border="RIGHT")
                status = "Compliant" if _ips_band_compliant(v_cur, v_min, v_max) else "Non-Compliant"
                r.cell(status, style=reg_data_style)
                
        pdf.ln(26)
        
        try:
            ips_chart_img = generate_ips_chart(ips_rows)
            if ips_chart_img:
                chart_w = 265
                legend_start_x = pdf.w - 125
                legend_y = pdf.get_y()
                pdf.set_font('Carlito', '', 12)
                pdf.set_text_color(0, 0, 0) 
                key1_x = legend_start_x
                pdf.set_draw_color(211, 211, 211) 
                pdf.set_line_width(1)             
                pdf.line(key1_x, legend_y+2, key1_x+10, legend_y+2) 
                pdf.set_xy(key1_x + 12, legend_y)
                pdf.cell(30, 4, "Compliance Range", align='L')
                key2_x = legend_start_x + 53
                pdf.set_draw_color(0, 0, 0)
                pdf.set_line_width(0.5)
                pdf.line(key2_x, legend_y, key2_x, legend_y+4) 
                pdf.set_xy(key2_x + 2, legend_y)
                pdf.cell(20, 4, "Target", align='L')
                key3_x = legend_start_x + 77
                pdf.set_fill_color(*C_BLUE_LOGO)
                pdf.set_draw_color(*C_BLUE_LOGO)
                pdf.circle(key3_x, legend_y+2, 1, style="FD") 
                pdf.set_xy(key3_x + 3, legend_y)
                pdf.cell(30, 4, "Current", align='L')
                pdf.set_line_width(0.2)
                chart_x = ((pdf.w - chart_w) / 2) - 10
                chart_y = legend_y + 8 
                pdf.image(ips_chart_img, x=chart_x, y=chart_y, w=chart_w)
                os.remove(ips_chart_img)   
        except Exception as e:
            print(f"IPS Chart Error: {e}")

    #  ==========================================
    #   CONSOLIDATED ONLY: BREAKDOWN OF ACCOUNTS (after Target Allocations)
    #  ==========================================
    if consolidated_breakdown_rows and vis('breakdown_of_accounts'):
        pdf.show_standard_header = True
        pdf.header_text = "Breakdown of Accounts"
        pdf.add_page()

        line_height = 8
        col_widths = (30, 44, 36, 36, 30)
        table_width = sum(col_widths)
        n = len(consolidated_breakdown_rows) + (1 if key_statistics else 0)
        table_h = line_height * (1 + n)
        table_start_x = (pdf.w - table_width) / 2
        table_start_y = (pdf.h - table_h) / 2
        text_block_height = 12
        text_start_y = table_start_y - text_block_height
        if text_start_y < 35:
            text_start_y = 35
            table_start_y = text_start_y + text_block_height

        pdf.set_y(text_start_y)
        pdf.set_x(table_start_x)
        pdf.set_font("Carlito", "B", 12)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 5, "Breakdown of Accounts", new_x="LMARGIN", new_y="NEXT", align="L")
        pdf.set_x(table_start_x)
        pdf.set_font("Carlito", "", 10)
        pdf.set_text_color(*C_TEXT_GREY)
        pdf.cell(0, 4, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align="L")
        pdf.ln(3)
        pdf.set_y(table_start_y)
        pdf.set_text_color(0, 0, 0)

        with pdf.table(
            col_widths=col_widths,
            text_align=("LEFT", "LEFT", "RIGHT", "RIGHT", "RIGHT"),
            borders_layout="NONE",
            align="CENTER",
            width=table_width,
            line_height=line_height,
        ) as table:
            hdr = table.row()
            hdr.cell("Account", style=header_style)
            hdr.cell("Type", style=header_style)
            hdr.cell("Beginning NAV", style=header_style)
            hdr.cell("Ending NAV", style=header_style)
            hdr.cell("Return", style=header_style)
            for br in consolidated_breakdown_rows:
                row = table.row()
                row.cell(clean_text(br.account_number), style=reg_name_style)
                row.cell(clean_text(br.type_label), style=reg_name_style)
                row.cell(f"${br.beginning_nav:,.0f}", style=reg_data_style)
                row.cell(f"${br.ending_nav:,.0f}", style=reg_data_style)
                row.cell(f"{br.return_pct:.2f}%", style=reg_data_style)
            if key_statistics:
                total_style = FontFace(emphasis="BOLD", size_pt=12, fill_color=C_LIGHT_BG)
                total_row = table.row()
                total_row.cell("Total", style=total_style)
                total_row.cell("", style=total_style)
                total_row.cell(f"${key_statistics.get('BeginningNAV', 0.0):,.0f}", style=total_style)
                total_row.cell(f"${key_statistics.get('EndingNAV', 0.0):,.0f}", style=total_style)
                total_row.cell(f"{key_statistics.get('CumulativeReturn', 0.0) * 100:.2f}%", style=total_style)
        render_page_disclosure(pdf, DISCLOSURE_BREAKDOWN)


    #  ==========================================
    #   PAGE 5: CHANGE IN PORTFOLIO VALUE
    #  ==========================================
    if vis('change_in_portfolio_value'):
        pdf.show_standard_header = True; pdf.header_text = "Change in Portfolio Value"; pdf.add_page()
        breakdown = {}
        if key_statistics:
            breakdown = {
                "Starting Value": key_statistics.get('BeginningNAV', 0.0),
                "Mark-to-Market": key_statistics.get('MTM', 0.0),
                "Deposits & Withdrawals": key_statistics.get('Deposits & Withdrawals', 0.0),
                "Dividends": key_statistics.get('Dividends', 0.0),
                "Interest": key_statistics.get('Interest', 0.0),
                "Fees & Commissions": key_statistics.get('Fees & Commissions', 0.0),
                "Change in Interest Accruals": key_statistics.get('ChangeInInterestAccruals', 0.0),
                "Ending Value": key_statistics.get('EndingNAV', 0.0)
            }
        if breakdown:
            table_width = 105
            table_start_x = (pdf.w - table_width) / 2
            table_height = 9 * 8 
            table_start_y = (pdf.h - table_height) / 2
            text_block_height = 12
            text_start_y = table_start_y - text_block_height
            if text_start_y < 35:
                text_start_y = 35
                table_start_y = text_start_y + text_block_height
            pdf.set_y(text_start_y)
            pdf.set_x(table_start_x)
            pdf.set_font('Carlito', 'B', 12)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(0, 5, "Quarterly Change in Net Asset Value", new_x="LMARGIN", new_y="NEXT", align='L')
            pdf.set_x(table_start_x)
            pdf.set_font('Carlito', '', 10)
            pdf.set_text_color(*C_TEXT_GREY)
            pdf.cell(0, 4, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
            pdf.ln(3)
            pdf.set_y(table_start_y)
            pdf.set_text_color(0, 0, 0)
            with pdf.table(col_widths=(75, 30),
                           text_align=("LEFT", "RIGHT"), 
                           borders_layout="NONE",
                           align="CENTER", 
                           width=table_width, 
                           line_height=8) as table:
                header = table.row()
                header.cell("Category", style=header_style)
                header.cell("Value", style=header_style)
                row_order = ["Starting Value", "Mark-to-Market", "Deposits & Withdrawals", "Dividends", "Interest","Fees & Commissions", "Change in Interest Accruals", "Ending Value"]
                for key in row_order:
                    val = breakdown.get(key, 0.0)
                    is_bold = key in ["Starting Value", "Ending Value"]
                    display_name = key if is_bold else f"      {key}"
                    bg_color = C_LIGHT_BG if is_bold else C_WHITE
                    style_row = FontFace(emphasis="BOLD" if is_bold else "", size_pt=12, fill_color=bg_color)
                    r = table.row()
                    r.cell(display_name, style=style_row)
                    r.cell(f"${val:,.0f}", style=style_row)
            render_page_disclosure(pdf, DISCLOSURE_NAV)


    #  ==========================================
    #   PAGE 6: PORTFOLIO OVERVIEW
    #  ==========================================
    if vis('portfolio_overview'):
        pdf.show_standard_header = True; pdf.header_text = "Portfolio Overview"; pdf.add_page()
        pdf.set_font('Carlito', '', 10); pdf.set_text_color(*C_TEXT_GREY); pdf.cell(0, 1, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT"); 
        pdf.ln(3); start_y = pdf.get_y()

        has_1y = True
        has_3y = True
        show_footnotes = False
        if portfolio_inception_date is not None:
            data_span = (pd.to_datetime(report_date) - pd.to_datetime(portfolio_inception_date)).days
            has_1y = data_span >= 365
            has_3y = data_span >= 365 * 3
            show_footnotes = not has_1y or not has_3y

        if performance_chart_data is not None:
            try:
                line_chart_img = generate_line_chart(performance_chart_data)
                if line_chart_img:
                    chart_title = "Portfolio Performance vs Benchmark"
                    pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 9, chart_title, new_x="LMARGIN", new_y="NEXT")
                    pdf.set_y(start_y+10); pdf.image(line_chart_img, w=165); os.remove(line_chart_img)
            except Exception as e:
                print(f"Performance Chart Error: {e}")
        try:
            chart_img = generate_donut_chart(summary_df); 
            if chart_img: 
                pdf.set_y(start_y); pdf.set_x(185); pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 9, "Current Asset Allocation", new_x="LMARGIN", new_y="NEXT")
                pdf.set_y(start_y+10); pdf.set_x(190); pdf.image(chart_img, w=95); os.remove(chart_img)
        except Exception as e:
            print(f"Asset Allocation Chart Error: {e}")
        
        table_width = 180
        table_start_x = (pdf.w - table_width) / 2
        pdf.set_y(start_y + 107)
        pdf.set_x(table_start_x)
        pdf.set_font('Carlito', 'B', 12)
        pdf.set_text_color(0,0,0)
        pdf.cell(0, 9, "Historical Performance", new_x="LMARGIN", new_y="NEXT", align='L')
        
        keys = ["Quarter", "YTD", "1Y", "3Y", "Inception"]
        headers_map = {"Quarter": f"{quarter_label}", "YTD": "YTD", "1Y": "1YR", "3Y": "3YR", "Inception": "Inception"}
        col_widths = (60, 24, 24, 24, 24, 24)
        alignments = ("LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT")

        def _perf_cell_text(key, val):
            if val is None:
                if key == "3Y" and not has_3y: return "\u2014\u00B9 \u00B2"
                if key == "1Y" and not has_1y: return "\u2014\u00B9 \u00B2"
                return "\u2014"
            text = f"{val:.2%}"
            if key == "Inception" and show_footnotes: text += "\u00B9"
            return text

        with pdf.table(col_widths=col_widths, text_align=alignments, borders_layout="NONE", align="CENTER", width=table_width) as table:
            row1 = table.row()
            row1.cell("Account", style=header_style, align="LEFT")
            for k in keys:
                row1.cell(headers_map.get(k, k), style=header_style, align="RIGHT")
            row = table.row()
            acct_str = account_title[:38] + "..." if len(account_title) > 40 else account_title
            p5_style = FontFace(size_pt=12, fill_color=C_LIGHT_BG)
            row.cell(acct_str, style=p5_style, align="LEFT")
            for i, k in enumerate(keys): 
                val = performance_windows.get(k) if performance_windows else None
                b_style = "RIGHT" if k != "Inception" else "NONE"
                row.cell(_perf_cell_text(k, val), style=p5_style, align="RIGHT", border=b_style)
            if benchmark_performance_windows:
                row_bench = table.row()
                p5_bench_style = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
                row_bench.cell("Benchmark", style=p5_bench_style)
                for i, k in enumerate(keys): 
                    val = benchmark_performance_windows.get(k)
                    b_style = "RIGHT" if k != "Inception" else "NONE"
                    row_bench.cell(_perf_cell_text(k, val), style=p5_bench_style, align="RIGHT", border=b_style)

        if show_footnotes:
            pdf.set_y(-40)
            pdf.set_font('Carlito', '', 7)
            pdf.set_text_color(C_TEXT_LIGHT_GREY)
            pdf.cell(0, 3, "1. Annualized Return", new_x="LMARGIN", new_y="NEXT")
            pdf.cell(0, 3, "2. Not held for entire period")

        render_page_disclosure(pdf, disclosure_performance)


    #  ==========================================
    #   PAGE 7: PORTFOLIO PERFORMANCE BY ALLOCATION
    #  ==========================================
    # === UPDATE HOLDINGS: MOVE ACCRUALS INTO CASH ===
    # 1. Update Holdings Ticker: Move 'Other' to 'Cash'
    if 'asset_class' in holdings_df.columns:
        holdings_df.loc[holdings_df['asset_class'] == 'Other', 'asset_class'] = 'Cash'

    # 2. Update Summary DF: Sum 'Other' values into 'Cash' row WITHOUT breaking sort order
    if not summary_df.empty:
        # Find the specific index for Cash and Other buckets
        cash_indices = summary_df[(summary_df['Type'] == 'Bucket') & (summary_df['Name'] == 'Cash')].index
        other_indices = summary_df[(summary_df['Type'] == 'Bucket') & (summary_df['Name'] == 'Other')].index

        # If both exist, merge 'Other' into 'Cash'
        if not cash_indices.empty and not other_indices.empty:
            cash_idx = cash_indices[0]
            other_idx = other_indices[0]
            
            # Extract values from 'Other'
            other_mv = summary_df.at[other_idx, 'MarketValue']
            other_alloc = summary_df.at[other_idx, 'Allocation']
            
            # Add to 'Cash'
            summary_df.at[cash_idx, 'MarketValue'] += other_mv
            summary_df.at[cash_idx, 'Allocation'] += other_alloc
            
            # Drop the 'Other' row
            summary_df = summary_df.drop(other_idx).reset_index(drop=True)
            
        # If only 'Other' exists (no Cash bucket yet), just rename it
        elif not other_indices.empty:
             other_idx = other_indices[0]
             summary_df.at[other_idx, 'Name'] = 'Cash'
             summary_df.at[other_idx, 'IsCash'] = True
    
    # === RENDER PAGE 7 ===
    quarter_val = pdf_info.get('quarter', '')
    return_header = f"{quarter_val} Return" if quarter_val else "Return"

    if vis('portfolio_performance'):
        pdf.show_standard_header = True; pdf.header_text = "Portfolio Performance by Allocation"; pdf.add_page()
        table_width = 140
        table_start_x = (pdf.w - table_width) / 2
        num_data_rows = len(summary_df)
        table_height = (1 + num_data_rows) * 8
        table_start_y = (pdf.h - table_height) / 2
        text_block_height = 12
        text_start_y = table_start_y - text_block_height
        if text_start_y < 35:
            text_start_y = 35
            table_start_y = text_start_y + text_block_height
        pdf.set_y(text_start_y)
        pdf.set_x(table_start_x)
        pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 5, "Allocation Summary", new_x="LMARGIN", new_y="NEXT", align='L')
        pdf.set_x(table_start_x)
        pdf.set_font('Carlito', '', 10); pdf.set_text_color(*C_TEXT_GREY)
        pdf.cell(0, 4, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
        pdf.ln(3)
        pdf.set_y(table_start_y)
        bucket_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_LIGHT_BG)
        bench_style = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
        with pdf.table(col_widths=(60, 25, 30, 25), text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                       borders_layout="NONE", align="CENTER", width=table_width, line_height=8) as table:
            header = table.row()
            header.cell("Asset Class", style=header_style, align="LEFT")
            header.cell("Allocation", style=header_style, align="RIGHT")
            header.cell("Ending Value", style=header_style, align="RIGHT")
            header.cell(return_header, style=header_style, align="RIGHT")
            pdf.set_draw_color(*C_GREY_BORDER)
            for _, row in summary_df.iterrows():
                if row['Type'] == 'Bucket':
                    r = table.row()
                    r.cell(row['Name'], style=bucket_style)
                    r.cell(f"{row['Allocation']:.2%}", style=bucket_style, border="RIGHT")
                    r.cell(f"${row['MarketValue']:,.0f}", style=bucket_style, border="RIGHT")
                    r.cell(f"{row['Return']:.2%}", style=bucket_style)
                elif row['Type'] == 'Benchmark':
                    r = table.row()
                    r.cell(row['Name'], style=bench_style)
                    r.cell("", style=bench_style, border="RIGHT") 
                    r.cell("", style=bench_style, border="RIGHT") 
                    r.cell(f"{row['Return']:.2%}", style=bench_style)
        render_page_disclosure(pdf, DISCLOSURE_ALLOCATION)
        
        
    #  ==========================================
    #   PAGE 8+: EXPANDED INVESTMENT PERFORMANCE BY ALLOCATION
    #  ==========================================
    if vis('expanded_performance'):
        pdf.show_standard_header = True; pdf.header_text = "Expanded Investment Performance by Allocation"; pdf.add_page()
        pdf.set_font('Carlito', 'B', 14); pdf.set_text_color(0, 0, 0); pdf.ln(2)

        bucket_order = summary_df[summary_df['Type'] == 'Bucket']['Name'].unique().tolist()
        sorter_map = {name: i for i, name in enumerate(bucket_order)}
        holdings_df['sort_key'] = holdings_df['asset_class'].map(sorter_map).fillna(999)
        sorted_holdings = holdings_df.sort_values(['sort_key', 'weight'], ascending=[True, False])
        unique_buckets = sorted_holdings['asset_class'].unique()

        bucket_bench_map = {}
        current_bucket = None
        for _, row in summary_df.iterrows():
            if row['Type'] == 'Bucket': current_bucket = row['Name']
            elif row['Type'] == 'Benchmark' and current_bucket: bucket_bench_map[current_bucket] = (row['Name'], row['Return'])

        col_widths = (25, 145, 30, 45, 30)
        header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
        bucket_name_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_LIGHT_BG)
        bucket_data_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_LIGHT_BG)
        bench_name_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
        bench_data_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
        reg_name_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
        reg_data_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
        pdf.set_font('Carlito', '', 12)
        page_start = pdf.page
        
        with pdf.table(col_widths=col_widths, 
                       text_align=("LEFT", "LEFT", "RIGHT", "RIGHT", "RIGHT"),
                       borders_layout="NONE", align="LEFT", width=275) as table:
            h_row = table.row()
            headers = ["Ticker", "Name", "Allocation", "Ending Value", return_header] # Add back "Cost Basis" here
            for h in headers: h_row.cell(h, style=header_style)
            for bucket in unique_buckets:
                subset = sorted_holdings[sorted_holdings['asset_class'] == bucket]
                sum_value = subset['raw_value'].sum()
                sum_alloc = subset['weight'].sum()
                bk_row = summary_df[(summary_df['Type']=='Bucket') & (summary_df['Name']==bucket)]
                sum_ret = bk_row['Return'].iloc[0] if not bk_row.empty else 0.0
                bucket_ret_str = f"{sum_ret:.2%}"
                s_row = table.row()
                s_row.cell(bucket, colspan=2, style=bucket_name_style, align="LEFT")
                pdf.set_draw_color(*C_GREY_BORDER)
                s_row.cell(f"{sum_alloc:.2%}", style=bucket_data_style, border="RIGHT")
                s_row.cell(f"${sum_value:,.0f}", style=bucket_data_style, border="RIGHT")
                s_row.cell(bucket_ret_str, style=bucket_data_style)
                bench_info = bucket_bench_map.get(bucket)
                if bench_info:
                    b_name, b_ret = bench_info
                    b_row = table.row()
                    b_row.cell(b_name, colspan=2, style=bench_name_style, align="LEFT")
                    b_row.cell("", style=bench_data_style, border="RIGHT")
                    b_row.cell("", style=bench_data_style, border="RIGHT")
                    b_row.cell(f"{b_ret:.2%}", style=bench_data_style)
                for _, pos in subset.iterrows():
                    r = table.row()
                    raw_ticker = str(pos['ticker'])
                    if raw_ticker == "USD":
                        display_ticker = "USD"
                        name_str = "Settled Cash"
                        c = pos.get("contribution")
                        ret_str = f"{float(c):.2%}" if c is not None and pd.notna(c) else f"{pos['cumulative_return']:.2%}"
                    elif raw_ticker == "ACCRUALS":
                        display_ticker = "Accruals"; name_str = "Interest Accruals"; ret_str = "\u2014"
                    else:
                        display_ticker = raw_ticker
                        ret_str = f"{pos['cumulative_return']:.2%}"
                        name_str = str(pos.get('official_name', ''))
                    if len(name_str) > 60: name_str = name_str[:58] + "..."
                    r.cell(display_ticker, style=reg_name_style)
                    r.cell(name_str, style=reg_name_style)
                    r.cell(f"{pos['weight']:.2%}", style=reg_data_style, border="RIGHT")
                    r.cell(f"${pos['raw_value']:,.0f}", style=reg_data_style, border="RIGHT")
                    r.cell(ret_str, style=reg_data_style)

        page_end = pdf.page
        for pg in range(page_start, page_end + 1):
            pdf.page = pg
            render_page_disclosure(pdf, DISCLOSURE_ALLOCATION)
        pdf.page = page_end


    #  ==========================================
    #   PAGE 9: RISK ANALYTICS
    #  ==========================================
    if vis('risk_analysis'):
        pdf.show_standard_header = True; pdf.header_text = "Risk Analysis"; pdf.add_page()
        left_rows = []
        left_rows.append(("Performance & Drawdown Profile", None, None, True))
        left_rows.append(("Ending VAMI", risk_metrics.get('Ending VAMI', 0.0), "float", False))
        left_rows.append(("Mean Return", risk_metrics.get('Mean Return', 0.0), "percent", False))
        left_rows.append(("Max Drawdown", risk_metrics.get('Max Drawdown', 0.0), "percent", False))
        ptv_val = risk_metrics.get('Peak-To-Valley')
        ptv_display = f"{ptv_val} Days" if ptv_val is not None else "N/A"
        left_rows.append(("Peak-To-Valley", ptv_display, "string", False))
        left_rows.append(("Recovery", risk_metrics.get('Recovery', 'N/A'), "string", False))
        left_rows.append(("Volatility & Risk-Adjusted Returns", None, None, True))
        left_rows.append(("Standard Deviation", risk_metrics.get('Standard Deviation', 0.0), "percent", False))
        left_rows.append(("Downside Deviation", risk_metrics.get('Downside Deviation', 0.0), "percent", False))
        left_rows.append(("Sharpe Ratio", risk_metrics.get('Sharpe Ratio', 0.0), "float", False))
        left_rows.append(("Sortino Ratio", risk_metrics.get('Sortino Ratio', 0.0), "float", False))
        right_rows = []
        right_rows.append(("Relative Risk vs Benchmark", None, None, True))
        right_rows.append(("Idiosyncratic Risk", risk_metrics.get('Idiosyncratic Risk', 0.0), "percent", False))
        right_rows.append(("R-Squared", risk_metrics.get('R-Squared (vs Bench)', 0.0), "float", False))
        right_rows.append(("Factor Coefficients (Betas)", None, None, True))
        factor_keys = [
            ('Size (IWM)', 'Beta: Size (IWM)'), ('Value (IWD)', 'Beta: Value (IWD)'), 
            ('Quality (QUAL)', 'Beta: Quality (QUAL)'), ('Momentum (MTUM)', 'Beta: Momentum (MTUM)')
        ]
        for label, key in factor_keys:
            right_rows.append((label, risk_metrics.get(key, 0.0), "float", False))
        table_width = 133
        table_gap = 10
        total_block_width = (table_width * 2) + table_gap
        table1_start_x = (pdf.w - total_block_width) / 2
        table2_start_x = table1_start_x + table_width + table_gap
        max_data_rows = max(len(left_rows), len(right_rows))
        table_height = (1 + max_data_rows) * 8
        table_start_y = (pdf.h - table_height) / 2
        text_block_height = 12
        text_start_y = table_start_y - text_block_height
        if text_start_y < 35:
            text_start_y = 35
            table_start_y = text_start_y + text_block_height
        pdf.set_y(text_start_y)
        pdf.set_x(table1_start_x)
        pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 5, f"Risk Profile", new_x="LMARGIN", new_y="NEXT", align='L')
        pdf.set_x(table1_start_x)
        pdf.set_font('Carlito', '', 10); pdf.set_text_color(*C_TEXT_GREY)
        pdf.cell(0, 3, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
        pdf.ln(3)
        locked_start_y = pdf.get_y()
        original_l_margin = pdf.l_margin

        def render_risk_table(rows, start_x):
            pdf.set_left_margin(start_x)
            pdf.set_y(locked_start_y)
            pdf.set_text_color(0, 0, 0)
            with pdf.table(col_widths=(65, 65), text_align=("LEFT", "RIGHT"), borders_layout="NONE", 
                           align="LEFT", width=table_width, line_height=8) as table:
                header = table.row()
                header.cell("Risk Metric", style=header_style)
                header.cell("Value", style=header_style)
                for label, val, fmt, is_header in rows:
                    if is_header:
                        display_name = label; val_str = ""; bg_color = C_LIGHT_BG; font_style = "BOLD"
                    else:
                        display_name = f"      {label}"; bg_color = C_WHITE; font_style = ""
                        if fmt == 'percent': val_str = f"{val:.2f}%"
                        elif fmt == 'float': val_str = f"{val:.2f}"
                        else: val_str = str(val)
                    style_row = FontFace(emphasis=font_style, size_pt=12, fill_color=bg_color)
                    r = table.row()
                    r.cell(display_name, style=style_row)
                    r.cell(val_str, style=style_row)

        render_risk_table(left_rows, table1_start_x)
        render_risk_table(right_rows, table2_start_x)
        pdf.set_left_margin(original_l_margin)
        render_page_disclosure(pdf, disclosure_risk_metrics)
        
        
    #  ==========================================
    #   PAGE 10: FINANCIAL STATISTICS
    #  ==========================================
    if vis('financial_statistics'):
        pdf.show_standard_header = True; pdf.header_text = "Financial Statistics"; pdf.add_page()
        pdf.set_y(15); pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 11: MACRO VIEWS, EMPIRICAL
    #  ==========================================
    if vis('macro_views'):
        pdf.show_standard_header = True; pdf.header_text = "Macro Views, Empirical"; pdf.add_page()
        pdf.set_y(15); pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 12: MARKET REVIEW
    #  ==========================================
    if vis('market_review'):
        pdf.show_standard_header = True; pdf.header_text = f"Market Review: {quarter_label}"; pdf.add_page()
        raw_text = clean_text(pdf_info.get('page_11_macro_market_recap', 'No macro views provided.'))
        paragraphs = [p.replace('\xa0', ' ').strip() for p in raw_text.split('\n') if p.strip()]

        side_margin = 20
        col_gap = 10
        col_width = (pdf.w - (side_margin * 2) - col_gap) / 2
        col2_x = side_margin + col_width + col_gap
        line_h = 6
        para_gap = 2
        footer_clearance = 18  # mm above page bottom to stop a column

        pdf.set_font('Carlito', '', 12)
        pdf.set_text_color(40, 40, 40)

        def _para_height(text):
            """Estimate rendered height of a paragraph using fpdf's own line-wrap logic."""
            lines = pdf.multi_cell(col_width, line_h, text, dry_run=True, output="LINES")
            return len(lines) * line_h + para_gap

        def _col_max_y():
            return pdf.h - footer_clearance

        def _new_market_page():
            pdf.show_standard_header = True
            pdf.header_text = f"Market Review: {quarter_label}"
            pdf.add_page()
            pdf.set_font('Carlito', '', 12)
            pdf.set_text_color(40, 40, 40)
            return pdf.get_y() + 1

        # Assign paragraphs to (page, col) slots based on estimated heights
        # so we know before rendering whether a new page is needed.
        slots = []          # list of (col_x, start_y, [paragraphs])
        current_paras = []
        current_height = 0
        col_idx = 0         # 0=left, 1=right

        page_start_y = pdf.get_y() + 1
        col_max_y = _col_max_y()
        # Approximate y where content starts on a fresh page (after header + ln)
        new_page_content_y = 30.0

        for p in paragraphs:
            h = _para_height(p)
            effective_start_y = page_start_y if page_start_y is not None else new_page_content_y
            if current_height + h > (col_max_y - effective_start_y) and current_paras:
                # Flush current slot
                x = side_margin if col_idx == 0 else col2_x
                slots.append((x, page_start_y, current_paras))
                current_paras = []
                current_height = 0
                if col_idx == 0:
                    col_idx = 1          # move to right column, same page
                    # Right column shares the same page_start_y — no change needed
                else:
                    col_idx = 0          # move to left column, new page
                    page_start_y = None  # sentinel: add a new page when rendering
            current_paras.append(p)
            current_height += h

        if current_paras:
            x = side_margin if col_idx == 0 else col2_x
            slots.append((x, page_start_y, current_paras))

        # Render slots.
        # `sy is None` means "this slot lives on a page that hasn't been added yet".
        # Both the left AND right column of a new page carry sy=None, so we must
        # add the page exactly once (when the left column is encountered) and reuse
        # that same start_y for the matching right column.
        current_page_start_y = None  # actual y after the last _new_market_page() call

        for x, sy, paras in slots:
            if sy is None:
                if current_page_start_y is None:
                    # Left column of a new page — add the page now
                    current_page_start_y = _new_market_page()
                # Both left and right column slots for this new page use the same y
                sy = current_page_start_y
            else:
                # First page: sy is already the real start y; no new page needed
                current_page_start_y = None

            pdf.set_y(sy)
            for p in paras:
                pdf.set_x(x)
                pdf.multi_cell(col_width, line_h, p)
                pdf.ln(para_gap)

            # After the right column is finished, reset so the next left column
            # will correctly trigger a new page on its next iteration
            if x == col2_x:
                current_page_start_y = None
    
    
    #  ==========================================
    #   IMPORTANT INFORMATION AND DISCLOSURES
    #  ==========================================
    if vis('disclosures'):
        discl_header_style = FontFace(size_pt=8, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
        discl_row_style = FontFace(size_pt=8, emphasis="", color=(0, 0, 0), fill_color=C_WHITE)

        def _discl_section_heading(title):
            pdf.set_font('Carlito', 'B', 11)
            pdf.set_text_color(*C_BLUE_LOGO)
            # Use y0 + cell height for the rule — not get_y(), which fpdf2 forbids inside unbreakable().
            y0 = pdf.y
            pdf.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT", align='L')
            pdf.set_draw_color(*C_BLUE_LOGO)
            pdf.set_line_width(0.3)
            line_y = y0 + 6
            pdf.line(pdf.l_margin, line_y, pdf.w - 12, line_y)
            pdf.ln(3)

        def _disclosures_new_page():
            pdf.show_standard_header = True
            pdf.header_text = "Important Information and Disclosures"
            pdf.add_page()
            pdf.ln(1)

        extra_disclaimer = pdf_info.get('page_2_disclaimer', '')
        if extra_disclaimer:
            _disclosures_new_page()
            _discl_section_heading("General Disclosures")
            pdf.set_font('Carlito', '', 9); pdf.set_text_color(40, 40, 40)
            pdf.multi_cell(pdf.w - 24, 4, _disclosure_single_paragraph(clean_text(extra_disclaimer)))
            pdf.ln(5)
        else:
            # Start disclosures on its own page only when there is no General Disclosures block above.
            _disclosures_new_page()
        # _discl_section_heading("Capital Market Assumptions")
        # pdf.set_font('Carlito', 'I', 9); pdf.set_text_color(120, 120, 120)
        # pdf.cell(0, 4, "(Millcreek placeholder)", new_x="LMARGIN", new_y="NEXT", align='L'); pdf.ln(5)
        # _discl_section_heading("Forward Looking Statements")
        # pdf.set_font('Carlito', 'I', 9); pdf.set_text_color(120, 120, 120)
        # pdf.cell(0, 4, "(Millcreek placeholder)", new_x="LMARGIN", new_y="NEXT", align='L'); pdf.ln(5)
        _discl_section_heading("Benchmark Definitions")
        pdf.set_font('Carlito', '', 8)
        with pdf.table(col_widths=(60, 210), text_align=("LEFT", "LEFT"), borders_layout="HORIZONTAL_LINES",
                       align="LEFT", width=270, line_height=4) as table:
            h = table.row()
            h.cell("Benchmark", style=discl_header_style); h.cell("Description", style=discl_header_style)
            for bench_name, bench_desc in BENCHMARK_DEFINITIONS:
                r = table.row()
                r.cell(bench_name, style=discl_row_style); r.cell(bench_desc, style=discl_row_style)
        pdf.ln(5)
        def _definitions_and_calculations_table():
            _discl_section_heading("Definitions & Calculations")
            metric_definitions = [
                ("Time-Weighted Return (TWR)", "Return from investments only, not from deposits or withdrawals.", "Sub-period returns linked end-to-end, with flows removed."),
                ("Ending VAMI", "Growth of a hypothetical $1,000 investment since inception.", "Ending value / starting value, scaled to $1,000"),
                ("Mean Return", "Average daily return over the analysis period, annualized.", "Average of daily returns, annualized"),
                ("Max Drawdown", "Largest peak-to-trough decline in portfolio value.", "Lowest point vs. prior peak"),
                ("Peak-To-Valley", "Calendar days from peak to trough of the maximum drawdown.", "Days elapsed from the maximum drawdown's peak to its bottom."),
                ("Recovery", "Whether the portfolio has recovered from its maximum drawdown.", "Indicates if the portfolio has regained its prior peak post-drawdown (Yes/No/NA)"),
                ("Std. Deviation", "Annualized total return volatility (both up and down).", "Daily return standard deviation, annualized"),
                ("Downside Dev.", "Annualized volatility of negative returns only.", "Negative return standard deviation, annualized"),
                ("Sharpe Ratio", "Excess return over risk-free rate per unit of total volatility.", "(Return - Risk-Free) / Volatility, ann."),
                ("Sortino Ratio", "Excess return over risk-free rate per unit of downside volatility.", "(Return - Risk-Free) / Downside Vol., ann."),
                ("Idiosyncratic Risk", "Portfolio volatility not explained by the benchmark.", "Regression residual standard deviation, annualized"),
                ("R-Squared", "Portfolio return variance explained by the benchmark (0 to 1).", "Squared correlation from regression"),
                ("Beta: Size (IWM)", "Sensitivity to the small-cap factor (Russell 2000).", "Regression slope vs. factor"),
                ("Beta: Value (IWD)", "Sensitivity to the value factor (Russell 1000 Value).", "Regression slope vs. factor"),
                ("Beta: Quality (QUAL)", "Sensitivity to the quality factor (MSCI USA Quality).", "Regression slope vs. factor"),
                ("Beta: Momentum (MTUM)", "Sensitivity to the momentum factor (MSCI USA Momentum).", "Regression slope vs. factor"),
            ]
            risk_calc_style = FontFace(size_pt=8, emphasis="ITALICS", color=(80, 80, 80), fill_color=C_WHITE)
            pdf.set_font('Carlito', '', 8)
            with pdf.table(col_widths=(50, 120, 100), text_align=("LEFT", "LEFT", "LEFT"), borders_layout="HORIZONTAL_LINES",
                           align="LEFT", width=270, line_height=4) as table:
                h = table.row()
                h.cell("Metric", style=discl_header_style); h.cell("Definition", style=discl_header_style); h.cell("Calculation", style=discl_header_style)
                for metric, definition, calc in metric_definitions:
                    r = table.row()
                    r.cell(metric, style=discl_row_style); r.cell(definition, style=discl_row_style); r.cell(calc, style=risk_calc_style)

        def _disclosures_copyright_footer():
            pdf.ln(2)
            pdf.set_font('Carlito', '', 8); pdf.set_text_color(120, 120, 120)
            pdf.cell(0, 4, "\u00A9 2026 All rights reserved. Gaard Capital LLC. May not be used or reproduced without express permission.",
                     new_x="LMARGIN", new_y="NEXT", align='C')

        if legal_notes is not None and not legal_notes.empty:
            _definitions_and_calculations_table()
            pdf.ln(5)
            with pdf.unbreakable():
                _discl_section_heading("Account & Trading Notes")
                notes_df = legal_notes.copy()
                pdf.set_font('Carlito', '', 8)
                with pdf.table(col_widths=(50, 220), text_align=("LEFT", "LEFT"), borders_layout="HORIZONTAL_LINES",
                               align="LEFT", width=270, line_height=4) as table:
                    h = table.row()
                    h.cell("Type", style=discl_header_style); h.cell("Note", style=discl_header_style)
                    note_style = FontFace(size_pt=8, emphasis="", color=(0, 0, 0), fill_color=C_WHITE)
                    for _, row in notes_df.iterrows():
                        r = table.row()
                        r.cell(str(row.get('Type', '')), style=note_style); r.cell(str(row.get('Note', '')), style=note_style)
                _disclosures_copyright_footer()
        else:
            with pdf.unbreakable():
                _definitions_and_calculations_table()
                _disclosures_copyright_footer()


    #  ==========================================
    #   END COVER PAGE
    #  ==========================================            
    if vis('end_cover'):
        pdf.show_standard_header = False
        pdf.add_page()
        _render_cover_page()
        pdf.suppress_footer = True
    
    # == PDF OUTPUT ===
    if output_path.endswith('.xlsx'):
        output_path = output_path.replace('.xlsx', '.pdf')
    
    pdf.output(output_path)
    if os.path.exists("temp_chart.png"): os.remove("temp_chart.png")
    print(f"   > SUCCESS: PDF saved to {output_path}")