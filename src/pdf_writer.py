import pandas as pd
import altair as alt
from fpdf import FPDF
from fpdf.fonts import FontFace
import os
import datetime

# --- CONSTANTS & COLORS ---
C_BLUE_PRIMARY = (89, 120, 247)   # #5978F7
C_BLUE_LOGO    = (73, 106, 154)    # #496A94
C_GREY_LIGHT   = (245, 247, 255)  
C_GREY_BORDER  = (200, 200, 200)
C_TEXT_GREY    = (100, 100, 100)
C_WHITE        = (255, 255, 255)  

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
            self.add_font('Carlito', '', 'data/fonts/Carlito-Regular.ttf', uni=True)  # Carlito is Metric-compatible with Calibri
            self.add_font('Carlito', 'B', 'data/fonts/Carlito-Bold.ttf',uni=True)
            self.add_font('Carlito', 'I', 'data/fonts/Carlito-Italic.ttf', uni=True)
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

    # 2. International Equities (Dev + EM)
    dev_min = get_val('page_4_ips_non_us_equity_dev_range_min')
    dev_max = get_val('page_4_ips_non_us_equity_dev_range_max')
    dev_tgt = get_val('page_4_ips_non_us_equity_dev_target')
    
    em_min  = get_val('page_4_ips_non_us_equity_em_range_min')
    em_max  = get_val('page_4_ips_non_us_equity_em_range_max')
    em_tgt  = get_val('page_4_ips_non_us_equity_em_target')
    
    intl_min = dev_min + em_min
    intl_max = dev_max + em_max
    intl_tgt = dev_tgt + em_tgt
    intl_cur = get_current('International Equities')
    rows.append(('International Equities', intl_min, intl_max, intl_tgt, intl_cur))

    # # 3. Total Equities
    # total_eq_min = us_min + intl_min
    # total_eq_max = us_max + intl_max
    # total_eq_tgt = us_tgt + intl_tgt
    # total_eq_cur = us_cur + intl_cur
    # rows.append(('Total Equities', total_eq_min, total_eq_max, total_eq_tgt, total_eq_cur))

    # 4. Fixed Income (US + Global)
    us_fi_min = get_val('page_4_ips_us_fixed_income_range_min')
    us_fi_max = get_val('page_4_ips_us_fixed_income_range_max')
    us_fi_tgt = get_val('page_4_ips_us_fixed_income_target')
    
    gl_fi_min = get_val('page_4_ips_non_us_fixed_income_global_range_min')
    gl_fi_max = get_val('page_4_ips_non_us_fixed_income_global_range_max')
    gl_fi_tgt = get_val('page_4_ips_non_us_fixed_income_global_target')
    
    fi_min = us_fi_min + gl_fi_min
    fi_max = us_fi_max + gl_fi_max
    fi_tgt = us_fi_tgt + gl_fi_tgt
    fi_cur = get_current('Fixed Income')
    rows.append(('Fixed Income', fi_min, fi_max, fi_tgt, fi_cur))

    # 5. Alternatives
    alt_min = get_val('page_4_ips_alternatives_range_min')
    alt_max = get_val('page_4_ips_alternatives_range_max')
    alt_tgt = get_val('page_4_ips_alternatives_target')
    alt_cur = get_current('Alternative Assets')
    rows.append(('Alternatives', alt_min, alt_max, alt_tgt, alt_cur))

    # 6. Cash
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
        is_compliant = (v_min <= v_cur <= v_max)
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
    chart.save(chart_path, scale_factor=3.0)
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
    
    # Ensure 'Portfolio' is first in the list so it gets the Blue color (#5978F7)
    if 'Portfolio' in series_names:
        series_names.remove('Portfolio')
        # The remaining name Composite Benchmark
        bench_name = series_names[0] if series_names else "Benchmark"
        domain = ['Portfolio', bench_name]
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
    
    chart.save("temp_line_chart.png", scale_factor=3.0)
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
    chart.save(chart_path, scale_factor=3.0)
    return chart_path


#  ==========================================
#   OVERALL PORTFOLIO REPORT
#  ==========================================
def write_portfolio_report(summary_df, holdings_df, nav_performance, total_metrics, risk_metrics, report_date, output_path, account_title="Total Portfolio",
                           performance_windows=None, benchmark_performance_windows=None, performance_chart_data=None, period_label="Quarter", main_benchmark_tckr="SPY", risk_time_horizon=1,
                           legal_notes=None, pdf_info=None, text_logo_path=None, logo_path=None):
    
    print(f"   > Generating PDF Report: {output_path}")
    if pdf_info is None: pdf_info = {}
    
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
    
    # 1. *** SETUP ***
    pdf = PortfolioPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.text_logo_path = text_logo_path
    pdf.logo_path = logo_path
    data_rep_date = format_nice_date(report_date) # Format WHEN GENERATED Report Date
    
    # COMMON STYLES
    header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
    
    
    #  ==========================================
    #   COVER PAGE
    #  ==========================================
    pdf.show_standard_header = False
    pdf.add_page()
    
    # --- DATA EXTRACTION ---
    account_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    report_title = f"{period_label} Portfolio Report"
    title_date_input = pdf_info.get('page_1_report_date', report_date)
    title_rep_date = format_nice_date(title_date_input)
    
    # --- 1. LOGO (TOP CENTER) ---
    if logo_path and os.path.exists(logo_path):
        logo_w = 15  # Much smaller size
        logo_x = (pdf.w - logo_w) / 2
        logo_y = 55  # Positioned near the top
        try: 
            pdf.image(logo_path, x=logo_x, y=logo_y, w=logo_w)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")

    # --- 2. MAIN SECTION (VERTICALLY CENTERED) ---
    # Calculate vertical center of page (A4 Landscape Height ~210mm)
    page_center_y = pdf.h / 2
    
    # Estimated Heights: Gap(5) + Name(12) + Title(10) + Date(8) + Gap(5) = ~40mm total content
    content_height = 40 
    start_y = page_center_y - (content_height / 2)
    
    # Line Settings
    line_width = 200
    line_start_x = (pdf.w - line_width) / 2
    line_end_x = line_start_x + line_width
    
    # A. TOP BLUE LINE
    pdf.set_draw_color(*C_BLUE_LOGO)
    pdf.set_line_width(0.5)
    pdf.line(line_start_x, start_y, line_end_x, start_y)
    
    # B. TEXT BLOCK
    pdf.set_y(start_y + 6) # Small gap after line
    
    # Account Name
    pdf.set_font('Carlito', 'B', 24)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 12, account_name, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # Report Title
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 10, report_title, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # Report Date
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, f"{title_rep_date}", align='C', new_x="LMARGIN", new_y="NEXT")
    
    # C. BOTTOM BLUE LINE
    final_y = pdf.get_y() + 6 # Small gap after text
    pdf.line(line_start_x, final_y, line_end_x, final_y)
    
    
    #  ==========================================
    #   PAGE 2: ENDOWMENT GOALS & OBJECTIVES
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Endowment Goals and Objectives"; pdf.add_page()
    pdf.set_y(15); pdf.ln(10)
    
    # Body Text
    ips_text = clean_text(pdf_info.get('page_3_ips_objectives_text', 'No IPS text provided.'))
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(0, 0, 0)
    side_margin = 55
    text_block_width = pdf.w - (side_margin *2)
    line_height = 6
    
    # Calculate Block Height (Simulate Writing)
    lines = pdf.multi_cell(text_block_width, line_height, ips_text, dry_run=True, output="LINES")
    num_lines = len(lines)
    block_height = num_lines * line_height
    
    start_y = (pdf.h - block_height) / 2    # vertical center
    
    # Safety Check: Header Overlap
    if start_y < 35: 
        start_y = 35
        
    pdf.set_y(start_y)
    pdf.set_x(side_margin)
    pdf.multi_cell(text_block_width, line_height, ips_text)
    
    #  ==========================================
    #   PAGE 3: ENDOWMENT TARGET ALLOCATIONS
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Endowment Target Allocations"; pdf.add_page()
    pdf.ln(2) 
    
    reg_name_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    reg_data_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    
    # ---IPS COMPLIANCE TABLE ---    
    with pdf.table(col_widths=(55, 20, 20, 20, 20, 55), 
                   text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="NONE", # No horizontal lines
                   align="CENTER", 
                   width=190, 
                   line_height=8) as table:
        
        # Header
        h = table.row()
        h.cell("Category", style=header_style)
        for t in ["Min", "Max", "Target", "Current", "Compliance Status"]: 
            h.cell(t, style=header_style)
            
        # Data
        pdf.set_draw_color(*C_GREY_BORDER) # Light grey for vertical lines
        for cat, v_min, v_max, v_tgt, v_cur in ips_rows:
            r = table.row()
            r.cell(cat, style=reg_name_style)
            
            # Numeric columns with Right Borders
            r.cell(f"{v_min:.1%}", style=reg_data_style, border="RIGHT")
            r.cell(f"{v_max:.1%}", style=reg_data_style, border="RIGHT")
            r.cell(f"{v_tgt:.1%}", style=reg_data_style, border="RIGHT")
            r.cell(f"{v_cur:.1%}", style=reg_data_style, border="RIGHT")
            
            # Status
            status = "Compliant" if v_min <= v_cur <= v_max else "Non-Compliant"
            r.cell(status, style=reg_data_style)
            
    pdf.ln(26)
    
    # --- IPS BOX & WHISKERS CHART *LEGEND CREATION HERE* ---
    try:
        ips_chart_img = generate_ips_chart(ips_rows)
        if ips_chart_img:
            # 1. Setup Dimensions
            chart_w = 265
            legend_start_x = pdf.w - 125
            legend_y = pdf.get_y()
            
            # 2. Draw Custom Legend (TOP - Left Aligned to Graph)
            pdf.set_font('Carlito', '', 12)
            pdf.set_text_color(0, 0, 0) 
            
            # --- ITEM 1: RANGE ---
            key1_x = legend_start_x
            pdf.set_draw_color(211, 211, 211) 
            pdf.set_line_width(1)             
            pdf.line(key1_x, legend_y+2, key1_x+10, legend_y+2) 
            # Label
            pdf.set_xy(key1_x + 12, legend_y)
            pdf.cell(30, 4, "Compliance Range", align='L')
            
            # --- ITEM 2: TARGET ---
            key2_x = legend_start_x + 53
            pdf.set_draw_color(0, 0, 0)
            pdf.set_line_width(0.5)
            pdf.line(key2_x, legend_y, key2_x, legend_y+4) 
            # Label
            pdf.set_xy(key2_x + 2, legend_y)
            pdf.cell(20, 4, "Target", align='L')
            
            # --- ITEM 3: CURRENT ---
            key3_x = legend_start_x + 77
            pdf.set_fill_color(*C_BLUE_LOGO)
            pdf.set_draw_color(*C_BLUE_LOGO)
            pdf.circle(key3_x, legend_y+2, 1, style="FD") 
            # Label
            pdf.set_xy(key3_x + 3, legend_y)
            pdf.cell(30, 4, "Current", align='L')
            
            # 3. Place Image (Below Legend)
            pdf.set_line_width(0.2) # Reset line width
            chart_x = ((pdf.w - chart_w) / 2) - 10
            chart_y = legend_y + 8 
            pdf.image(ips_chart_img, x=chart_x, y=chart_y, w=chart_w)
            os.remove(ips_chart_img)   
    except Exception as e:
        print(f"IPS Chart Error: {e}")
        
        
    #  ==========================================
    #   PAGE 4: CHANGE IN PORTFOLIO VALUE
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Change in Portfolio Value"; pdf.add_page()
    breakdown = nav_performance.get('Breakdown', {}) if nav_performance else {}
    
    if breakdown:
        table_width = 105 # ensure any change here is accounted for in column widths below
        table_start_x = (pdf.w - table_width) / 2
        table_height = 9 * 8 
        table_start_y = (pdf.h - table_height) / 2
        text_block_height = 12 # Height Calculation: TableTitle(5) + ReportingDate(4) + Gap(3)
        text_start_y = table_start_y - text_block_height
        
        # Safety Check: Don't let text hit the header (Top Margin ~35mm)
        if text_start_y < 35:
            text_start_y = 35
            table_start_y = text_start_y + text_block_height

        # Table Title (Left Aligned to Table)
        pdf.set_y(text_start_y)
        pdf.set_x(table_start_x)
        pdf.set_font('Carlito', 'B', 12)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 5, "Quarterly Change in Net Asset Value", new_x="LMARGIN", new_y="NEXT", align='L')
        
        # Reporting Date
        pdf.set_x(table_start_x)
        pdf.set_font('Carlito', '', 10)
        pdf.set_text_color(*C_TEXT_GREY)
        pdf.cell(0, 4, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
      
        pdf.ln(3)
        
        # --- TABLE ---
        pdf.set_y(table_start_y)
        pdf.set_text_color(0, 0, 0)
        with pdf.table(col_widths=(75, 30),     # Sum up to table_width above
                       text_align=("LEFT", "RIGHT"), 
                       borders_layout="NONE",
                       align="CENTER", 
                       width=table_width, 
                       line_height=8) as table:
            
            # Header
            header = table.row()
            header.cell("Category", style=header_style)
            header.cell("Value", style=header_style)
            
            # Data
            row_order = ["Starting Value", "Mark-to-Market", "Deposits & Withdrawals", "Dividends", "Interest", "Change in Interest Accruals", "Commissions", "Ending Value"]
            for key in row_order:
                val = breakdown.get(key, 0.0)
                is_bold = key in ["Starting Value", "Ending Value"]
                display_name = key if is_bold else f"      {key}"
                
                # Logic: Grey for bold rows, White for others
                bg_color = C_GREY_LIGHT if is_bold else C_WHITE
                
                style_row = FontFace(emphasis="BOLD" if is_bold else "", size_pt=12, fill_color=bg_color)
                
                r = table.row()
                r.cell(display_name, style=style_row)
                r.cell(f"${val:,.0f}", style=style_row)
       
       
       
       
    #  ==========================================
    #   PAGE 5: PORTFOLIO OVERVIEW
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Portfolio Overview"; pdf.add_page()
    pdf.set_font('Carlito', '', 10); pdf.set_text_color(*C_TEXT_GREY); pdf.cell(0, 1, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT"); 
    pdf.ln(3); start_y = pdf.get_y()       

    # --- 1. PERFORMANCE AND ALLOCATION CHARTS ---
    if performance_chart_data is not None:
        try:
            line_chart_img = generate_line_chart(performance_chart_data)
            if line_chart_img: 
                pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 9, f"Endowment Performance vs Benchmark", new_x="LMARGIN", new_y="NEXT")
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
    
    # --- 2. NAV PERFORMANCE TABLE ---
    # Horizontally Center Page Placement
    table_width = 180
    table_start_x = (pdf.w - table_width) / 2
    
    # 3. Table Title
    pdf.set_y(start_y + 120)
    pdf.set_x(table_start_x)  # Moves title to start of table
    pdf.set_font('Carlito', 'B', 12)
    pdf.set_text_color(0,0,0)
    pdf.cell(0, 9, "Historical Performance", new_x="LMARGIN", new_y="NEXT", align='L')
    
    # Setup Data
    keys = ["Period", "YTD", "1Y", "3Y", "Inception"]
    
    headers_map = {
        "Period": f"{period_label}",
        "YTD": "YTD",
        "1Y": "1YR",
        "3Y": "3YR",
        "Inception": "Inception"
    }

    col_widths = (60, 24, 24, 24, 24, 24)
    alignments = ("LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT")

    # Render Table
    with pdf.table(col_widths=col_widths, 
                   text_align=alignments, 
                   borders_layout="NONE", 
                   align="CENTER",
                   width=table_width) as table:
        
        # --- HEADER ROW ---
        row1 = table.row()
        row1.cell("Account", style=header_style, align="LEFT")
        
        for k in keys:
            label = headers_map.get(k, k)
            row1.cell(label, style=header_style, align="RIGHT")
            
        # --- ROW 1: ACCOUNT DATA ---
        row = table.row()
        acct_str = account_title[:38] + "..." if len(account_title) > 40 else account_title
        p5_style = FontFace(size_pt=12, fill_color=C_GREY_LIGHT)
        
        row.cell(acct_str, style=p5_style, align="LEFT")
        
        for i, k in enumerate(keys): 
            val = performance_windows.get(k) if performance_windows else None
            b_style = "RIGHT" if k != "Inception" else "NONE"
            row.cell(f"{val:.2%}" if val is not None else "-", style=p5_style, align="RIGHT", border=b_style)

        # --- ROW 2: BENCHMARK DATA (NEW) ---
        if benchmark_performance_windows:
            row_bench = table.row()
            p5_bench_style = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
            bench_label = main_benchmark_tckr
            row_bench.cell(f"Benchmark: {bench_label}", style=p5_bench_style)
            
            for i, k in enumerate(keys): 
                val = benchmark_performance_windows.get(k)
                b_style = "RIGHT" if k != "Inception" else "NONE"
                row_bench.cell(f"{val:.2%}" if val is not None else "-", style=p5_bench_style, align="RIGHT", border=b_style)

    #  ==========================================
    #   PAGE 6: PORTFOLIO PERFORMANCE BY ALLOCATION
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
    
    # === RENDER PAGE 6 ===
    pdf.show_standard_header = True; pdf.header_text = "Portfolio Performance by Allocation"; pdf.add_page()
    
    # --- 1. CALCULATE POSITIONS ---
    table_width = 140
    
    # Horizontal Center: Calculate where the table (and text) starts on the X-axis
    table_start_x = (pdf.w - table_width) / 2
    
    # Vertical Center: Calculate height of the TABLE ONLY
    num_data_rows = len(summary_df)
    table_height = (1 + num_data_rows) * 8  # Header + Data Rows
    table_start_y = (pdf.h - table_height) / 2
    text_block_height = 12   # Heights: TableTitle(5) + ReportingTitle(5) + Gap(3)
    text_start_y = table_start_y - text_block_height
    
    # Safety Check: Don't let text hit the header (Top Margin ~35mm)
    if text_start_y < 35:
        text_start_y = 35
        table_start_y = text_start_y + text_block_height
    
    # --- 2. RENDER TEXT (Left Aligned to Table) ---
    pdf.set_y(text_start_y)
    
    # Table Title
    pdf.set_x(table_start_x) # Start at table's left edge
    pdf.set_font('Carlito', 'B', 12)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 5, "Allocation Summary", new_x="LMARGIN", new_y="NEXT", align='L')
    
    # Reporting Date
    pdf.set_x(table_start_x)
    pdf.set_font('Carlito', '', 10)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 4, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(3)
    
    # --- 3. RENDER TABLE (Centered) ---
    # We ensure the cursor is exactly at the calculated table start
    pdf.set_y(table_start_y)
    
    # Styles
    bucket_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    bench_style = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
    
    quarter_val = pdf_info.get('quarter', '')
    return_header = f"{quarter_val} Return" if quarter_val else "Return"
    
    with pdf.table(col_widths=(60, 25, 30, 25),                     # make sure these are sum to table_width
                   text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="NONE", 
                   align="CENTER", 
                   width=table_width, 
                   line_height=8) as table:
        
        # Header
        header = table.row()
        header.cell("Asset Class", style=header_style, align="LEFT")
        header.cell("Allocation", style=header_style, align="RIGHT")
        header.cell("Market Value", style=header_style, align="RIGHT")
        header.cell(return_header, style=header_style, align="RIGHT")
            
        # Data
        pdf.set_draw_color(*C_GREY_BORDER)
        for _, row in summary_df.iterrows():
            if row['Type'] == 'Bucket':
                r = table.row()
                r.cell(row['Name'], style=bucket_style)
                r.cell(f"{row['Allocation']:.2%}", style=bucket_style, border="RIGHT")
                r.cell(f"${row['MarketValue']:,.0f}", style=bucket_style, border="RIGHT")
                r.cell("---" if row.get('IsCash') else f"{row['Return']:.2%}", style=bucket_style)
            elif row['Type'] == 'Benchmark':
                r = table.row()
                r.cell(f"Benchmark: {row['Name']}", style=bench_style)
                r.cell("", style=bench_style, border="RIGHT") 
                r.cell("", style=bench_style, border="RIGHT") 
                r.cell(f"{row['Return']:.2%}", style=bench_style)
        
        
    #  ==========================================
    #   PAGE 7+: EXPANDED INVESTMENT PERFORMANCE BY ALLOCATION
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Expanded Investment Performance by Allocation"; pdf.add_page()
    pdf.set_font('Carlito', 'B', 14); pdf.set_text_color(0, 0, 0); pdf.ln(2)

    # 1. Prepare Sorting
    bucket_order = summary_df[summary_df['Type'] == 'Bucket']['Name'].unique().tolist()
    sorter_map = {name: i for i, name in enumerate(bucket_order)}
    holdings_df['sort_key'] = holdings_df['asset_class'].map(sorter_map).fillna(999)
    sorted_holdings = holdings_df.sort_values(['sort_key', 'weight'], ascending=[True, False])
    unique_buckets = sorted_holdings['asset_class'].unique()

    # 2. Map Benchmarks
    bucket_bench_map = {}
    current_bucket = None
    for _, row in summary_df.iterrows():
        if row['Type'] == 'Bucket': current_bucket = row['Name']
        elif row['Type'] == 'Benchmark' and current_bucket: bucket_bench_map[current_bucket] = (row['Name'], row['Return'])

    # --- CONSTRUCT TABLE ---
    col_widths = (25, 130, 25, 35, 35, 25)
    
    # Styles
    header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
    
    # Asset Class Row: Name(White), Data(Grey)
    bucket_name_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    bucket_data_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    
    # Benchmark Row: Name(White), Return(Grey)
    bench_name_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
    bench_data_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)

    # Holdings Row: Name(White), Data(Grey)
    reg_name_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    reg_data_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)

    pdf.set_font('Carlito', '', 12)
    
    with pdf.table(col_widths=col_widths, 
                   text_align=("LEFT", "LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"),
                   borders_layout="NONE", 
                   align="LEFT",
                   width=275) as table:
        
        # --- HEADER ---
        h_row = table.row()
        headers = ["Ticker", "Name", "Allocation", "Cost Basis", "Value", return_header]
        for h in headers: h_row.cell(h, style=header_style)
            
        for bucket in unique_buckets:
            # 1. CALCULATE BUCKET TOTALS
            subset = sorted_holdings[sorted_holdings['asset_class'] == bucket]
            sum_cost = subset['avg_cost'].sum()
            sum_value = subset['raw_value'].sum()
            sum_alloc = subset['weight'].sum()
            
            # Get return from summary_df to match Page 6
            bk_row = summary_df[(summary_df['Type']=='Bucket') & (summary_df['Name']==bucket)]
            sum_ret = bk_row['Return'].iloc[0] if not bk_row.empty else 0.0

            # ASSET CLASS ROW (SUMMARY)
            s_row = table.row()
            s_row.cell(bucket, colspan=2, style=bucket_name_style, align="LEFT")
            
            # DATA COLUMNS
            pdf.set_draw_color(*C_GREY_BORDER)
            s_row.cell(f"{sum_alloc:.2%}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"${sum_cost:,.0f}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"${sum_value:,.0f}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"{sum_ret:.2%}", style=bucket_data_style) # No border on last col

            # 3. BENCHMARK ROW
            bench_info = bucket_bench_map.get(bucket)
            if bench_info:
                b_name, b_ret = bench_info
                b_row = table.row()
                b_row.cell(f"Benchmark: {b_name}", colspan=2, style=bench_name_style, align="LEFT")
                
                # Empty cells for Alloc, Cost, Value (with borders to maintain structure)
                b_row.cell("", style=bench_data_style, border="RIGHT")
                b_row.cell("", style=bench_data_style, border="RIGHT")
                b_row.cell("", style=bench_data_style, border="RIGHT")
                
                # Return
                b_row.cell(f"{b_ret:.2%}", style=bench_data_style)

            # HOLDINGS ROWS
            for _, pos in subset.iterrows():
                r = table.row()
                # Cash Balance Formatting
                raw_ticker = str(pos['ticker'])
                if raw_ticker == "CASH_BAL":
                    display_ticker = "Cash"
                    ret_str = "---"
                elif raw_ticker == "ACCRUALS":
                    display_ticker = "Accruals"  
                    ret_str = "---"
                else:
                    display_ticker = raw_ticker
                    ret_str = f"{pos['cumulative_return']:.2%}"
                
                # Ticker & Name Cols
                name_str = str(pos.get('official_name', ''))
                if len(name_str) > 60: name_str = name_str[:58] + "..."
                r.cell(display_ticker, style=reg_name_style)
                r.cell(name_str, style=reg_name_style)
                
                # Data Cols
                r.cell(f"{pos['weight']:.2%}", style=reg_data_style, border="RIGHT")
                r.cell(f"${pos['avg_cost']:,.0f}", style=reg_data_style, border="RIGHT")
                r.cell(f"${pos['raw_value']:,.0f}", style=reg_data_style, border="RIGHT")
                r.cell(ret_str, style=reg_data_style)

    
    #  ==========================================
    #   PAGE 8: RISK
    #  ==========================================
    #  ==========================================
    #   PAGE 8: RISK ANALYTICS
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Risk Analysis"; pdf.add_page()
    
    # 1. SETUP DATA & LABELS
    horizon_label = f"{risk_time_horizon} Year" if risk_time_horizon else "Full History"
    
    # Define the rows structure: (Label, Value, Format, IsSectionHeader)
    risk_rows = []
    
    # --- Section 1: Idiosyncratic Risk ---
    risk_rows.append((f"Idiosyncratic Risk vs {main_benchmark_tckr}", None, None, True))
    risk_rows.append(("Idiosyncratic Risk", risk_metrics.get('Idiosyncratic Risk', 0.0), "percent", False))
    risk_rows.append(("R-Squared", risk_metrics.get('R-Squared (vs Bench)', 0.0), "float", False))
    
    # --- Section 2: Factor Coefficients ---
    risk_rows.append(("Factor Coefficients (Betas)", None, None, True))
    factor_keys = [
        ('Size (IWM)', 'Beta: Size (IWM)'), 
        ('Value (IWD)', 'Beta: Value (IWD)'), 
        ('Quality (QUAL)', 'Beta: Quality (QUAL)'), 
        ('Momentum (MTUM)', 'Beta: Momentum (MTUM)')
    ]
    for label, key in factor_keys:
        risk_rows.append((label, risk_metrics.get(key, 0.0), "float", False))

    # 2. CALCULATE LAYOUT (Centering Logic from Page 4)
    table_width = 105 
    table_start_x = (pdf.w - table_width) / 2
    
    # Calculate Table Height: (Header + Data Rows) * Row Height
    # Header(1) + Data(len(risk_rows))
    total_rows = 1 + len(risk_rows)
    table_height = total_rows * 8 
    
    # Vertical Center
    table_start_y = (pdf.h - table_height) / 2
    
    # Title Position
    text_block_height = 12 # TableTitle(5) + ReportingTitle(4) + Gap(3) 
    text_start_y = table_start_y - text_block_height
    
    # Safety Check (Top Margin)
    if text_start_y < 35:
        text_start_y = 35
        table_start_y = text_start_y + text_block_height

    # 3. RENDER TITLE
    pdf.set_y(text_start_y)
    pdf.set_x(table_start_x)
    pdf.set_font('Carlito', 'B', 12)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 5, f"Risk Profile ({horizon_label})", new_x="LMARGIN", new_y="NEXT", align='L')

    # Reporting Date Footer (aligned to table)
    pdf.set_x(table_start_x)
    pdf.set_font('Carlito', '', 10)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 3, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(3)
    
    # 4. RENDER TABLE
    pdf.set_y(table_start_y)
    pdf.set_text_color(0, 0, 0)
    
    with pdf.table(col_widths=(75, 30), 
                   text_align=("LEFT", "RIGHT"), 
                   borders_layout="NONE", 
                   align="CENTER", 
                   width=table_width, 
                   line_height=8) as table:
        
        # --- HEADER ROW ---
        header = table.row()
        header.cell("Risk Metric", style=header_style)
        header.cell("Value", style=header_style)
        
        # --- DATA ROWS ---
        for label, val, fmt, is_header in risk_rows:
            
            # Style Logic
            if is_header:
                # Section Header: Bold, Grey Background, No Indent
                display_name = label
                val_str = ""
                bg_color = C_GREY_LIGHT
                font_style = "BOLD"
            else:
                # Metric Row: Normal, White Background, Indented
                display_name = f"      {label}"
                bg_color = C_WHITE
                font_style = ""
                
                # Formatting
                if fmt == 'percent': val_str = f"{val:.2%}"
                elif fmt == 'float': val_str = f"{val:.2f}"
                else: val_str = str(val)

            # Define FontFace for this row
            style_row = FontFace(emphasis=font_style, size_pt=12, fill_color=bg_color)
            
            r = table.row()
            r.cell(display_name, style=style_row)
            r.cell(val_str, style=style_row)
        
        
    #  ==========================================
    #   PAGE 9: FINANCIAL STATISTICS
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Financial Statistics"; pdf.add_page()
    pdf.set_y(15); pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 10: MACRO VIEWS, EMPIRICAL
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Macro Views, Empirical"; pdf.add_page()
    pdf.set_y(15); pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 11: MARKET REVIEW
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = f"Market Review: {period_label}"; pdf.add_page()
    
    # 1. Clean and Get Text
    raw_text = clean_text(pdf_info.get('page_11_macro_market_recap', 'No macro views provided.'))
    
    # 2. Split into Paragraphs
    paragraphs = [p.replace('\xa0', ' ').strip() for p in raw_text.split('\n') if p.strip()]
    
    # 3. Balance Columns
    col1_paras = []
    col2_paras = []
    
    total_char_count = sum(len(p) for p in paragraphs)
    current_count = 0
    target_count = total_char_count / 2
    
    for p in paragraphs:
        if current_count < target_count:
            col1_paras.append(p)
            current_count += len(p)
        else:
            col2_paras.append(p)
            
    # 4. Define Layout Dimensions (Perfectly Centered)
    # A4 Width = 297mm
    # Side Margins = 20mm each (Total 40mm)
    # Gap = 10mm
    side_margin = 20
    col_gap = 10
    
    # (297 - 40 - 10) / 2 = 123.5mm per column
    col_width = (pdf.w - (side_margin * 2) - col_gap) / 2
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(40, 40, 40)
    
    # Capture Starting Y position
    start_y = pdf.get_y() + 1
    
    # --- RENDER COLUMN 1 (Left) ---
    pdf.set_y(start_y)
    for p in col1_paras:
        # FIX: Start at side_margin (20), not 10
        pdf.set_x(side_margin)  
        pdf.multi_cell(col_width, 6, p)
        pdf.ln(2) 
        
    # --- RENDER COLUMN 2 (Right) ---
    pdf.set_y(start_y)      
    
    # Calculate exact start of Column 2
    # 20 (Margin) + 123.5 (Col 1) + 10 (Gap) = 153.5
    col2_x = side_margin + col_width + col_gap
    
    for p in col2_paras:
        pdf.set_x(col2_x) 
        pdf.multi_cell(col_width, 6, p)
        pdf.ln(2)
        
    
    #  ==========================================
    #   PAGE 12: IMPORTANT INFO AND DISCLOSURES
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Important Information and Disclosures"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    # Body Text
    disclaimer_text = clean_text(pdf_info.get('page_2_disclaimer', 'No disclosures provided.'))
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(40, 40, 40)
    pdf.multi_cell(0, 6, disclaimer_text)     # Multi_cell allows text wrapping
                
                
    #  ==========================================
    #   PAGE 13: STATMENT NOTES & DEFINITIONS
    #  ==========================================
    pdf.show_standard_header = True; pdf.header_text = "Statement Notes"; pdf.add_page()
    pdf.ln(2) 
    
    # -- LEGAL NOTES TABLE --
    if legal_notes is not None and not legal_notes.empty:
        # Reset Font to Regular BEFORE the table just in case
        pdf.set_font('Carlito', '', 8)
        
        notes_df = legal_notes.copy()
            
        with pdf.table(col_widths=(30, 240),
                       text_align=("LEFT", "LEFT"),
                       borders_layout="HORIZONTAL_LINES",
                       align="LEFT",
                       width=270,
                       line_height=4) as table: 
            
            # Header Style
            legalnotes_header_style = FontFace(size_pt=8, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
            h = table.row()
            h.cell("Type", style=legalnotes_header_style)
            h.cell("Note", style=legalnotes_header_style)
            
            # Data Style
            note_style = FontFace(size_pt=8, emphasis="", color=(0,0,0), fill_color=C_WHITE)
            
            for _, row in notes_df.iterrows():
                r = table.row()
                
                type_val = str(row.get('Type', ''))
                note_val = str(row.get('Note', ''))
                
                r.cell(type_val, style=note_style)
                r.cell(note_val, style=note_style)
                
                
    #  ==========================================
    #   END COVER PAGE
    #  ==========================================            
    pdf.show_standard_header = False
    pdf.add_page()
    
    # --- DATA EXTRACTION ---
    account_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    report_title = f"{period_label} Portfolio Report"
    title_date_input = pdf_info.get('page_1_report_date', report_date)
    title_rep_date = format_nice_date(title_date_input)
    
    # --- 1. LOGO (TOP CENTER) ---
    if logo_path and os.path.exists(logo_path):
        logo_w = 15  # Much smaller size
        logo_x = (pdf.w - logo_w) / 2
        logo_y = 55  # Positioned near the top
        try: 
            pdf.image(logo_path, x=logo_x, y=logo_y, w=logo_w)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")

    # --- 2. MAIN SECTION (VERTICALLY CENTERED) ---
    # Calculate vertical center of page (A4 Landscape Height ~210mm)
    page_center_y = pdf.h / 2
    
    # Estimated Heights: Gap(5) + Name(12) + Title(10) + Date(8) + Gap(5) = ~40mm total content
    content_height = 40 
    start_y = page_center_y - (content_height / 2)
    
    # Line Settings
    line_width = 200
    line_start_x = (pdf.w - line_width) / 2
    line_end_x = line_start_x + line_width
    
    # A. TOP BLUE LINE
    pdf.set_draw_color(*C_BLUE_LOGO)
    pdf.set_line_width(0.5)
    pdf.line(line_start_x, start_y, line_end_x, start_y)
    
    # B. TEXT BLOCK
    pdf.set_y(start_y + 6) # Small gap after line
    
    # Account Name
    pdf.set_font('Carlito', 'B', 24)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 12, account_name, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # Report Title
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 10, report_title, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # Report Date
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, f"{title_rep_date}", align='C', new_x="LMARGIN", new_y="NEXT")
    
    # C. BOTTOM BLUE LINE
    final_y = pdf.get_y() + 6 # Small gap after text
    pdf.line(line_start_x, final_y, line_end_x, final_y)
            
    pdf.suppress_footer = True
    
    # == PDF OUTPUT ===
    if output_path.endswith('.xlsx'):
        output_path = output_path.replace('.xlsx', '.pdf')
    
    pdf.output(output_path)
    if os.path.exists("temp_chart.png"): os.remove("temp_chart.png")
    print(f"   > SUCCESS: PDF saved to {output_path}")