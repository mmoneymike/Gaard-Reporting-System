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
            self.add_font('Carlito', '', 'data/fonts/Carlito-Regular.ttf', uni=True)  # Metric-compatible with Calibri
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

        # Determine Y position for content
        content_y = base_y + 2

        # --- LOGO (RIGHT SIDE) ---
        # Position preserved as requested
        if self.text_logo_path and os.path.exists(self.text_logo_path):
            self.image(self.text_logo_path, x=self.w - 39, y=content_y, h=8)

        # --- LEFT SIDE: Confidential Text ---
        self.set_y(content_y)
        self.set_x(10) # Align to left margin
        self.set_font(self.main_font, "", 10)
        self.set_text_color(*C_BLUE_LOGO)
        self.cell(60, 5, "Gaard Capital | Confidential", align='L')

        # --- MIDDLE: Page Number ---
        self.set_y(content_y)
        self.set_x(0) # Reset X to allow centering across entire page width
        self.set_font(self.main_font, 'I', 8)
        self.set_text_color(150, 150, 150)
        display_num = self.page_no()
        self.cell(0, 5, f'Page {display_num}', align='C')
        

#  === IPS TABLE DATA ===
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

    # 3. Total Equities
    total_eq_min = us_min + intl_min
    total_eq_max = us_max + intl_max
    total_eq_tgt = us_tgt + intl_tgt
    total_eq_cur = us_cur + intl_cur
    rows.append(('Total Equities', total_eq_min, total_eq_max, total_eq_tgt, total_eq_cur))

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


#  === IPS BOX & WHISKER PLOT GENERATION ===
def generate_ips_chart(ips_rows):
    """Creates a 'Box and Whisker' style plot for IPS Compliance. Range (Grey Bar), Target (Black Tick), Current (Colored Point)."""
    if not ips_rows: return None
    
    # 1. Prepare Data
    data = []
    for cat, v_min, v_max, v_tgt, v_cur in ips_rows:
        is_compliant = (v_min <= v_cur <= v_max)
        status_color = "#329632" if is_compliant else "#C83232" # Green vs Red
        
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
        labelFont='Calibri', titleFont='Calibri', titleFontSize=12, labelFontSize=12
    ).configure_legend(
        labelFont='Calibri', titleFont='Calibri', titleFontSize=12, labelFontSize=12
    ).configure_text(
        font='Calibri', fontSize=12
    )

    chart_path = "temp_ips_chart.png"
    chart.save(chart_path, scale_factor=3.0)
    return chart_path
    

# === PORTFOLIO RETURN CHART ===
def generate_line_chart(comparison_df):
    """Line Chart: Portfolio vs Benchmark."""
    if comparison_df is None or comparison_df.empty: return None
    
    source = comparison_df.melt('date', var_name='Series', value_name='Cumulative Return')
    domain = ['Portfolio', 'S&P 500']
    range_colors = ['#5978F7', '#7F7F7F'] 
    
    chart = alt.Chart(source).mark_line(strokeWidth=3).encode(
        x=alt.X('date:T', 
                title=None, 
                # GRID=FALSE for X-Axis (No vertical lines)
                axis=alt.Axis(format='%b %Y', labelAngle=0, tickCount=6, labelColor='black', grid=False)
        ), 
        y=alt.Y('Cumulative Return:Q', 
                title=None, 
                # GRID=TRUE for Y-Axis (Horizontal lines only)
                axis=alt.Axis(format='%', grid=True, labelColor='black')
        ), 
        color=alt.Color('Series:N', 
                        scale=alt.Scale(domain=domain, range=range_colors), 
                        legend=alt.Legend(title=None, orient='none', legendX=185, legendY=245, direction='horizontal', labelColor='black')
        )
    ).properties(
        # Increased width to 500 for better aspect ratio when widened on PDF
        width=500, 
        height=200
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
    """Generates a High-Res Altair chart with Legend using Calibri."""
    source = summary_df[
        (summary_df['Type'] == 'Bucket') & 
        (summary_df['Name'] != 'Other')
    ].copy()
    
    if source.empty: return None

    # Colors
    domain = source['Name'].tolist()
    range_colors = ['#0070C0', '#2F5597', '#5978F7', '#BDD7EE', '#7F7F7F', '#D9D9D9']
    
    base = alt.Chart(source).encode(theta=alt.Theta("MarketValue", stack=True))

    # 1. PIE & LEGEND
    pie = base.mark_arc(innerRadius=80, outerRadius=130).encode(
        color=alt.Color(
            "Name", 
            scale=alt.Scale(domain=domain, range=range_colors),
            legend=alt.Legend(
                title="Asset Class", 
                orient="bottom", 
                columns=3,          # <--- Forces 3 columns
                labelLimit=0,       # Prevents cutting off long names
                columnPadding=15    # Adds space between columns
            ) 
        ),
        order=alt.Order('MarketValue', sort="descending")
    )
    
    # 2. LABELS
    text = base.mark_text(radius=156, size=16, font='Calibri', fontWeight='bold').encode(
        text=alt.Text('Allocation', format=".2%"), 
        order=alt.Order('MarketValue', sort="descending"),
        color=alt.value("black")
    )
    
    # 3. COMBINE & CONFIGURE FONTS
    chart = (pie + text).properties(
        width=300, 
        height=375
    ).configure_legend(
        # Set Legend Fonts
        labelFont='Calibri',
        titleFont='Calibri',
        titleFontSize=16,
        labelFontSize=16
    ).configure_text(
        # Set Global Text Fonts (Backup)
        font='Calibri', fontSize=16
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
                           performance_windows=None, performance_chart_data=None, period_label="Period", main_benchmark_tckr="SPY", risk_time_horizon=1,
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
    
    # --- COMMON STYLES ---
    header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
    
    #  ==========================================
    #   COVER PAGE
    #  ==========================================
    pdf.show_standard_header = False 
    pdf.add_page()
    
    # Data extraction
    rpt_title = clean_text(pdf_info.get('page_1_report_title', 'Quarterly Portfolio Report'))
    firm_name = clean_text(pdf_info.get('page_1_firm_name', 'Gaard Capital LLC'))
    acct_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    title_date_input = pdf_info.get('page_1_report_date', report_date)
    title_rep_date = format_nice_date(title_date_input)
    
    # --- Cover Config ---
    left_margin_x = 20
    logo_x_pos = 190
    content_start_y = 65
    
    # --- LEFT SIDE: TEXT BLOCK ---
    pdf.set_y(content_start_y)
    
    # 1. FIRM NAME (Top)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 56)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 15, 'Gaard', new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(5)
    
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 56)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 15, 'Capital LLC', new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(5)
    
    # 2. ACCOUNT NAME (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(0,0,0)
    pdf.cell(0, 12, acct_name, new_x="LMARGIN", new_y="NEXT", align='L')
    
    # 3. REPORT TITLE (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(40, 40, 40) # Dark Grey
    pdf.cell(0, 12, rpt_title, new_x="LMARGIN", new_y="NEXT", align='L')
    
    # 4. REPORT DATE (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, f"{title_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')

    # --- RIGHT SIDE: LOGO ---
    if logo_path and os.path.exists(logo_path):
        try:
            # Place logo at specific X/Y to sit to the right of the text
            pdf.image(logo_path, x=logo_x_pos-10, y=content_start_y-12, w=90)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")
            
    
    #  ==========================================
    #   PAGE 2: GOALS & OBJECTIVES (IPS)
    #  ==========================================
    pdf.header_text = "Goals and Objectives (IPS)"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    # Body Text
    ips_text = clean_text(pdf_info.get('page_3_ips_objectives_text', 'No IPS text provided.'))
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(0, 0, 0)
    pdf.multi_cell(0, 6, ips_text)
    
    #  ==========================================
    #   PAGE 3: STATEMENT OF COMPLIANCE
    #  ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Statement of Compliance"
    pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    reg_name_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    reg_data_style = FontFace(size_pt=12, emphasis=None, color=(0,0,0), fill_color=C_WHITE)
    
    # ---IPS COMPLIANCE TABLE ---    
    with pdf.table(col_widths=(65, 35, 35, 35, 35, 65), 
                   text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="NONE", # No horizontal lines
                   align="LEFT", 
                   width=275, 
                   line_height=8) as table:
        
        # Header
        h = table.row()
        h.cell("Category", style=header_style)
        for t in ["Min", "Max", "Target", "Actual", "Compliance Status"]: 
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
            
    pdf.ln(22)
    
    # --- IPS BOX & WHISKERS CHART ---
    try:
        ips_chart_img = generate_ips_chart(ips_rows)
        if ips_chart_img: pdf.image(ips_chart_img, x=pdf.get_x(), w=265); os.remove("temp_ips_chart.png")
    except Exception as e:
        print(f"IPS Chart Error: {e}")
        
        
    #  ==========================================
    #   PAGE 4: CHANGE IN NET ASSET VALUE
    #  ==========================================
    pdf.header_text = "Change in Portfolio"; pdf.add_page()
    pdf.set_font('Carlito', 'B', 16); pdf.set_text_color(0, 0, 0); pdf.cell(0, 8, account_title, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font('Carlito', '', 8); pdf.set_text_color(*C_TEXT_GREY); pdf.cell(0, 6, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT"); pdf.ln(2)
    breakdown = nav_performance.get('Breakdown', {}) if nav_performance else {}
    if breakdown:
        pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 10, "Net Asset Value", new_x="LMARGIN", new_y="NEXT")
        
        with pdf.table(col_widths=(100, 60), 
                       text_align=("LEFT", "RIGHT"), 
                       borders_layout="NONE",
                       align="LEFT", 
                       width=160, 
                       line_height=8) as table:
            
            # Header
            header = table.row()
            header.cell("Field Name", style=header_style)
            header.cell("Field Value", style=header_style)
            
            # Data
            row_order = ["Starting Value", "Mark-to-Market", "Deposits & Withdrawals", "Dividends", "Interest", "Change in Interest Accruals", "Commissions", "Ending Value"]
            for key in row_order:
                val = breakdown.get(key, 0.0)
                is_bold = key in ["Starting Value", "Ending Value"]
                display_name = key if is_bold else f"      {key}"
                
                # LOGIC CHANGE: Use Grey for bold rows, White for others
                bg_color = C_GREY_LIGHT if is_bold else C_WHITE
                
                style_row = FontFace(emphasis="BOLD" if is_bold else "", size_pt=12, fill_color=bg_color)
                
                r = table.row()
                r.cell(display_name, style=style_row)
                r.cell(f"${val:,.2f}", style=style_row)
       
       
    #  ==========================================
    #   PAGE 5: PORTFOLIO OVERVIEW
    #  ==========================================
    pdf.header_text = "Portfolio Overview"; pdf.add_page()
    pdf.set_font('Carlito', '', 8); pdf.set_text_color(*C_TEXT_GREY); pdf.cell(0, 1, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT"); 
    pdf.ln(3); start_y = pdf.get_y()       

    # --- 1. Performance and Asset Allocation Charts ---
    if performance_chart_data is not None:
        try:
            line_chart_img = generate_line_chart(performance_chart_data)
            if line_chart_img: 
                pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 8, f"Period Performance vs {main_benchmark_tckr}", new_x="LMARGIN", new_y="NEXT")
                pdf.set_y(start_y+16); pdf.image(line_chart_img, w=165); os.remove(line_chart_img)
        except Exception as e:
            print(f"Performance Chart Error: {e}")
    try:
        chart_img = generate_donut_chart(summary_df); 
        if chart_img: 
            pdf.set_y(start_y); pdf.set_x(180); pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 8, "Asset Allocation", new_x="LMARGIN", new_y="NEXT")
            pdf.set_y(start_y+8); pdf.set_x(190); pdf.image(chart_img, w=95); os.remove(chart_img)
    except Exception as e:
        print(f"Asset Allocation Chart Error: {e}")
    
    pdf.ln(10)
    
    # --- 2. NAV PERFORMANCE TABLE ---
    pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0,0,0); pdf.cell(0, 8, "Performance Overview", new_x="LMARGIN", new_y="NEXT")
    cols = ["Account", period_label, "1 Month", "3 Month", "6 Month", "YTD"]
    keys = ["Period", "1M", "3M", "6M", "YTD"]     
    
    with pdf.table(col_widths=(65, 30, 20, 20, 20, 20), 
                   text_align="CENTER", 
                   borders_layout="NONE", 
                   align="LEFT", 
                   width=165) as table:
        
        # Header
        header = table.row()
        for i, c in enumerate(cols): 
            align = "LEFT" if i < 1 else "RIGHT"
            header.cell(c, style=header_style, align=align)
            
        # Data Row
        row = table.row()
        acct_str = account_title[:38] + "..." if len(account_title) > 40 else account_title
        
        # CHANGE 1: Set Background to GREY_LIGHT
        p5_style = FontFace(size_pt=12, fill_color=C_GREY_LIGHT)
        
        # Account Name
        row.cell(acct_str, style=p5_style, align="LEFT")
        
        # CHANGE 2: Period Return (No Border)
        period_date = performance_windows.get("Period") if performance_windows else None
        # Removed border="RIGHT" here
        row.cell(f"{period_date:.2%}" if period_date is not None else "-", style=p5_style, align="RIGHT")
        
        # Trailing Returns
        for k in keys[1:]: 
            val = performance_windows.get(k) if performance_windows else None
            
            # Logic: Right border for 1M, 3M, 6M. No border for YTD.
            b_style = "RIGHT" if k != "YTD" else "NONE"
            row.cell(f"{val:.2%}" if val is not None else "-", style=p5_style, align="RIGHT", border=b_style)
            
        # # --- RISK METRICS TABLE ---
        # risk_y_offset = chart_y_offset + 75
        # risk_y_pos = start_y + risk_y_offset
        # if risk_metrics:
        #     pdf.set_y(risk_y_pos)
        #     pdf.set_x(155)
            
        #     pdf.set_font('Carlito', 'B', 11)
        #     pdf.set_text_color(0, 0, 0)
        #     header_text = f"Risk Profile: {risk_time_horizon} vs {risk_benchmark_tckr}"
        #     pdf.cell(0, 8, header_text, new_x="LMARGIN", new_y="NEXT")
        #     pdf.set_x(165)
            
        #     beta_str = f"{risk_metrics.get('Beta', 0):.2f}"
        #     stdev_str = f"{risk_metrics.get('Daily Standard Deviation', 0):.2%}" 
        #     sharpe_str = f"{risk_metrics.get('Sharpe Ratio', 0):.2f}"
        #     r2_str = f"{risk_metrics.get('R2', 0):.2f}"

        #     col_widths = (10, 14, 13, 14, 20, 17, 16, 14)
        #     with pdf.table(col_widths=col_widths, borders_layout="NONE", align="LEFT", width=125) as table:
        #         row = table.row()
        #         row.cell("Beta", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
        #         row.cell(beta_str, style=FontFace(size_pt=9, emphasis="BOLD"))
        #         row.cell("Sharpe", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
        #         row.cell(sharpe_str, style=FontFace(size_pt=9, emphasis="BOLD"))
        #         row.cell("Std Dev (Day)", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
        #         row.cell(stdev_str, style=FontFace(size_pt=9, emphasis="BOLD"))
        #         row.cell("R-Square", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
        #         row.cell(r2_str, style=FontFace(size_pt=9, emphasis="BOLD"))   


    #  ==========================================
    #   PAGE 6: PORTFOLIO PERFORMANCE BY ALLOCATION
    #  ==========================================
    pdf.header_text = "Portfolio Performance by Allocation"; pdf.add_page()
    pdf.set_font('Carlito', '', 8); pdf.set_text_color(*C_TEXT_GREY); pdf.cell(0, 1, f"Reportings as of {data_rep_date}", new_x="LMARGIN", new_y="NEXT"); pdf.ln(3)
    pdf.set_font('Carlito', 'B', 12); pdf.set_text_color(0, 0, 0); pdf.cell(0, 8, "Allocation Summary", new_x="LMARGIN", new_y="NEXT")
    
    # Styles for Summary
    bucket_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    bench_style = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_WHITE)
    
    with pdf.table(col_widths=(60, 45, 45, 40), 
                   text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="NONE", 
                   align="LEFT", 
                   width=190, 
                   line_height=8) as table:
        
        # Header
        header = table.row()
        for col in ["Asset Class", "Allocation", "Market Value", "Return"]: 
            header.cell(col, style=header_style)
            
        pdf.set_draw_color(*C_GREY_BORDER)
        for _, row in summary_df.iterrows():
            if row['Type'] == 'Bucket':
                r = table.row()
                r.cell(row['Name'], style=bucket_style)
                r.cell(f"{row['Allocation']:.2%}", style=bucket_style, border="RIGHT")
                r.cell(f"${row['MarketValue']:,.2f}", style=bucket_style, border="RIGHT")
                r.cell("---" if row.get('IsCash') else f"{row['Return']:.2%}", style=bucket_style)
            elif row['Type'] == 'Benchmark':
                r = table.row()
                r.cell(f"      {row['Name']}", style=bench_style)
                r.cell("", style=bench_style, border="RIGHT") 
                r.cell("", style=bench_style, border="RIGHT") 
                r.cell(f"{row['Return']:.2%}", style=bench_style)


    #  ==========================================
    #   PAGE 7+: EXPANDED INVESTMENT PERFORMANCE BY ALLOCATION
    #  ==========================================
    pdf.header_text = "Expanded Investment Performance by Allocation"; pdf.add_page()
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

    # 3. CONSTRUCT TABLE
    col_widths = (25, 130, 25, 35, 35, 25)
    
    # Styles
    header_style = FontFace(size_pt=12, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
    
    # Asset Class Row: Name(White), Data(Grey)
    bucket_name_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    bucket_data_style = FontFace(size_pt=12, emphasis="BOLD", color=(0,0,0), fill_color=C_GREY_LIGHT)
    
    # Benchmark Row: Name(White), Return(Grey)
    bench_name_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_GREY_LIGHT)
    bench_data_style  = FontFace(size_pt=12, emphasis="ITALICS", color=C_TEXT_GREY, fill_color=C_GREY_LIGHT)

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
        headers = ["Ticker", "Name", "Allocation", "Cost Basis", "Value", "Return"]
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

            # 2. ASSET CLASS ROW (SUMMARY)
            # Row spans Ticker+Name for the Label
            s_row = table.row()
            s_row.cell(bucket, colspan=2, style=bucket_name_style, align="LEFT")
            
            # Data Columns with Vertical Lines
            pdf.set_draw_color(*C_GREY_BORDER)
            s_row.cell(f"{sum_alloc:.2%}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"${sum_cost:,.2f}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"${sum_value:,.2f}", style=bucket_data_style, border="RIGHT")
            s_row.cell(f"{sum_ret:.2%}", style=bucket_data_style) # No border on last col

            # 3. BENCHMARK ROW (If exists)
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

            # 4. HOLDINGS ROWS
            for _, pos in subset.iterrows():
                r = table.row()
                ret_str = "---" if pos['ticker'] == 'CASH_BAL' else f"{pos['cumulative_return']:.2%}"
                name_str = str(pos.get('official_name', ''))
                if len(name_str) > 60: name_str = name_str[:58] + "..."
                
                # Name Cols
                r.cell(str(pos['ticker']), style=reg_name_style)
                r.cell(name_str, style=reg_name_style)
                
                # Data Cols
                r.cell(f"{pos['weight']:.2%}", style=reg_data_style, border="RIGHT")
                r.cell(f"${pos['avg_cost']:,.2f}", style=reg_data_style, border="RIGHT")
                r.cell(f"${pos['raw_value']:,.2f}", style=reg_data_style, border="RIGHT")
                r.cell(ret_str, style=reg_data_style)

    
    #  ==========================================
    #   PAGE 8: RISK
    #  ==========================================
    pdf.header_text = "Risk"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 9: FINANCIAL STATISTICS
    #  ==========================================
    pdf.header_text = "Financial Statistics"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 10: MACRO VIEWS, EMPIRICAL
    #  ==========================================
    pdf.header_text = "Macro Views, Empirical"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    
    #  ==========================================
    #   PAGE 11: MARKET RECAP
    #  ==========================================
    pdf.header_text = "Market Recap"; pdf.add_page()
    
    # 1. Clean and Get Text
    raw_text = clean_text(pdf_info.get('page_11_macro_market_recap', 'No macro views provided.'))
    
    # 2. Split into Paragraphs & Deep Clean
    # .replace('\xa0', ' ') removes non-breaking spaces which cause "weird indents"
    paragraphs = [p.replace('\xa0', ' ').strip() for p in raw_text.split('\n') if p.strip()]
    
    # 3. Balance Columns (Auto-Distribute)
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
            
    # 4. Define Layout Dimensions
    col_gap = 5
    col_width = (297 - 40 - col_gap) / 2  # (PageW - Margins - Gap) / 2
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(40, 40, 40)
    
    # Capture Starting Y position
    start_y = pdf.get_y() + 1
    
    # --- RENDER COLUMN 1 (Left) ---
    pdf.set_y(start_y)
    for p in col1_paras:
        pdf.set_x(10)  # <--- FORCE X ALIGNMENT EVERY TIME
        pdf.multi_cell(col_width, 6, p)
        pdf.ln(2) 
        
    # --- RENDER COLUMN 2 (Right) ---
    pdf.set_y(start_y)      # Reset Y to top for the second column
    col2_x = 10 + col_width + col_gap
    
    for p in col2_paras:
        pdf.set_x(col2_x) # <--- FORCE X ALIGNMENT EVERY TIME (Fixes "drift" to left margin)
        pdf.multi_cell(col_width, 6, p)
        pdf.ln(2)
    
    
    #  ==========================================
    #   PAGE 12: IMPORTANT INFO AND DISCLOSURES
    #  ==========================================
    pdf.header_text = "Important Information and Disclosures"; pdf.add_page()
    pdf.set_y(15)
    
    pdf.ln(10)
    
    # Body Text
    disclaimer_text = clean_text(pdf_info.get('page_2_disclaimer', 'No disclosures provided.'))
    
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(40, 40, 40)
    pdf.multi_cell(0, 6, disclaimer_text)     # Multi_cell allows text wrapping
    
    # -- LEGAL NOTES TABLE --
    if legal_notes is not None and not legal_notes.empty:
        pdf.ln(10)
        
        # 1. Title is Bold
        pdf.set_font('Carlito', 'B', 14)
        pdf.set_text_color(*C_BLUE_LOGO)
        pdf.cell(0, 10, "Statement Notes", new_x="LMARGIN", new_y="NEXT")
        
        # 2. Reset Font to Regular BEFORE the table just in case
        pdf.set_font('Carlito', '', 8)
        
        notes_df = legal_notes.copy()
            
        with pdf.table(col_widths=(30, 240),
                       text_align=("LEFT", "LEFT"),
                       borders_layout="HORIZONTAL_LINES",
                       align="LEFT",
                       width=270,
                       line_height=5) as table: 
            
            # Header Style
            legalnotes_header_style = FontFace(size_pt=8, emphasis="BOLD", color=C_WHITE, fill_color=C_BLUE_LOGO)
            h = table.row()
            h.cell("Type", style=legalnotes_header_style)
            h.cell("Note", style=legalnotes_header_style)
            
            # Data Style
            # CHANGE HERE: Use emphasis="" to force Regular (None inherits Bold)
            note_style = FontFace(size_pt=8, emphasis="", color=(0,0,0), fill_color=C_WHITE)
            
            for _, row in notes_df.iterrows():
                r = table.row()
                
                type_val = str(row.get('Type', ''))
                note_val = str(row.get('Note', ''))
                
                r.cell(type_val, style=note_style)
                r.cell(note_val, style=note_style)
                
    #  ==========================================
    #   PAGE 13: END COVER PAGE
    #  ==========================================
    pdf.show_standard_header = False 
    pdf.add_page()
    
    # Data extraction
    rpt_title = clean_text(pdf_info.get('page_1_report_title', 'Quarterly Portfolio Report'))
    firm_name = clean_text(pdf_info.get('page_1_firm_name', 'Gaard Capital LLC'))
    acct_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    title_date_input = pdf_info.get('page_1_report_date', report_date)
    title_rep_date = format_nice_date(title_date_input)
    
    # --- Cover Config ---
    left_margin_x = 20
    logo_x_pos = 190
    content_start_y = 65
    
    # --- LEFT SIDE: TEXT BLOCK ---
    pdf.set_y(content_start_y)
    
    # 1. FIRM NAME (Top)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 56)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 15, 'Gaard', new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(5)
    
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 56)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 15, 'Capital LLC', new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.ln(5)
    
    # 2. ACCOUNT NAME (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(0,0,0)
    pdf.cell(0, 12, acct_name, new_x="LMARGIN", new_y="NEXT", align='L')
    
    # 3. REPORT TITLE (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', 'B', 18)
    pdf.set_text_color(40, 40, 40) # Dark Grey
    pdf.cell(0, 12, rpt_title, new_x="LMARGIN", new_y="NEXT", align='L')
    
    # 4. REPORT DATE (Underneath)
    pdf.set_x(left_margin_x)
    pdf.set_font('Carlito', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, f"{title_rep_date}", new_x="LMARGIN", new_y="NEXT", align='L')

    # --- RIGHT SIDE: LOGO ---
    if logo_path and os.path.exists(logo_path):
        try:
            # Place logo at specific X/Y to sit to the right of the text
            pdf.image(logo_path, x=logo_x_pos-10, y=content_start_y-12, w=90)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")
            
    pdf.suppress_footer = True
    
    # == PDF OUTPUT ===
    if output_path.endswith('.xlsx'):
        output_path = output_path.replace('.xlsx', '.pdf')
    
    pdf.output(output_path)
    if os.path.exists("temp_chart.png"): os.remove("temp_chart.png")
    print(f"   > SUCCESS: PDF saved to {output_path}")