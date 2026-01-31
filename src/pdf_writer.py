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
C_TEXT_GREY    = (100, 100, 100)  

class PortfolioPDF(FPDF):
    def __init__(self, orientation='P', unit='mm', format='A4'):
        super().__init__(orientation, unit, format)
        # Flag to control if the standard blue header appears
        self.show_standard_header = False
        self.header_text = "PORTFOLIO REPORT" 

    def header(self):
        # Only show the standard "PORTFOLIO STATEMENT" header if flag is True
        if self.show_standard_header:
            # 1. Clean Top Bar
            self.set_fill_color(*C_BLUE_LOGO)
            self.rect(0, 0, self.w, 4, 'F') 
            
            # 2. Title
            self.set_y(10)
            self.set_font('Arial', 'B', 14)
            self.set_text_color(*C_BLUE_LOGO)
            self.cell(0, 10, self.header_text, new_x="LMARGIN", new_y="NEXT", align='L')
            self.ln(2)

    def footer(self):
        # We generally want footers on all pages (or you can restrict this too)
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')

# === ASSET ALLOCATION CHART CREATION ===
def generate_donut_chart(summary_df):
    """Generates a High-Res Altair chart with Legend."""
    source = summary_df[
        (summary_df['Type'] == 'Bucket') & 
        (summary_df['Name'] != 'Other')
    ].copy()
    
    if source.empty: return None

    # Colors
    domain = source['Name'].tolist()
    range_colors = ['#0070C0', '#2F5597', '#5978F7', '#BDD7EE', '#7F7F7F', '#D9D9D9']
    
    base = alt.Chart(source).encode(theta=alt.Theta("MarketValue", stack=True))

    # PIE & LEGEND
    pie = base.mark_arc(innerRadius=60, outerRadius=100).encode(
        color=alt.Color(
            "Name", 
            scale=alt.Scale(domain=domain, range=range_colors),
            legend=alt.Legend(title="Asset Class", orient="right") 
        ),
        order=alt.Order('MarketValue', sort="descending")
    )
    
    # LABELS
    text = base.mark_text(radius=120).encode(
        text=alt.Text('Allocation', format=".0%"), 
        order=alt.Order('MarketValue', sort="descending"),
        color=alt.value("black")
    )
    
    chart = (pie + text).properties(width=300, height=300)

    chart_path = "temp_chart.png"
    chart.save(chart_path, scale_factor=3.0)
    return chart_path


# === IPS TABLE DATA ===
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
            "Current": v_cur,
            "StatusColor": status_color
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
    currents = base.mark_circle(size=120, opacity=1).encode(
        x="Current",
        color=alt.Color("StatusColor", scale=None)
    )

    # Combine Layers
    chart = (ranges + targets + currents).properties(
        width=800, 
        height=200
    ).configure_axis(
        labelFont='Helvetica',
        titleFont='Helvetica'
    ).configure_legend(
        labelFont='Helvetica',
        titleFont='Helvetica'
    ).configure_text(
        font='Helvetica'
    )

    chart_path = "temp_ips_chart.png"
    chart.save(chart_path, scale_factor=3.0)
    return chart_path
    
    
# ==========================================
#  OVERALL PORTFOLIO REPORT
# ==========================================
def write_portfolio_report(summary_df, holdings_df, nav_performance, total_metrics, risk_metrics, report_date, output_path, account_title="Total Portfolio",
                           risk_benchmark_tckr="SPY", risk_time_horizon=1, pdf_info=None, logo_path=None):
    
    print(f"   > Generating PDF Report: {output_path}")
    if pdf_info is None: pdf_info = {}
    
    # Pre-calculate IPS Data Rows
    ips_rows = get_ips_table_data(pdf_info, summary_df)
    
    # --- CLEAN TEXT HELPER FUNCTION FOR PDF INFO FROM data/info_for_pdf.xlsx ---
    def clean_text(text):
        """Replaces 'smart' punctuation (unsupported by Arial) with standard ASCII."""
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
    # ----------------------------------------------------------------------------
    
    # 1. SETUP
    pdf = PortfolioPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15)

    # ==========================================
    # PAGE 1: COVER PAGE
    # ==========================================
    pdf.show_standard_header = False 
    pdf.add_page()
    
    # Data extraction
    rpt_title = clean_text(pdf_info.get('page_1_report_title', 'Quarterly Portfolio Report'))
    firm_name = clean_text(pdf_info.get('page_1_firm_name', 'Gaard Capital, LLC'))
    acct_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    raw_date_str = clean_text(pdf_info.get('page_1_report_date', report_date))
    raw_date = raw_date_str.split(' ')[0]
    
    # Start Content (Adjust Y to vertically center the whole block)
    pdf.set_y(40)
    
    # 1. REPORT TITLE
    pdf.set_font('Arial', 'B', 24)
    pdf.set_text_color(*C_BLUE_PRIMARY)
    pdf.set_x(0) 
    pdf.cell(pdf.w, 15, rpt_title, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 2. FIRM NAME
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(0, 0, 0)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, firm_name, align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.ln(33)
    
    # 3. ACCOUNT NAME
    pdf.set_font('Arial', 'B', 22)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, acct_name, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 4. REPORT DATE
    pdf.set_font('Arial', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.set_x(0)
    pdf.cell(pdf.w, 10, f"As of {raw_date}", align='C', new_x="LMARGIN", new_y="NEXT")

    # 5. GAARD LOGO
    if logo_path and os.path.exists(logo_path):
        try:
            # Dynamic Calculation: (Page Width - Image Width) / 2
            img_w = 45
            x_pos = (pdf.w - img_w) / 2
            
            logo_y = pdf.get_y() + 53
            pdf.image(logo_path, x=x_pos, y=logo_y, w=img_w)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")
            
            
    # ==========================================
    # PAGE 2: DISCLOSURES
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Disclosures"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    # Body Text
    disclaimer_text = clean_text(pdf_info.get('page_2_disclaimer', 'No disclosures provided.'))
    
    pdf.set_font('Arial', '', 10)
    pdf.set_text_color(40, 40, 40)
    pdf.multi_cell(0, 6, disclaimer_text)     # Multi_cell allows text wrapping


    # ==========================================
    # PAGE 3: GOALS & OBJECTIVES (IPS)
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Goals and Objectives (IPS)"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    # Body Text
    ips_text = clean_text(pdf_info.get('page_3_ips_objectives_text', 'No IPS text provided.'))
    
    pdf.set_font('Arial', '', 11)
    pdf.set_text_color(0, 0, 0)
    pdf.multi_cell(0, 6, ips_text)
    

    # ==========================================
    # PAGE 4: IPS CATEGORY RANGE VS ACTUAL
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "IPS Category Ranges vs. Current Allocation"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(15)
    
    # ---IPS COMPLIANCE TABLE ---    
    with pdf.table(col_widths=(65, 35, 35, 35, 35, 60), 
                   text_align=("LEFT", "CENTER", "CENTER", "CENTER", "CENTER", "CENTER"),
                   borders_layout="HORIZONTAL_LINES", 
                   align="LEFT", 
                   width=270,  
                   line_height=7) as table:
        
        h = table.row()
        h.cell("Category", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
        h.cell("Min", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
        h.cell("Target", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9)) 
        h.cell("Max", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
        h.cell("Current Allocation", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
        h.cell("Compliance Status", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
        
        for cat, v_min, v_max, v_tgt, v_cur in ips_rows:
            r = table.row()
            r.cell(cat, style=FontFace(size_pt=9))
            r.cell(f"{v_min:.0%}", style=FontFace(size_pt=9))
            r.cell(f"{v_tgt:.0%}", style=FontFace(size_pt=9))
            r.cell(f"{v_max:.0%}", style=FontFace(size_pt=9))
            r.cell(f"{v_cur:.1%}", style=FontFace(size_pt=9))
            
            # Compliance Check
            if v_min <= v_cur <= v_max:
                r.cell("Compliant", style=FontFace(size_pt=9, emphasis="BOLD", color=(50, 150, 50)))
            else:
                r.cell("Non-Compliant", style=FontFace(size_pt=9, emphasis="BOLD", color=(200, 50, 50)))

    pdf.ln(20)

    # --- IPS BOX & WHISKERS CHART ---
    # pdf.set_font('Arial', 'B', 11)
    # pdf.set_text_color(0, 0, 0)
    # pdf.cell(0, 8, "IPS Ranges" , new_x="LMARGIN", new_y="NEXT")
    
    try:
        ips_chart_img = generate_ips_chart(ips_rows)
        if ips_chart_img:
            # Place image at current X (left margin), width=265mm
            pdf.image(ips_chart_img, x=pdf.get_x(), w=265)
            
            if os.path.exists("temp_ips_chart.png"): 
                os.remove("temp_ips_chart.png")
    except Exception as e:
        print(f"IPS Chart Error: {e}")
        
         
    # ==========================================
    # PAGE 5: NET ASSET VALUE
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Overall Performance (Quarterly)"
    pdf.add_page()

    # --- HEADER INFO ---
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, account_title, new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font('Arial', '', 6)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 6, f"Reportings as of {report_date}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    
    # === CHANGE IN NAV TABLE ===
    breakdown = nav_performance.get('Breakdown', {}) if nav_performance else {}
    
    if breakdown:
        pdf.set_font('Arial', 'B', 11)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 10, "Change in Net Asset Value", new_x="LMARGIN", new_y="NEXT")
        
        # Table Config: 2 Columns
        with pdf.table(col_widths=(100, 60), 
                       text_align=("LEFT", "RIGHT"), 
                       borders_layout="HORIZONTAL_LINES", 
                       align="LEFT", 
                       width=160, 
                       line_height=8) as table:
            
            # 1. Header Row
            header = table.row()
            header.cell("Field Name", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            header.cell("Field Value", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            
            # 2. Data Rows
            row_order = [
                "Starting Value", 
                "Mark-to-Market", 
                "Deposits & Withdrawals", 
                "Dividends", 
                "Interest", 
                "Change in Interest Accruals", 
                "Commissions", 
                "Ending Value"
            ]
            
            for key in row_order:
                val = breakdown.get(key, 0.0)
                
                # Formatting Logic
                is_bold = key in ["Starting Value", "Ending Value"]
                should_indent = not is_bold 
                
                # Indentation
                display_name = key
                if should_indent:
                    display_name = f"      {key}" 
                
                # Font Style
                weight = "BOLD" if is_bold else ""
                
                # Render Row
                r = table.row()
                r.cell(display_name, style=FontFace(emphasis=weight, size_pt=9))
                r.cell(f"${val:,.2f}", style=FontFace(emphasis=weight, size_pt=9))
       
       
    # ==========================================
    # PAGE 6: PORTFOLIO VALUE OVER TIME
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Portfolio Value Over Time"
    pdf.add_page()

    # --- HEADER INFO ---
    # pdf.set_font('Arial', 'B', 16)
    # pdf.set_text_color(0, 0, 0)
    # pdf.cell(0, 8, account_title, new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font('Arial', '', 6)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 6, f"Reportings as of {report_date}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)

    # === TWO-COLUMN LAYOUT START ===
    start_y = pdf.get_y()
    
    # --- LEFT COLUMN: DATA ---
    # 1. NAV PERFORMANCE TABLE
    if nav_performance:
        pdf.set_font('Arial', 'B', 11)
        pdf.set_text_color(0,0,0)
        pdf.cell(0, 8, "Performance History", new_x="LMARGIN", new_y="NEXT")
        
        # Define Values
        nav_val = nav_performance.get('NAV', 0.0)
        nav_ret = nav_performance.get('Return', 0.0)
        
        nav_str = f"${nav_val:,.2f}"
        ret_str = f"{nav_ret:.2%}"
        
        # Draw Table with Columns: Description (Merged), Market Value, Return
        with pdf.table(col_widths=(50, 25, 25, 30), text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                       borders_layout="HORIZONTAL_LINES", align="LEFT", width=130) as table:
            
            # Header Row
            header = table.row()
            header.cell("Account", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            header.cell("Total Value", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            header.cell("", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9)) # Spacer
            header.cell("Total Return", style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            
            # Data Row
            row = table.row()
            row.cell("Account # or Name?", style=FontFace(emphasis="BOLD", size_pt=9))
            row.cell(nav_str, style=FontFace(size_pt=9))
            row.cell("", style=FontFace(size_pt=9))
            row.cell(ret_str, style=FontFace(size_pt=9))

    pdf.ln(16)
     
    # --- RIGHT COLUMN: ALLOCATION CHART---
    # --- ASSET ALLOCATION CHART ---
    try:
        chart_img = generate_donut_chart(summary_df)
        if chart_img:      
            chart_y_offset = 0
            pdf.set_y(start_y + chart_y_offset) 
            pdf.set_x(155) 
            pdf.set_font('Arial', 'B', 11)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(0, 8, "Asset Allocation", new_x="LMARGIN", new_y="NEXT")
            
            pdf.set_y(start_y + chart_y_offset + 8)
            pdf.set_x(170)
            pdf.image(chart_img, w=110)
            
        # # --- RISK METRICS TABLE ---
        # risk_y_offset = chart_y_offset + 75
        # risk_y_pos = start_y + risk_y_offset
        # if risk_metrics:
        #     pdf.set_y(risk_y_pos)
        #     pdf.set_x(155)
            
        #     pdf.set_font('Arial', 'B', 11)
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
    except Exception as e:
        print(f"Chart Error: {e}")

    # ==========================================
    # PAGE 7: CATEGORY ALLOCATION & PERFORMANCE
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Category Allocation and Performance by Model"
    pdf.add_page()
    
    pdf.set_font('Arial', '', 6)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 6, f"Reportings as of {report_date}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    
    # --- SUMMARY TABLE ---
    pdf.set_font('Arial', 'B', 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, "Allocation Summary", new_x="LMARGIN", new_y="NEXT")
    
    with pdf.table(col_widths=(60, 45, 45, 40), text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="HORIZONTAL_LINES", align="LEFT", width=190, line_height=7) as table:
        header = table.row()
        for col in ["Asset Class", "Market Value", "Allocation", "Return"]:
            header.cell(col, style=FontFace(emphasis="BOLD", color=C_BLUE_PRIMARY, size_pt=9))
            
        for _, row in summary_df.iterrows():
            row_type = row['Type']
            name = row['Name']
            
            if row_type == 'Bucket':
                r = table.row()
                r.cell(name, style=FontFace(emphasis="BOLD", size_pt=9))
                r.cell(f"${row['MarketValue']:,.2f}", style=FontFace(size_pt=9))
                r.cell(f"{row['Allocation']:.2%}", style=FontFace(size_pt=9))
                if row.get('IsCash', False):
                    r.cell("---", style=FontFace(size_pt=9, color=C_TEXT_GREY))
                else:
                    r.cell(f"{row['Return']:.2%}", style=FontFace(size_pt=9))
            elif row_type == 'Benchmark':
                r = table.row()
                r.cell(f"      {name}", style=FontFace(emphasis="ITALICS", size_pt=8, color=C_TEXT_GREY))
                r.cell("") 
                r.cell("") 
                r.cell(f"{row['Return']:.2%}", style=FontFace(emphasis="ITALICS", size_pt=8, color=C_TEXT_GREY))

    # ==========================================
    # PAGE 7+: EXPANDED CATEGORY HOLDINGS & PERFORMANCE
    # ==========================================
    pdf.header_text = "Expanded Category Holdings and Performance"
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)

    bucket_order = summary_df[summary_df['Type'] == 'Bucket']['Name'].unique().tolist()
    sorter_map = {name: i for i, name in enumerate(bucket_order)}
    holdings_df['sort_key'] = holdings_df['asset_class'].map(sorter_map).fillna(999)
    sorted_holdings = holdings_df.sort_values(['sort_key', 'weight'], ascending=[True, False])

    unique_buckets = sorted_holdings['asset_class'].unique()

    for bucket in unique_buckets:
        pdf.set_font('Arial', 'B', 11)
        pdf.set_text_color(*C_BLUE_PRIMARY)
        pdf.cell(0, 10, bucket.upper(), new_x="LMARGIN", new_y="NEXT")
        
        with pdf.table(col_widths=(25, 110, 35, 35, 20, 20), 
                       text_align=("LEFT", "LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"),
                       borders_layout="HORIZONTAL_LINES", align="LEFT") as table:
            h_row = table.row()
            headers = ["Ticker", "Name", "Cost Basis", "Value", "Alloc", "Return"]
            for h in headers:
                h_row.cell(h, style=FontFace(size_pt=8, emphasis="BOLD", color=C_TEXT_GREY))
            
            subset = sorted_holdings[sorted_holdings['asset_class'] == bucket]
            for _, pos in subset.iterrows():
                r = table.row()
                ret_str = "---" if pos['ticker'] == 'CASH_BAL' else f"{pos['cumulative_return']:.2%}"
                name_str = str(pos.get('official_name', ''))
                if len(name_str) > 80: name_str = name_str[:78] + "..."
                
                r.cell(str(pos['ticker']), style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(name_str, style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"${pos['avg_cost']:,.0f}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"${pos['raw_value']:,.0f}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"{pos['weight']:.1%}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(ret_str, style=FontFace(size_pt=8, color=(0,0,0)))

    
    # ==========================================
    # PAGE 8: RISK
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Risk"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    
    # ==========================================
    # PAGE 9: FINANCIAL STATISTICS
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Financial Statistics"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    
    # ==========================================
    # PAGE 10: MACRO VIEWS, EMPIRICAL
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Macro Views, Empirical"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    
    # ==========================================
    # PAGE 11: MACRO VIEWS, VERBAL
    # ==========================================
    pdf.show_standard_header = True
    pdf.header_text = "Macro Views, Verbal"
    pdf.add_page()
    pdf.set_y(20)
    
    pdf.ln(10)
    
    # Body Text
    disclaimer_text = clean_text(pdf_info.get('page_11_macro_market_recap', 'No macro views provided.'))
    
    pdf.set_font('Arial', '', 10)
    pdf.set_text_color(40, 40, 40)
    pdf.multi_cell(0, 6, disclaimer_text)  
    
    
    # ==========================================
    # PAGE 12: END COVER PAGE
    # ==========================================
    pdf.show_standard_header = False 
    pdf.add_page()
    
    # Data extraction
    rpt_title = clean_text(pdf_info.get('page_1_report_title', 'Quarterly Portfolio Report'))
    firm_name = clean_text(pdf_info.get('page_1_firm_name', 'Gaard Capital, LLC'))
    acct_name = clean_text(pdf_info.get('page_1_account_name', account_title))
    raw_date_str = clean_text(pdf_info.get('page_1_report_date', report_date))
    raw_date = raw_date_str.split(' ')[0]
    
    # Start Content (Adjust Y to vertically center the whole block)
    pdf.set_y(40)
    
    # 1. REPORT TITLE
    pdf.set_font('Arial', 'B', 24)
    pdf.set_text_color(*C_BLUE_PRIMARY)
    pdf.set_x(0) 
    pdf.cell(pdf.w, 15, rpt_title, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 2. FIRM NAME
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(0, 0, 0)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, firm_name, align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.ln(33)
    
    # 3. ACCOUNT NAME
    pdf.set_font('Arial', 'B', 22)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, acct_name, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 4. REPORT DATE
    pdf.set_font('Arial', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.set_x(0)
    pdf.cell(pdf.w, 10, f"As of {raw_date}", align='C', new_x="LMARGIN", new_y="NEXT")

    # 5. GAARD LOGO
    if logo_path and os.path.exists(logo_path):
        try:
            # Dynamic Calculation: (Page Width - Image Width) / 2
            img_w = 45
            x_pos = (pdf.w - img_w) / 2
            
            logo_y = pdf.get_y() + 53
            pdf.image(logo_path, x=x_pos, y=logo_y, w=img_w)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")
        
    # == PDF OUTPUT ===
    if output_path.endswith('.xlsx'):
        output_path = output_path.replace('.xlsx', '.pdf')
    
    pdf.output(output_path)
    if os.path.exists("temp_chart.png"): os.remove("temp_chart.png")
    print(f"   > SUCCESS: PDF saved to {output_path}")