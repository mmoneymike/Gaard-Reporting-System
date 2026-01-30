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

    def header(self):
        # Only show the standard "PORTFOLIO STATEMENT" header if flag is True
        if self.show_standard_header:
            # 1. Clean Top Bar
            self.set_fill_color(*C_BLUE_PRIMARY)
            self.rect(0, 0, self.w, 4, 'F') 
            
            # 2. Title
            self.set_y(10)
            self.set_font('Helvetica', 'B', 14)
            self.set_text_color(*C_BLUE_PRIMARY)
            self.cell(0, 10, 'PORTFOLIO STATEMENT', new_x="LMARGIN", new_y="NEXT", align='L')
            self.ln(2)

    def footer(self):
        # We generally want footers on all pages (or you can restrict this too)
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')


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
    
    chart = (pie + text).properties(width=300, height=200)

    chart_path = "temp_chart.png"
    chart.save(chart_path, scale_factor=3.0)
    return chart_path

# ==========================================
#  OVERALL PORTFOLIO REPORT
# ==========================================
def write_portfolio_report(summary_df, holdings_df, total_metrics, risk_metrics, report_date, output_path, account_title="Total Portfolio",
                           risk_benchmark_tckr="SPY", risk_time_horizon=1, pdf_info=None, logo_path=None):
    
    print(f"   > Generating PDF Report: {output_path}")
    if pdf_info is None: pdf_info = {}
    
    # --- CLEAN TEXT HELPER FUNCTION FOR PDF INFO FROM data/info_for_pdf.xlsx ---
    def clean_text(text):
        """
        Replaces 'smart' punctuation (unsupported by Helvetica) with standard ASCII.
        """
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
    pdf.set_font('Helvetica', 'B', 24)
    pdf.set_text_color(*C_BLUE_PRIMARY)
    pdf.set_x(0) 
    pdf.cell(pdf.w, 15, rpt_title, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 2. FIRM NAME
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(0, 0, 0)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, firm_name, align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.ln(26)
    
    # 3. ACCOUNT NAME
    pdf.set_font('Helvetica', 'B', 22)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.set_x(0)
    pdf.cell(pdf.w, 12, acct_name, align='C', new_x="LMARGIN", new_y="NEXT")
    
    # 4. REPORT DATE
    pdf.set_font('Helvetica', '', 12)
    pdf.set_text_color(100, 100, 100)
    pdf.set_x(0)
    pdf.cell(pdf.w, 10, f"As of {raw_date}", align='C', new_x="LMARGIN", new_y="NEXT")

    # 5. GAARD LOGO
    if logo_path and os.path.exists(logo_path):
        try:
            # Dynamic Calculation: (Page Width - Image Width) / 2
            img_w = 45
            x_pos = (pdf.w - img_w) / 2
            
            logo_y = pdf.get_y() + 60
            pdf.image(logo_path, x=x_pos, y=logo_y, w=img_w)
        except Exception as e: 
            print(f"Warning: Could not load logo: {e}")
            
    # ==========================================
    # PAGE 2: DISCLOSURES
    # ==========================================
    pdf.add_page()
    pdf.set_y(20)
    
    # Header
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 10, "Important Disclosures", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    # Body Text
    disclaimer_text = clean_text(pdf_info.get('page_2_disclaimer', 'No disclosures provided.'))
    
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(40, 40, 40)
    # Multi_cell allows text wrapping
    pdf.multi_cell(0, 6, disclaimer_text)

    # ==========================================
    # PAGE 3: GOALS & OBJECTIVES (IPS)
    # ==========================================
    pdf.add_page()
    pdf.set_y(20)
    
    # Header
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(*C_BLUE_LOGO)
    pdf.cell(0, 10, "Investment Policy Statement: Goals & Objectives", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    # Body Text
    ips_text = clean_text(pdf_info.get('page_3_ips_objectives_text', 'No IPS text provided.'))
    
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(0, 0, 0)
    pdf.multi_cell(0, 7, ips_text)

    # ==========================================
    # PAGE 4+: DASHBOARD (Existing)
    # ==========================================
    # Turn on the standard header for the subsequent pages
    pdf.show_standard_header = True
    pdf.add_page()

    # --- HEADER INFO (Manual writing removed as it's now in header(), but page-specific subtitles remain) ---
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 8, account_title, new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font('Helvetica', '', 6)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 6, f"Reportings as of {report_date}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # === TWO-COLUMN LAYOUT START ===
    start_y = pdf.get_y()
    
    # --- LEFT COLUMN: DATA (Width ~140mm) ---
    # 1. COMPACT SCORECARD
    val_str = f"${total_metrics['value']:,.0f}" if isinstance(total_metrics['value'], (int, float)) else str(total_metrics['value'])
    ret_str = f"{total_metrics['return']:.2%}" if isinstance(total_metrics['return'], (int, float)) else str(total_metrics['return'])

    # Labels
    with pdf.table(col_widths=(30, 30), borders_layout="NONE", align="LEFT", width=70) as table:
        row = table.row()
        row.cell("TOTAL VALUE", style=FontFace(size_pt=8, color=C_TEXT_GREY))
        row.cell("RETURN", style=FontFace(size_pt=8, color=C_TEXT_GREY))
        
    # Values (Smaller Font: 14pt)
    with pdf.table(col_widths=(30, 30), borders_layout="NONE", align="LEFT", width=70) as table:
        row = table.row()
        row.cell(val_str, style=FontFace(size_pt=14, emphasis="BOLD", color=C_BLUE_LOGO))
        row.cell(ret_str, style=FontFace(size_pt=14, emphasis="BOLD", color=(50, 150, 50)))

    pdf.ln(5)
     
    # 2. SUMMARY TABLE (Left Aligned)
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, "Allocation Summary", new_x="LMARGIN", new_y="NEXT")
    
    with pdf.table(col_widths=(50, 35, 25, 20), text_align=("LEFT", "RIGHT", "RIGHT", "RIGHT"), 
                   borders_layout="HORIZONTAL_LINES", align="LEFT", width=130) as table:
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

    # --- RIGHT COLUMN: ALLOCATION CHART---
    # --- ASSET ALLOCATION CHART ---
    try:
        chart_img = generate_donut_chart(summary_df)
        if chart_img:      
            chart_y_offset = 54
            pdf.set_y(start_y + chart_y_offset) 
            pdf.set_x(155) 
            pdf.set_font('Helvetica', 'B', 11)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(0, 8, "Asset Allocation", new_x="LMARGIN", new_y="NEXT")
            
            pdf.set_y(start_y + chart_y_offset + 8)
            pdf.set_x(160)
            pdf.image(chart_img, w=110)
            
        # --- RISK METRICS TABLE ---
        risk_y_offset = chart_y_offset + 75
        risk_y_pos = start_y + risk_y_offset
        if risk_metrics:
            pdf.set_y(risk_y_pos)
            pdf.set_x(155)
            
            pdf.set_font('Helvetica', 'B', 11)
            pdf.set_text_color(0, 0, 0)
            header_text = f"Risk Profile: {risk_time_horizon} vs {risk_benchmark_tckr}"
            pdf.cell(0, 8, header_text, new_x="LMARGIN", new_y="NEXT")
            pdf.set_x(165)
            
            beta_str = f"{risk_metrics.get('Beta', 0):.2f}"
            stdev_str = f"{risk_metrics.get('Daily Standard Deviation', 0):.2%}" 
            sharpe_str = f"{risk_metrics.get('Sharpe Ratio', 0):.2f}"
            r2_str = f"{risk_metrics.get('R2', 0):.2f}"

            col_widths = (10, 14, 13, 14, 20, 17, 16, 14)
            with pdf.table(col_widths=col_widths, borders_layout="NONE", align="LEFT", width=125) as table:
                row = table.row()
                row.cell("Beta", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
                row.cell(beta_str, style=FontFace(size_pt=9, emphasis="BOLD"))
                row.cell("Sharpe", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
                row.cell(sharpe_str, style=FontFace(size_pt=9, emphasis="BOLD"))
                row.cell("Std Dev (Day)", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
                row.cell(stdev_str, style=FontFace(size_pt=9, emphasis="BOLD"))
                row.cell("R-Square", style=FontFace(size_pt=8, color=C_BLUE_PRIMARY))
                row.cell(r2_str, style=FontFace(size_pt=9, emphasis="BOLD"))   
    except Exception as e:
        print(f"Chart Error: {e}")

    # === PAGE 5: DETAILED HOLDINGS ===
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 14)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 10, "Holdings Detail", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    bucket_order = summary_df[summary_df['Type'] == 'Bucket']['Name'].unique().tolist()
    sorter_map = {name: i for i, name in enumerate(bucket_order)}
    holdings_df['sort_key'] = holdings_df['asset_class'].map(sorter_map).fillna(999)
    sorted_holdings = holdings_df.sort_values(['sort_key', 'weight'], ascending=[True, False])

    unique_buckets = sorted_holdings['asset_class'].unique()

    for bucket in unique_buckets:
        pdf.set_font('Helvetica', 'B', 11)
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
                ret_str = "---" if pos['ticker'] == 'CASH_BAL' else f"{pos['cumulative_return']:.1%}"
                name_str = str(pos.get('official_name', ''))
                if len(name_str) > 80: name_str = name_str[:78] + "..."
                
                r.cell(str(pos['ticker']), style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(name_str, style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"${pos['avg_cost']:,.0f}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"${pos['raw_value']:,.0f}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(f"{pos['weight']:.1%}", style=FontFace(size_pt=8, color=(0,0,0)))
                r.cell(ret_str, style=FontFace(size_pt=8, color=(0,0,0)))
        
        pdf.ln(5)

    if output_path.endswith('.xlsx'):
        output_path = output_path.replace('.xlsx', '.pdf')
    
    pdf.output(output_path)
    if os.path.exists("temp_chart.png"): os.remove("temp_chart.png")
    print(f"   > SUCCESS: PDF saved to {output_path}")