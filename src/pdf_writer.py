import pandas as pd
import altair as alt
from fpdf import FPDF
from fpdf.fonts import FontFace
import os

# --- CONSTANTS & COLORS ---
C_BLUE_PRIMARY = (89, 120, 247)   # #5978F7
C_BLUE_DARK    = (47, 85, 151)    # #2F5597
C_GREY_LIGHT   = (245, 247, 255)  
C_TEXT_GREY    = (100, 100, 100)  

class PortfolioPDF(FPDF):
    def header(self):
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
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')


# ==========================================
#  ALLOCATION CHART
# ==========================================
def generate_donut_chart(summary_df):
    """Generates a High-Res Altair chart with Legend."""
    source = summary_df[
        (summary_df['Type'] == 'Bucket') & 
        (summary_df['Name'] != 'Other')
    ].copy()
    
    if source.empty: return None

    # Colors
    domain = source['Name'].tolist()
    range_colors = ['#0070C0', '#2F5597', '#5978F7', '#BDD7EE',' #7F7F7F', '#D9D9D9']
    
    base = alt.Chart(source).encode(theta=alt.Theta("MarketValue", stack=True))

    # PIE & LEGEND
    pie = base.mark_arc(innerRadius=60, outerRadius=100).encode(
        color=alt.Color(
            "Name", 
            scale=alt.Scale(domain=domain, range=range_colors),
            # Legend Configuration
            legend=alt.Legend(
                title="Asset Class",
                orient="right") 
        ),
        order=alt.Order('MarketValue', sort="descending")
    )
    
    # LABELS
    text = base.mark_text(radius=120).encode(
        text=alt.Text('Allocation', format=".0%"), 
        order=alt.Order('MarketValue', sort="descending"),
        color=alt.value("black")
    )
    
    chart = (pie + text).properties(
        title=alt.TitleParams("Asset Allocation", fontSize=14, color='#404040'),
        width=300,  # Adjusted width for side-by-side
        height=200
    )

    chart_path = "temp_chart.png"
    chart.save(chart_path, scale_factor=3.0)
    return chart_path


# ==========================================
#  OVERALL PORTFOLIO REPORT
# ==========================================
def write_portfolio_report(summary_df, holdings_df, total_metrics, report_date, output_path, account_title="Total Portfolio"):
    print(f"   > Generating PDF Report: {output_path}")
    
    # 1. LANDSCAPE SETUP
    pdf = PortfolioPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # --- HEADER INFO ---
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 8, account_title, new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font('Helvetica', '', 6)
    pdf.set_text_color(*C_TEXT_GREY)
    pdf.cell(0, 6, f"Reportings as of {report_date}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # === TWO-COLUMN LAYOUT START ===
    # Save the Y position where the content starts
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
        row.cell(val_str, style=FontFace(size_pt=14, emphasis="BOLD", color=C_BLUE_DARK))
        row.cell(ret_str, style=FontFace(size_pt=14, emphasis="BOLD", color=(50, 150, 50)))

    pdf.ln(5)

    # 2. SUMMARY TABLE (Left Aligned)
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, "Asset Allocation", new_x="LMARGIN", new_y="NEXT")
    
    # Widths sum to ~130mm (approx half page)
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

    # --- RIGHT COLUMN: CHART ---
    # We move the cursor back to the top right
    # X = 150mm (Middle of page), Y = start_y (Top aligned with scorecard)
    try:
        chart_img = generate_donut_chart(summary_df)
        if chart_img:      
            y_offset = 81                    # Adjust for Vertical Alignment
            pdf.set_y(start_y + y_offset) 
            pdf.set_x(170)                    # Adjust for Horizontal Alignment
            pdf.image(chart_img, w=110) 
    except Exception as e:
        print(f"Chart Error: {e}")

    # === PAGE 2: DETAILED HOLDINGS ===
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
        
        # Wide Landscape Table
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