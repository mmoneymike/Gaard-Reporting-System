import pandas as pd
import datetime
import xlsxwriter

def write_portfolio_report_xlsx(
    summary_df: pd.DataFrame, 
    holdings_df: pd.DataFrame, 
    total_metrics: dict, 
    report_date: str,
    output_path: str,
    account_title: str = "Total Portfolio" # Default if not passed
):
    print(f"   > Writing Excel Report to: {output_path}")

    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        workbook = writer.book

        # ==========================================
        # 1. DEFINE COLORS & STATIC FORMATS
        # ==========================================
        c_gblue = '#5978F7'      
        c_benchmark_blue = '#0070C0' 
        c_header_bg = '#E4E5EB' 

        # Alternating Row Colors for Buckets
        bg_color_2 = '#FAFAFA'  # Lighter BG
        bg_color_1 = '#F0F0F0'  # Darker BG

        # Titles
        fmt_title = workbook.add_format({'bold':True, 'font_size':16, 'font_color':c_gblue})
        fmt_subtitle = workbook.add_format({'italic':False, 'font_color':'#000000','font_size':9, 'align':'right'})

        # Table Headers
        fmt_header_blank = workbook.add_format({'bold':True, 'text_wrap':True, 'valign':'vcenter'})
        fmt_header_mv = workbook.add_format({'bold':False, 'text_wrap': True, 'valign':'vcenter', 'align':'right', 'indent':1, 'fg_color':c_header_bg, 'right':1, 'right_color': '#FFFFFF'})
        fmt_header_rest = workbook.add_format({'bold':False, 'text_wrap': True, 'valign':'vcenter', 'align':'right', 'indent':1, 'fg_color':c_header_bg, 'right':1, 'right_color': '#000000'})

        # General Texts
        fmt_bold_text = workbook.add_format({'bold':True, 'border':1})
        
        # --- SCORECARD (TOTAL ROW) STYLES ---
        fmt_total_label = workbook.add_format({'bold':True, 'font_size':12, 'valign':'vcenter', 'align':'left', 'indent':1, 'top':1, 'bottom':1,})
        fmt_total_mv = workbook.add_format({'bold':True, 'font_size':12, 'valign':'vcenter', 'num_format':'$#,##0', 'align':'right', 'indent':1, 'top':1, 'bottom':1})
        fmt_total_alloc = workbook.add_format({'bold':True, 'font_size':12, 'valign':'vcenter', 'num_format':'0.00%', 'align':'right', 'indent':1, 'top':1, 'bottom':1, 'right':1})

        # --- DYNAMIC PORTFOLIO BUCKET STYLES ---
        # Helper: Create BUCKET styles for a specific background color
        def get_bucket_style(bg_color):
            return {
                'txt': workbook.add_format({'bold':False, 'bg_color':bg_color, 'align':'left', 'indent':1, 'top':1, 'border_color':'#FFFFFF'}),
                'mv': workbook.add_format({'bold':False, 'num_format':'$#,##0', 'bg_color':bg_color, 'align':'right', 'indent':1, 'top':1, 'right':1, 'left':1, 'border_color':'#FFFFFF'}),
                'alloc': workbook.add_format({'bold':False, 'num_format':'0.00%', 'bg_color':bg_color, 'align':'right', 'indent':1, 'top':1, 'top_color':'#FFFFFF', 'right':1, 'right_color':'#000000'}),
                'cash': workbook.add_format({'bold':False, 'bg_color':bg_color, 'align':'right', 'top':1, 'indent':1, 'top_color':'#FFFFFF', 'right':1, 'right_color':'#000000'})
            }
            
        # Helper: Create BENCHMARK styles for a specific background color
        # (This ensures the benchmark row matches the bucket row above it)
        def get_benchmark_style(bg_color):
            return {
                'txt': workbook.add_format({'font_color': c_benchmark_blue, 'italic':False, 'bg_color':bg_color, 'align':'left', 'indent':2}),
                'empty': workbook.add_format({'bg_color': bg_color,'right':1, 'left':1, 'border_color':'#FFFFFF'}),
                'pct': workbook.add_format({'font_color': c_benchmark_blue, 'italic':False, 'bg_color':bg_color, 'num_format':'0.00%', 'align':'right', 'indent':1, 'left':1, 'right':1})
            }

        # Set 1
        styles_1 = {
            'bucket': get_bucket_style(bg_color_1),
            'bench': get_benchmark_style(bg_color_1)
        }
        # Set 2
        styles_2 = {
            'bucket': get_bucket_style(bg_color_2),
            'bench': get_benchmark_style(bg_color_2)
        }

        # --- HOLDINGS SHEET STYLES ---
        fmt_h_header = workbook.add_format({'bold':True, 'bg_color':c_header_bg})
        fmt_h_txt = workbook.add_format({'border':1})
        fmt_h_mv = workbook.add_format({'num_format':'$#,##0', 'align':'right'})
        fmt_h_alloc = workbook.add_format({'num_format':'0.00%', 'align':'right'})
        fmt_h_center = workbook.add_format({'align':'center'})
        fmt_h_right = workbook.add_format({'align':'right'})

        # --- PIE CHART STYLES
        chart_colors = ['#5978F7', '#0070C0', '#7F7F7F', '#BDD7EE', '#2F5597', '#D9D9D9']
        
        # ==========================================
        # SHEET 1: DASHBOARD
        # ==========================================
        sheet = writer.book.add_worksheet('Dashboard')
        sheet.hide_gridlines(2) 

        # 1. Title
        sheet.write('B2', "Portfolio Performance", fmt_title)

        # 2. COLUMN HEADERS
        current_row = 3
        sheet.set_row(current_row, 22)
        headers = ['', 'Market Value', 'Allocation', 'Return']
        sheet.write(current_row, 1, headers[0], fmt_header_blank)
        sheet.write(current_row, 2, headers[1], fmt_header_mv)
        for i, h in enumerate(headers[2:], start=2):
            sheet.write(current_row, 1 + i, h, fmt_header_rest)

        # 3. SCORECARD
        current_row+=1
        sheet.set_row(current_row, 19)
        sheet.write(current_row, 1, account_title, fmt_total_label)
        sheet.write(current_row, 2, total_metrics['value'], fmt_total_mv)
        sheet.write(current_row, 3, 1.0, fmt_total_alloc) # 100%
        sheet.write(current_row, 4, total_metrics['return'], fmt_total_alloc)
        
        # 4. DATA ROWS (Start after Scorecard)
        current_row+=1 
        
        # Color Toggle Logic
        toggle = 0
        current_styles = styles_1

        for i, row in summary_df.iterrows():
            row_type = row['Type']
            name = row['Name']
            sheet.set_row(current_row, 17)
            
             # COLOR LOGIC
             # Only toggle color when we hit a NEW BUCKET (Asset Class).
            if row_type == 'Bucket':
                # Switch Color Logic
                toggle = 1 - toggle
                current_styles = styles_1 if toggle == 0 else styles_2
                
                # Portfolio Bucket
                fmts = current_styles['bucket']
                sheet.write(current_row, 1, name, fmts['txt'])
                sheet.write(current_row, 2, row['MarketValue'], fmts['mv'])
                sheet.write(current_row, 3, row['Allocation'], fmts['alloc'])
                
                if row.get('IsCash', False):
                    sheet.write(current_row, 4, "---", fmts['cash'])
                else:
                    sheet.write(current_row, 4, row['Return'], fmts['alloc'])

            elif row_type == 'Benchmark':
                # Use Benchmark formats from current set (Matches Bucket BG)
                fmts = current_styles['bench']
                sheet.write(current_row, 1, name, fmts['txt'])
                sheet.write(current_row, 2, "", fmts['empty']) 
                sheet.write(current_row, 3, "", fmts['empty']) 
                sheet.write(current_row, 4, row['Return'], fmts['pct'])
            
            current_row += 1

        # current_row+=1
        sheet.write(current_row, 4, f"Date as of: {report_date}", fmt_subtitle)
                
        sheet.set_column('B:B', 35)
        sheet.set_column('C:C', 18)
        sheet.set_column('D:D', 12)
        sheet.set_column('E:E', 12)

        # =========================
        # PIE CHART GENERATION
        # =========================       
        # A. Filter Data: Buckets only, EXCLUDING 'Other' (Accruals)
        # We use strict filtering to ensure the chart is clean
        buckets_only = summary_df[
            (summary_df['Type'] == 'Bucket') & 
            (summary_df['Name'] != 'Other')
        ].copy()
        
        # Optional: Sort largest to smallest for a better looking Pie Chart
        buckets_only = buckets_only.sort_values(by='MarketValue', ascending=False)
        
        # SAFETY: Only create chart if data exists
        if not buckets_only.empty:
            
            # Force strict Python Types (str, float) to prevent Excel Errors
            chart_names = [str(x) for x in buckets_only['Name'].tolist()]
            chart_values = [float(x) for x in buckets_only['MarketValue'].fillna(0.0).tolist()]
            
            end_row = len(chart_names)

            # B. Write Clean Data to Hidden Sheet
            # Using a hidden sheet is much more stable than embedding data directly
            chart_sheet = workbook.add_worksheet('_ChartData')
            chart_sheet.hide()
            
            chart_sheet.write_column('A1', chart_names)
            chart_sheet.write_column('B1', chart_values)
            
            # C. Create Chart (Changed to 'pie')
            pie_chart = workbook.add_chart({'type': 'donut'})
            
            points_list = []
            for i in range(end_row):
                color = chart_colors[i % len(chart_colors)] 
                points_list.append({'fill': {'color': color}})

            # D. Configure Series
            pie_chart.add_series({
                'name':       'Asset Allocation',
                'categories': ['_ChartData', 0, 0, end_row - 1, 0], # A1:A_N
                'values':     ['_ChartData', 0, 1, end_row - 1, 1], # B1:B_N
                'data_labels': {
                    'percentage': True, 
                    'position': 'center', # 'center' or 'inside_end' works best for Pie
                    'font': {'bold': True, 'color': '#FFFFFF'}
                },
                'points': points_list, 
            })
            
            # E. Style & Insert
            pie_chart.set_title({
                'name': 'Asset Allocation', 
                'name_font': {'size': 14, 'bold': True, 'color': '#404040'}
            })
            
            # Note: set_hole_size is removed because it's a Pie chart now
            pie_chart.set_style(10) 
            pie_chart.set_size({'width': 350, 'height': 250})
            pie_chart.set_hole_size(50)
            
            # Insert the chart into the main sheet
            sheet.insert_chart('G3', pie_chart)
        
        # ==========================================
        # SHEET 2: DETAILED HOLDINGS
        # ==========================================
        details_sheet = writer.book.add_worksheet('Holdings')
        writer.sheets['Holdings'] = details_sheet
        details_sheet.hide_gridlines(2) 
        
        details_sheet.write('A1', "Detailed Holdings Breakdown", fmt_title)
        details_sheet.write('A2', f"Generated on {datetime.date.today()}", fmt_subtitle)

        bucket_order = summary_df[summary_df['Type'] == 'Bucket']['Name'].unique().tolist()
        sorter_map = {name: i for i, name in enumerate(bucket_order)}
        
        holdings_df['sort_key'] = holdings_df['asset_class'].map(sorter_map).fillna(999)
        sorted_holdings = holdings_df.sort_values(['sort_key', 'weight'], ascending=[True, False])
        
        current_row = 4
        cols = ['Ticker', 'Name', 'Cost Basis', 'Market Value', 'Allocation', 'Return']

        unique_buckets = sorted_holdings['asset_class'].unique()

        for bucket in unique_buckets:
            # Section Header
            details_sheet.merge_range(current_row, 0, current_row, 5, bucket.upper(), fmt_h_header)
            current_row += 1
            
            # Column Headers
            for col_idx, col_name in enumerate(cols):
                details_sheet.write(current_row, col_idx, col_name, fmt_bold_text)
            current_row += 1
            
            # Data
            subset = sorted_holdings[sorted_holdings['asset_class'] == bucket]
            
            for _, pos in subset.iterrows():
                details_sheet.write(current_row, 0, pos['ticker'], fmt_h_txt)
                details_sheet.write(current_row, 1, pos.get('official_name', ''), fmt_h_txt)
                details_sheet.write(current_row, 2, pos['avg_cost'], fmt_h_mv)
                details_sheet.write(current_row, 3, pos['raw_value'], fmt_h_mv)
                details_sheet.write(current_row, 4, pos['weight'], fmt_h_alloc)
                
                if pos['ticker'] == 'CASH_BAL':
                    details_sheet.write(current_row, 5, "---", fmt_h_right)
                else:
                    details_sheet.write(current_row, 5, pos['cumulative_return'], fmt_h_alloc)
                
                current_row += 1
            
            current_row += 2

        details_sheet.set_column('A:A', 10)
        details_sheet.set_column('B:B', 45)
        details_sheet.set_column('C:D', 18)
        details_sheet.set_column('E:F', 12)

    print(f"   > SUCCESS: Report saved.")