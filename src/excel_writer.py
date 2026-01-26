import pandas as pd
import datetime
import xlsxwriter

def write_portfolio_report(
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

        # General TextsS
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