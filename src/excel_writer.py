import pandas as pd
import datetime

def write_portfolio_report(
    summary_df: pd.DataFrame, 
    holdings_df: pd.DataFrame, 
    total_metrics: dict, 
    report_date: str,
    output_path: str
):
    """
    Generates a polished Excel report with two sheets: 'Dashboard' and 'Holdings'.
    """
    # Create the Excel Writer object
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    workbook = writer.book

    # --- DEFINE FORMATS ---
    # We define specific styles for headers, money, percents, etc.
    fmt_header = workbook.add_format({
        'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1
    })
    fmt_currency = workbook.add_format({'num_format': '$#,##0', 'border': 1})
    fmt_percent = workbook.add_format({'num_format': '0.00%', 'border': 1})
    fmt_text = workbook.add_format({'border': 1})
    fmt_bold_text = workbook.add_format({'bold': True, 'border': 1})
    
    # Title Formats
    fmt_title = workbook.add_format({'bold': True, 'font_size': 14})
    fmt_subtitle = workbook.add_format({'italic': True, 'font_color': 'gray'})

    # ==========================================
    # SHEET 1: DASHBOARD
    # ==========================================
    summary_sheet = workbook.add_worksheet('Dashboard')
    summary_sheet.hide_gridlines(2) # Hide screen gridlines

    # 1. Write Titles & Top-Level Metrics
    summary_sheet.write('B2', f"Portfolio Performance Report", fmt_title)
    summary_sheet.write('B3', f"As of: {report_date}", fmt_subtitle)
    
    # Write Total Return Box
    summary_sheet.write('B5', "Total Portfolio Return:", fmt_bold_text)
    summary_sheet.write('C5', total_metrics['return'], fmt_percent)
    summary_sheet.write('B6', "Total Value:", fmt_bold_text)
    summary_sheet.write('C6', total_metrics['value'], fmt_currency)
    
    # 2. Write the Summary Table (Asset Class vs Benchmarks)
    # Define columns for the summary table
    headers = ['Asset Class', 'Type', 'Return']
    start_row = 9
    start_col = 1 # Column B

    # Write Headers
    for i, h in enumerate(headers):
        summary_sheet.write(start_row, start_col + i, h, fmt_header)

    # Write Rows
    for i, row in summary_df.iterrows():
        curr_row = start_row + 1 + i
        
        # Asset Class Name
        summary_sheet.write(curr_row, start_col, row['Asset Class'], fmt_text)
        
        # Type (Portfolio vs Benchmark)
        summary_sheet.write(curr_row, start_col + 1, row['Type'], fmt_text)
        
        # Return Column (Handle Strings like '$37k' vs Floats like 0.05)
        val = row['Return']
        if isinstance(val, (int, float)):
            summary_sheet.write(curr_row, start_col + 2, val, fmt_percent)
        else:
            summary_sheet.write(curr_row, start_col + 2, val, fmt_text)

    # Auto-adjust column widths
    summary_sheet.set_column('B:B', 25) # Asset Class
    summary_sheet.set_column('C:C', 15) # Type
    summary_sheet.set_column('D:D', 15) # Return

    # ==========================================
    # SHEET 2: DETAILED HOLDINGS
    # ==========================================
    details_sheet = workbook.add_worksheet('Holdings')
    
    details_sheet.write('A1', "Detailed Holdings Breakdown", fmt_title)
    details_sheet.write('A2', f"Generated on {datetime.date.today()}", fmt_subtitle)

    # We will loop through Asset Classes and write separate blocks
    # Logic: Filter DF -> Write Block -> Move Down
    
    current_row = 4
    cols = ['Ticker', 'Name', 'Cost Basis', 'Market Value', 'Weight', 'Return']
    
    # Get unique buckets in the desired order (if possible) from the DF
    # We assume the DF passed in is already sorted or we just take unique buckets
    buckets = holdings_df['asset_class'].unique()
    
    # Simple order enforcement if available
    ordered_buckets = [b for b in ['U.S. Equities', 'International Equities', 'Fixed Income', 'Alternative Assets', 'Cash'] if b in buckets]
    # Add leftovers
    for b in buckets:
        if b not in ordered_buckets: ordered_buckets.append(b)

    for bucket in ordered_buckets:
        # Write Section Header (e.g., "U.S. Equities")
        details_sheet.merge_range(current_row, 0, current_row, 5, bucket.upper(), fmt_header)
        current_row += 1
        
        # Write Column Headers
        for col_num, col_name in enumerate(cols):
            details_sheet.write(current_row, col_num, col_name, fmt_bold_text)
        current_row += 1
        
        # Filter Data
        subset = holdings_df[holdings_df['asset_class'] == bucket].copy()
        subset = subset.sort_values('weight', ascending=False)
        
        for _, pos in subset.iterrows():
            # Ticker
            details_sheet.write(current_row, 0, pos['ticker'], fmt_text)
            # Name
            details_sheet.write(current_row, 1, pos.get('official_name', ''), fmt_text)
            # Cost Basis
            details_sheet.write(current_row, 2, pos['avg_cost'], fmt_currency)
            # Market Value
            details_sheet.write(current_row, 3, pos['raw_value'], fmt_currency)
            # Weight
            details_sheet.write(current_row, 4, pos['weight'], fmt_percent)
            # Return (Handle CASH_BAL suppression)
            if pos['ticker'] == 'CASH_BAL':
                details_sheet.write(current_row, 5, "---", fmt_text)
            else:
                details_sheet.write(current_row, 5, pos['cumulative_return'], fmt_percent)
            
            current_row += 1
        
        # Add spacing between blocks
        current_row += 2

    # Auto-adjust columns for Holdings
    details_sheet.set_column('A:A', 10) # Ticker
    details_sheet.set_column('B:B', 40) # Name
    details_sheet.set_column('C:D', 18) # Value Cols
    details_sheet.set_column('E:F', 12) # % Cols

    # Save
    writer.close()
    print(f"Excel report saved successfully to: {output_path}")