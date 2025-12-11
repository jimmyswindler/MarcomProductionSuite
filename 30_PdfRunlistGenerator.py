# 30_PdfRunlistGenerator.py
import pandas as pd
import os
import yaml
import sys
import traceback # For error details
import time # For performance timing
import json
import argparse

# --- PDF Generation Libraries ---
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import ELEVENSEVENTEEN
    from reportlab.lib.units import inch
    from reportlab.lib.utils import simpleSplit # For text wrapping
    
    # --- NEW: Font Registration Imports ---
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
except ImportError:
    print("FATAL ERROR: Required library not found. Please install 'reportlab' by running:")
    print("pip install reportlab")
    sys.exit(1)

# =========================================================
# GLOBAL FONT CONSTANTS (Will be overridden by registration)
# =========================================================
CUSTOM_FONT_REGULAR = "Calibri-Light"
CUSTOM_FONT_BOLD = "Calibri-Bold"

# =========================================================
# FONT REGISTRATION FUNCTION
# =========================================================
def register_custom_fonts(config):
    """
    Registers custom TTF fonts using paths from the central config.
    Returns: True if registration was successful, False otherwise.
    """
    global CUSTOM_FONT_REGULAR, CUSTOM_FONT_BOLD

    paths = config.get('paths', {})
    light_path = paths.get('calibri_light_font_path')
    bold_path = paths.get('calibri_bold_font_path')

    if not light_path or not bold_path:
        print("⚠️ FONT REGISTRATION SKIPPED: Font paths missing in config.yaml.")
        CUSTOM_FONT_REGULAR = "Helvetica"
        CUSTOM_FONT_BOLD = "Helvetica-Bold"
        return False
        
    try:
        if not os.path.exists(light_path) or not os.path.exists(bold_path):
            raise FileNotFoundError(f"Calibri TTF files not found at specified paths: {light_path} or {bold_path}")

        # Register the fonts with the actual file paths
        pdfmetrics.registerFont(TTFont(CUSTOM_FONT_REGULAR, light_path))
        pdfmetrics.registerFont(TTFont(CUSTOM_FONT_BOLD, bold_path))
        print(f"✓ Successfully registered custom fonts from config: {CUSTOM_FONT_REGULAR} and {CUSTOM_FONT_BOLD}.")
        return True

    except FileNotFoundError as e:
        print(f"⚠️ FONT REGISTRATION FAILED: {e}")
        print("   Falling back to standard ReportLab fonts: Helvetica and Helvetica-Bold.")
        CUSTOM_FONT_REGULAR = "Helvetica"
        CUSTOM_FONT_BOLD = "Helvetica-Bold"
        return False
    except Exception as e:
        print(f"⚠️ FONT REGISTRATION FAILED (General Error): {e}")
        print("   Falling back to standard ReportLab fonts: Helvetica and Helvetica-Bold.")
        CUSTOM_FONT_REGULAR = "Helvetica"
        CUSTOM_FONT_BOLD = "Helvetica-Bold"
        return False


# =========================================================
# RUN HISTORY FUNCTIONS
# =========================================================
def load_run_history(history_path="run_history.yaml"):
    if not os.path.exists(history_path):
        print(f"INFO: Run history file not found. Creating a default file.")
        default_history = {'monthly_pace_job_number': 100000, 'last_used_gang_run_suffix': 0}
        with open(history_path, 'w') as f: yaml.dump(default_history, f)
        print("✓ Default history file created. Please review 'run_history.yaml'.")
        return default_history
    try:
        with open(history_path, 'r') as f: return yaml.safe_load(f)
    except Exception as e:
        print(f"FATAL ERROR: Could not read or parse run history file: {e}"); sys.exit(1)

# =========================================================
# PDF GENERATION
# =========================================================

def generate_pdf_run_list(excel_path, pdf_path, config, history, fragmentation_map=None):
    """
    Generates PDF run list with the new custom fonts.
    """
    
    print("\n--- Generating PDF Run List (Layout v12.4 - Fixes Applied) ---")
    
    if fragmentation_map is None:
        fragmentation_map = {'store_report_map': {}, 'unclaimed_report_map': {}}
    store_report_map = fragmentation_map.get('store_report_map', {})
    unclaimed_report_map = fragmentation_map.get('unclaimed_report_map', {})

    c = canvas.Canvas(pdf_path, pagesize=ELEVENSEVENTEEN); width, height = ELEVENSEVENTEEN
    margin = 0.375 * inch; frame_padding = 0.05 * inch; printable_width = width - 2 * margin; header_padding = 0.05 * inch
    store_padding = 5
    col_names = config.get('column_names', {});
    col_order_num = col_names.get('order_number');
    col_base_job = 'Base Job Ticket Number'
    col_cost_center = col_names.get('cost_center')
    col_prod_id = col_names.get('product_id')

    default_widths = {
        'Job\nNumber': 1.*inch,
        'Order\nNumber': 1.125*inch,
        'Store\nNumber': 0.75*inch,
        'Product\nID': 0.75*inch,
        'Qty': 0.625*inch,
        'SKU': 2.625*inch,
        'Product\nDescription': 2.625*inch
    }
    
    sku_col = 'SKU'; desc_col = 'Product\nDescription'; sku_source = col_names.get('sku'); desc_source = col_names.get('product_description')
    
    fixed_cols_width = default_widths['Job\nNumber'] + default_widths['Order\nNumber'] + default_widths['Store\nNumber'] + default_widths['Product\nID'] + default_widths['Qty']
    available_table_width = (printable_width - (2 * frame_padding) - (2 * store_padding)) - fixed_cols_width
    
    dynamic_cols_default_width = default_widths[sku_col] + default_widths[desc_col]

    if dynamic_cols_default_width > available_table_width:
        print("  - INFO: Adjusting PDF SKU/Desc widths.");
        excess = dynamic_cols_default_width - available_table_width; min_width = 1.5 * inch
        current_sku_w = default_widths[sku_col]; current_desc_w = default_widths[desc_col]
        total_adj_width = current_sku_w + current_desc_w
        
        if total_adj_width > 0:
            sku_prop = current_sku_w / total_adj_width
            desc_prop = current_desc_w / total_adj_width
            new_sku_w = max(min_width, current_sku_w - excess * sku_prop)
            new_desc_w = max(min_width, current_desc_w - excess * desc_prop)
        else:
            new_sku_w, new_desc_w = min_width, min_width
            
        default_widths[sku_col] = new_sku_w
        default_widths[desc_col] = new_desc_w

    PDF_COLS = {
        'Job\nNumber': {'source': col_names.get('job_ticket_number'), 'width': default_widths['Job\nNumber'], 'align': 'left'},
        'Qty': {'source': col_names.get('quantity_ordered'), 'width': default_widths['Qty'], 'align': 'center'},
        'Order\nNumber': {'source': col_order_num, 'width': default_widths['Order\nNumber'], 'align': 'center'},
        'Store\nNumber': {'source': col_cost_center, 'width': default_widths['Store\nNumber'], 'align': 'center'},
        'Product\nID': {'source': col_prod_id, 'width': default_widths['Product\nID'], 'align': 'center'},
        'SKU': {'source': sku_source, 'width': default_widths[sku_col], 'align': 'left'},
        'Product\nDescription': {'source': desc_source, 'width': default_widths[desc_col], 'align': 'left'}
    }
    if not all(v['source'] for v in PDF_COLS.values()): print("⚠️ WARNING: PDF column sources missing.")

    # --- NEW v11: Helper function for building fragmentation lines ---
    def _build_fragmentation_lines(entities_to_report, store_report_map, sheet_name):
        messages_to_build = set()
        
        def get_store_num(store_id_str):
            return str(store_id_str).split('-', 1)[0].strip()

        def format_dests(dests_list, current_sheet):
            other_dests = sorted([
                str(d) for d in dests_list
                if d != current_sheet
            ])
            dests_str = ", ".join(other_dests)
            return dests_str
            
        for (store_id, order_id, job_id) in entities_to_report:
            
            map_store = store_report_map.get(store_id)
            if not map_store or not map_store.get('is_fragmented'):
                continue

            store_num_display = get_store_num(store_id)
            context_parts = []
            
            map_order = map_store.get('fragmented_orders', {}).get(order_id)
            if map_order and map_order.get('is_fragmented'):
                context_parts.append(f"Order {order_id}")
                
                map_job = map_order.get('fragmented_jobs', {}).get(job_id)
                if map_job and map_job.get('is_fragmentED'):
                    context_parts.append(f"JOB {job_id}")
            
            dests_display = format_dests(map_store.get('destinations', []), sheet_name)
            if not dests_display:
                continue

            if context_parts:
                context_str = ", ".join(context_parts)
                msg = f"Store {store_num_display} ({context_str}) has other content appearing in: {dests_display}"
            else:
                msg = f"Store {store_num_display} has other content appearing in: {dests_display}"
            
            messages_to_build.add(msg)
            
        return sorted(list(messages_to_build))

    # --- NEW v11: Helper function for drawing message blocks ---
    def draw_message_block(c, lines_to_draw, current_y, font_size, line_height, start_x, drawable_width, page_bottom_y, new_page_callback):
        """
        Draws a block of messages, handling page breaks as needed.
        Returns: (new_y_pos, did_page_break_flag)
        """
        if not lines_to_draw:
            return current_y, False
        
        did_page_break = False
        
        current_y -= 5
        
        is_first_message = True
        
        for line in lines_to_draw:
            if current_y - line_height < page_bottom_y:
                current_y, did_page_break = new_page_callback()
            
            # --- START: FONT MODIFICATION ---
            c.setFont(CUSTOM_FONT_BOLD, font_size)
            # --- END: FONT MODIFICATION ---
            c.setFillColor(colors.black)
            
            # Use the registered font name for splitting
            wrapped_lines = simpleSplit(line, CUSTOM_FONT_BOLD, font_size, drawable_width)
            
            for w_line in wrapped_lines:
                if current_y - line_height < page_bottom_y:
                    current_y, did_page_break = new_page_callback()
                    # Re-set font after page break
                    # --- START: FONT MODIFICATION ---
                    c.setFont(CUSTOM_FONT_BOLD, font_size)
                    # --- END: FONT MODIFICATION ---
                    c.setFillColor(colors.black)
                    
                c.drawString(start_x, current_y - (font_size * 0.9), w_line)
                current_y -= line_height
        
            is_first_message = False
        
        return current_y, did_page_break

    try:
        if not os.path.exists(excel_path): print(f"⚠️ PDF Error: Excel file not found: {excel_path}"); return False
        xls = pd.ExcelFile(excel_path)
        
        for sheet_name in xls.sheet_names:
            try: df_sheet = xls.parse(sheet_name)
            except Exception as e: print(f"⚠️ PDF Warning: Cannot parse sheet '{sheet_name}'. {e}"); continue
            if df_sheet.empty: continue

            is_bundle_sheet_for_warnings = True
            
            # --- Get Dates ---
            order_date_col = col_names.get('order_date'); ship_date_col = col_names.get('ship_date'); earliest_order_date, earliest_ship_date = pd.NaT, pd.NaT
            if order_date_col and order_date_col in df_sheet.columns: df_sheet[order_date_col] = pd.to_datetime(df_sheet[order_date_col], errors='coerce'); earliest_order_date = df_sheet[order_date_col].min()
            if ship_date_col and ship_date_col in df_sheet.columns: df_sheet[ship_date_col] = pd.to_datetime(df_sheet[ship_date_col], errors='coerce'); earliest_ship_date = df_sheet[ship_date_col].min()
            order_date_str = earliest_order_date.strftime('%a %m/%d/%Y') if pd.notna(earliest_order_date) else "N/A"
            ship_date_str = earliest_ship_date.strftime('%a %m/%d/%Y') if pd.notna(earliest_ship_date) else "N/A"

            # --- Define Layout Metrics ---
            header_height, footer_height, row_height, table_header_height = 1.25*inch, 0.5*inch, 0.22*inch, 0.5*inch
            manifest_line_height_factor = 1.2
            
            header_gap = 0.22 * inch
            job_gap = 0.22 * inch
            order_gap = 0.22 * inch
            store_gap = 0.22 * inch
            
            order_box_line_width = 2.0; frag_font_size = 9; text_cell_padding = 5
            page_bottom_margin_y = margin + footer_height
            
            frag_msg_font_size = 14
            frag_msg_line_height = frag_msg_font_size * 1.3
            frag_msg_start_x = margin + frame_padding + 5
            frag_msg_drawable_width = printable_width - (2 * frame_padding) - 10
            
            page_bottom_content_area_y = page_bottom_margin_y + (0.5 * inch)
            
            if col_cost_center not in df_sheet.columns: print(f"⚠️ PDF Err: Store/CostCenter col '{col_cost_center}' missing."); continue
            if col_order_num not in df_sheet.columns: print(f"⚠️ PDF Err: Order# col '{col_order_num}' missing."); continue
            if col_base_job not in df_sheet.columns: print(f"⚠️ PDF Err: BaseJob col '{col_base_job}' missing."); continue
            
            try:
                df_sheet[col_cost_center] = df_sheet[col_cost_center].astype(str).fillna('N/A')
                df_sheet[col_order_num] = df_sheet[col_order_num].astype(str).fillna('N/A')
                df_sheet[col_base_job] = df_sheet[col_base_job].astype(str).fillna('N/A')
                
                df_sheet.sort_values(by=[col_base_job, col_cost_center, col_order_num], inplace=True)
                df_sheet.reset_index(drop=True, inplace=True)
            except Exception as e:
                print(f"⚠️ PDF Err: Could not sort sheet '{sheet_name}'. {e}"); continue
                
            # --- CRITICAL FIX: Initialize pagination state variables ---
            current_row_index = 0
            page_num = 0
            is_continuing_store_box = False
            
            # --- Cross-page state (the "State Machine") ---
            current_store, current_order, current_job = None, None, None
            entities_in_current_store_box = set() # Holds (store, order, job)
            store_start_y = None # Y-coord of the top of the current store box
            # --- END CRITICAL FIX ---


            # --- NEW v11: Helper function for drawing headers on a new page ---
            def draw_new_page_headers(page_num):
                # --- Draw Page Header ---
                # --- START: FONT MODIFICATION ---
                label_font, label_size = CUSTOM_FONT_BOLD, 14
                value_font, value_size = CUSTOM_FONT_BOLD, 22
                # --- END: FONT MODIFICATION ---
                y_label = height - margin - 0.5 * inch; y_value = y_label - 30
                
                c.setFont(value_font, value_size); max_sheet_name_width = (width / 3) - (2 * header_padding); sheet_name_display = sheet_name
                try:
                    # Use the registered font name for width calculation
                    while c.stringWidth(sheet_name_display, value_font, value_size) > max_sheet_name_width and len(sheet_name_display) > 5:
                        sheet_name_display = sheet_name_display[:-4] + "..."
                except Exception:
                    sheet_name_display = sheet_name[:15] + "..." if len(sheet_name) > 15 else sheet_name
                
                sheet_name_width = c.stringWidth(sheet_name_display, value_font, value_size); left_x_start = margin + header_padding; c.drawString(left_x_start, y_value, sheet_name_display)
                
                c.setFont(label_font, label_size); job_num_center_x = left_x_start + (sheet_name_width / 2); c.drawCentredString(job_num_center_x, y_label, str(history.get('monthly_pace_job_number', 'N/A')))
                
                c.setFont(value_font, value_size); ship_date_width = c.stringWidth(ship_date_str, value_font, value_size); right_x_end = width - margin - header_padding; c.drawRightString(right_x_end, y_value, ship_date_str)
                
                c.setFont(label_font, label_size); ship_date_label_center_x = right_x_end - (ship_date_width / 2); c.drawCentredString(ship_date_label_center_x, y_label, "Ship Date:")
                
                left_boundary = left_x_start + sheet_name_width; right_boundary = right_x_end - ship_date_width; order_date_center_x = (left_boundary + right_boundary) / 2
                
                c.setFont(label_font, label_size); c.drawCentredString(order_date_center_x, y_label, "Order Date:")
                c.setFont(value_font, value_size); c.drawCentredString(order_date_center_x, y_value, order_date_str)
                
                c.setLineWidth(0.5)
                c.rect(margin, margin, printable_width, height - (2*margin))
                
                # --- End Header ---

                y_pos = height - margin - header_height; x_pos = margin + frame_padding + store_padding
                
                # --- Draw Table Header ---
                font_size_header = 11
                # --- START: FONT MODIFICATION ---
                c.setFont(CUSTOM_FONT_BOLD, font_size_header)
                # --- END: FONT MODIFICATION ---
                header_line_height = font_size_header * 1.3
                header_v_center = y_pos - (table_header_height / 2)
                
                c.setLineWidth(0.5); c.rect(margin + frame_padding, y_pos - table_header_height, printable_width - (2 * frame_padding), table_header_height)
                
                page_qty_total_x, page_qty_total_y = None, None
                
                for pdf_col_name, col_props in PDF_COLS.items():
                    col_width = col_props.get('width', 1*inch); align = col_props.get('align', 'left')
                    if x_pos > margin + frame_padding: c.line(x_pos, y_pos, x_pos, y_pos - table_header_height)
                    
                    header_lines = pdf_col_name.split('\n')
                    h_center = x_pos + (col_width / 2); h_left = x_pos + text_cell_padding

                    if pdf_col_name == 'Qty':
                        y1 = header_v_center + (header_line_height / 2); c.drawCentredString(h_center, y1, "Qty")
                        y2 = header_v_center - (header_line_height / 2)
                        page_qty_total_x = h_center; page_qty_total_y = y2
                    elif len(header_lines) == 2:
                        y1 = header_v_center + (header_line_height / 2); y2 = header_v_center - (header_line_height / 2)
                        if align == 'center':
                            c.drawCentredString(h_center, y1, header_lines[0]); c.drawCentredString(h_center, y2, header_lines[1])
                        else:
                            c.drawString(h_left, y1, header_lines[0]); c.drawString(h_left, y2, header_lines[1])
                    else:
                        y1 = header_v_center - (font_size_header / 2.5)
                        if align == 'center': c.drawCentredString(h_center, y1, header_lines[0])
                        else: c.drawString(h_left, y1, header_lines[0])
                    x_pos += col_width
                
                y_pos -= table_header_height
                return y_pos, page_qty_total_x, page_qty_total_y

            # --- Page callback for message drawer ---
            def trigger_page_break_for_messages():
                nonlocal y_pos, page_qty_total_x, page_qty_total_y, page_total_qty
                
                if page_qty_total_x is not None:
                    # --- START: FONT MODIFICATION ---
                    c.setFont(CUSTOM_FONT_BOLD, 11)
                    # --- END: FONT MODIFICATION ---
                    c.drawCentredString(page_qty_total_x, page_qty_total_y, str(int(page_total_qty)))
                    
                c.showPage()
                nonlocal page_num
                page_num += 1
                y_pos, page_qty_total_x, page_qty_total_y = draw_new_page_headers(page_num)
                
                page_total_qty = 0
                
                return y_pos, True

            while current_row_index < len(df_sheet):
                page_num += 1
                
                final_consolidated_lines = []
                entities_on_page = set()
                
                # --- Draw Page Headers ---
                y_pos, page_qty_total_x, page_qty_total_y = draw_new_page_headers(page_num)
                
                page_total_qty = 0
                
                # --- Add Header Gap ---
                y_pos -= header_gap
                
                row_line_start_x = margin + frame_padding + store_padding
                row_line_end_x = width - margin - frame_padding - store_padding
                
                store_start_y_on_page = y_pos
                if is_continuing_store_box:
                    store_start_y = y_pos
                    y_pos -= store_padding
                    is_continuing_store_box = False
                
                font_size_row = 10
                page_has_ended = False

                # --- Draw Store Blocks (Row-by-Row) ---
                for index in range(current_row_index, len(df_sheet)):
                    row = df_sheet.iloc[index]
                    
                    row_store = row[col_cost_center]
                    row_order = row[col_order_num]
                    row_job = row[col_base_job]
                    
                    if current_store is None:
                        current_store, current_order, current_job = row_store, row_order, row_job
                        store_start_y = y_pos
                        y_pos -= store_padding
                        
                    # =========================================================
                    # --- 1. Check for State Change (Store, Order, Job) ---
                    # =========================================================
                    
                    # --- A. Check for NEW STORE ---
                    if row_store != current_store:
                        # --- Close the PREVIOUS store box ---
                        c.setLineWidth(order_box_line_width)
                        y_pos -= store_padding
                        c.rect(margin + frame_padding, y_pos, printable_width - (2 * frame_padding), store_start_y - y_pos, stroke=1, fill=0)
                        
                        # --- Build and Draw Frag Messages for PREVIOUS store ---
                        lines_to_draw = _build_fragmentation_lines(entities_in_current_store_box, store_report_map, sheet_name)
                        
                        y_pos, did_break = draw_message_block(c, lines_to_draw, y_pos,
                                                              frag_msg_font_size, frag_msg_line_height,
                                                              frag_msg_start_x, frag_msg_drawable_width,
                                                              page_bottom_margin_y,
                                                              trigger_page_break_for_messages)
                        
                        # --- Draw LARGE STORE GAP ---
                        if y_pos - store_gap < page_bottom_content_area_y:
                            current_row_index = index
                            page_has_ended = True
                            is_continuing_store_box = False
                            break
                        
                        c.setLineWidth(0.5)
                        c.line(row_line_start_x, y_pos, row_line_end_x, y_pos)
                        y_pos -= store_gap
                        
                        # --- Update state for NEW store ---
                        current_store, current_order, current_job = row_store, row_order, row_job
                        store_start_y = y_pos
                        y_pos -= store_padding
                        entities_in_current_store_box.clear()

                    # --- B. Check for NEW ORDER (within same store) ---
                    elif row_order != current_order:
                        if y_pos - order_gap < page_bottom_content_area_y:
                            current_row_index = index
                            page_has_ended = True
                            is_continuing_store_box = True
                            break
                            
                        c.setLineWidth(0.5)
                        c.line(row_line_start_x, y_pos, row_line_end_x, y_pos)
                        y_pos -= order_gap
                        
                        # --- Update state for NEW order ---
                        current_order, current_job = row_order, row_job
                    
                    # --- C. Check for NEW JOB (within same order) ---
                    elif row_job != current_job:
                        if y_pos - job_gap < page_bottom_content_area_y:
                            current_row_index = index
                            page_has_ended = True
                            is_continuing_store_box = True
                            break
                        
                        c.setLineWidth(0.5)
                        c.line(row_line_start_x, y_pos, row_line_end_x, y_pos)
                        y_pos -= job_gap
                        
                        # --- Update state for NEW job ---
                        current_job = row_job

                    # =========================================================
                    # --- 2. Check for Row Page Break ---
                    # =========================================================
                    if y_pos - row_height < page_bottom_content_area_y:
                        current_row_index = index
                        page_has_ended = True
                        is_continuing_store_box = True
                        break
                    
                    # =========================================================
                    # --- 3. Draw The Row ---
                    # =========================================================
                    x_pos = margin + frame_padding + store_padding
                    
                    c.line(row_line_start_x, y_pos, row_line_end_x, y_pos)
                    
                    row_text_y = y_pos - (row_height / 2) - (font_size_row / 2.5)
                    c.setLineWidth(0.5)
                    
                    try:
                        store_val = row.get(col_cost_center, "N/A")
                        order_val = row.get(col_order_num, "N/A")
                        job_val = row.get(col_base_job, "N/A")
                        if pd.notna(store_val) and pd.notna(order_val) and pd.notna(job_val):
                            entities_in_current_store_box.add((str(store_val), str(order_val), str(job_val)))
                    except Exception as e:
                        print(f"Warn: Could not log entity for fragmap: {e}")

                    for pdf_col_name, col_props in PDF_COLS.items():
                        if x_pos > margin + frame_padding: c.line(x_pos, y_pos, x_pos, y_pos - row_height)
                        
                        source_col = col_props.get('source'); raw_value = row.get(source_col, "");
                        raw_text = str(raw_value) if not pd.isna(raw_value) else ""
                        
                        raw_text = str(raw_value) if not pd.isna(raw_value) else ""
                        if raw_text.strip().lower() == 'nan':
                            raw_text = ""
                            
                        if pdf_col_name == 'Store\nNumber':
                            raw_text = raw_text.split('-', 1)[0].strip()
                        
                        if pdf_col_name == 'Qty':
                            qty_val = pd.to_numeric(raw_text, errors='coerce')
                            if pd.notna(qty_val):
                                page_total_qty += qty_val

                        col_width = col_props.get('width', 1*inch); align = col_props.get('align', 'left'); allowed_width = col_width - (2 * text_cell_padding)
                        
                        # Use the appropriate registered font name for splitting
                        font_to_use_for_split = CUSTOM_FONT_BOLD if pdf_col_name == 'Store\nNumber' else CUSTOM_FONT_REGULAR
                        
                        try: lines_split = simpleSplit(raw_text, font_to_use_for_split, font_size_row, allowed_width); raw_text = lines_split[0] if lines_split else ""
                        except Exception: raw_text = raw_text[:int(allowed_width/(font_size_row*0.6))]
                        
                        # --- START: FONT MODIFICATION ---
                        cell_text_y = row_text_y
                        if pdf_col_name == 'Store\nNumber':
                            c.setFont(CUSTOM_FONT_BOLD, 12)
                            cell_text_y = y_pos - (row_height / 2) - (12 / 2.5)
                        else:
                            c.setFont(CUSTOM_FONT_REGULAR, font_size_row)
                        # --- END: FONT MODIFICATION ---
                        
                        if align == 'center': c.drawCentredString(x_pos + (col_width / 2), cell_text_y, raw_text)
                        else: c.drawString(x_pos + text_cell_padding, cell_text_y, raw_text)
                        
                        x_pos += col_width
                    
                    # --- NEW: Draw Right and Bottom borders for the row ---
                    c.setLineWidth(0.5)
                    c.line(row_line_end_x, y_pos, row_line_end_x, y_pos - row_height) # Right
                    c.line(row_line_start_x, y_pos - row_height, row_line_end_x, y_pos - row_height) # Bottom
                    
                    # --- START: FONT MODIFICATION ---
                    c.setFont(CUSTOM_FONT_REGULAR, font_size_row) # Reset font
                    # --- END: FONT MODIFICATION ---
                    y_pos -= row_height
                    
                # --- End of inner 'for' loop (processing rows on page) ---
                
                # =========================================================
                # --- 4. End of Page Processing ---
                # =========================================================
                
                # --- Draw the box for the last item on the page ---
                c.setLineWidth(order_box_line_width)
                
                y_pos -= store_padding
                c.rect(margin + frame_padding, y_pos, printable_width - (2 * frame_padding), store_start_y - y_pos, stroke=1, fill=0)
                
                # --- Check if we finished *all* rows in the sheet ---
                if not page_has_ended:
                    current_row_index = len(df_sheet)
                    
                    # --- We finished all rows, so "Close" the very last box ---
                    lines_to_draw = _build_fragmentation_lines(entities_in_current_store_box, store_report_map, sheet_name)
                    
                    y_pos, did_break = draw_message_block(c, lines_to_draw, y_pos,
                                                          frag_msg_font_size, frag_msg_line_height,
                                                          frag_msg_start_x, frag_msg_drawable_width,
                                                          page_bottom_margin_y,
                                                          trigger_page_break_for_messages)

                # =================================================================
                # --- NEW: Stamp Page Total Qty in Header ---
                # =================================================================
                if page_qty_total_x is not None and page_qty_total_y is not None:
                    try:
                        # --- START: FONT MODIFICATION ---
                        c.setFont(CUSTOM_FONT_BOLD, 11)
                        # --- END: FONT MODIFICATION ---
                        c.drawCentredString(page_qty_total_x, page_qty_total_y, str(int(page_total_qty)))
                    except Exception as e:
                        print(f"Warn: Could not stamp page total. {e}")
                
                c.showPage()
            # --- End Pagination (outer 'while') Loop ---

        if c.getPageNumber() == 0:
            print("⚠️ WARNING: No data found in any sheets. No PDF pages were generated.")
            print(f"  - Check the source file: {excel_path}")
            return False
        
        c.save()
        print(f"✓ PDF run list saved to: {pdf_path}")
        return True
    except Exception as e:
        print(f"⚠️ WARNING: Could not generate PDF run list. Error: {e}"); traceback.print_exc()
        return False


# --- MODIFIED main function (from 40_PdfRunlistGenerator.py) ---
def main(bundled_excel_path, output_dir, central_config_json, fragmentation_map_json):
    """
    Main execution function for generating PDF run lists.
    MODIFIED: Handles loading config, registering fonts, and calls PDF generation.
    """
    print("\n" + "="*50); print(" PDF RUN LIST GENERATOR SCRIPT (v12.4) ".center(50, "=")); print("="*50 + "\n")

    # --- Load config and fragmentation map from JSON strings ---
    try:
        central_config = json.loads(central_config_json)
        print("✓ Successfully loaded configuration from controller.")
    except json.JSONDecodeError as e:
        print(f"FATAL ERROR: Could not parse central_config JSON: {e}")
        traceback.print_exc()
        sys.exit(1)

    try:
        fragmentation_map = json.loads(fragmentation_map_json)
        print("✓ Successfully loaded fragmentation map from controller.")
    except json.JSONDecodeError as e:
        print(f"WARNING: Could not parse fragmentation_map JSON: {e}. Proceeding with empty map.")
        fragmentation_map = {'store_report_map': {}, 'unclaimed_report_map': {}}
    except Exception as e:
        print(f"FATAL ERROR: Unexpected error loading fragmentation_map: {e}")
        traceback.print_exc()
        sys.exit(1)
    # ---

    # --- FIX 2: Register fonts BEFORE calling PDF generation ---
    register_custom_fonts(central_config)

    start_time = time.time()
    
    base_name = os.path.splitext(os.path.basename(bundled_excel_path))[0]
    pdf_output_path = os.path.join(output_dir, f"{base_name}_RunLists.pdf")

    try:
        print(f"\n# GENERATING PDF FOR: {os.path.basename(bundled_excel_path)} #")

        pdf_settings = central_config.get('pdf_settings', {})
        if pdf_settings.get('generate_pdf_run_lists', False):
             history_path = 'run_history.yaml'
             if 'paths' in central_config and 'paths' in central_config['paths']:
                 history_path = central_config['paths'].get('run_history_path', 'run_history.yaml')
                 
             history = load_run_history(history_path)
             pdf_generated = generate_pdf_run_list(
                 excel_path=bundled_excel_path,
                 pdf_path=pdf_output_path,
                 config=central_config,
                 history=history,
                 fragmentation_map=fragmentation_map
             )
             if not pdf_generated:
                 raise Exception("PDF generation failed (see log for details).")

             print(f"\n✓ PDF generation complete. Final file: {pdf_output_path}")
        else:
            print("\n- PDF generation is disabled in config. Skipping.")

    except Exception as e:
        print(f"\n⚠️ CRITICAL ERROR generating PDF for {os.path.basename(bundled_excel_path)}: {e}")
        if "PDF generation failed" not in str(e):
             traceback.print_exc()
        sys.exit(1)
    finally:
        processing_time = time.time() - start_time
        print(f"\n--- Processing Time: {processing_time:.2f} seconds ---")

# --- UPDATED __main__ block (from 30_PdfRunlistGenerator.py) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="40 - Generate PDF Run Lists from bundled Excel.")
    parser.add_argument("bundled_excel_path", help="Path to the bundled Excel file created by 20a.")
    parser.add_argument("output_dir", help="Directory to save the new PDF and report.")
    parser.add_argument("central_config_json", help="Central configuration dictionary passed as a JSON string.")
    parser.add_argument("fragmentation_map_json", help="Fragmentation map dictionary passed as a JSON string.")

    args = parser.parse_args()

    # Call main with parsed args
    main(args.bundled_excel_path, args.output_dir, args.central_config_json, args.fragmentation_map_json)
