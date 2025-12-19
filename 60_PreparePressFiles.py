# 60_PreparePressFiles.py
import os
import time
import shutil
import re
import pandas as pd
import traceback
import math
from datetime import datetime, timedelta
from io import BytesIO
import sys
from itertools import groupby
import argparse
import json

# PDF Libraries
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter, PageObject, Transformation
from pypdf.generic import DictionaryObject, NameObject, RectangleObject
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from PIL import Image

# ReportLab Barcode Library
try:
    from reportlab.graphics.barcode import code128
    from reportlab.pdfgen import canvas as rl_canvas
except ImportError:
    print("FATAL ERROR: Required library for barcodes not found. Please ensure 'reportlab' is installed.")
    sys.exit(1)

# --- Define the string that identifies a gang run sheet ---
GANG_RUN_TRIGGER = "-GR-"

# Configuration for Header Pages
HEADER_FONT_SIZE = 18
HEADER_TOP_MARGIN = 72
HEADER_PAGE_WIDTH = 2.25 * 72
HEADER_PAGE_HEIGHT = 3.75 * 72
HEADER_TRIM_WIDTH = 2 * 72
HEADER_TRIM_HEIGHT = 3.5 * 72

# --- Fixed Layout Constants (Added for Manual Control) ---
FN_FONT_SIZE = 12         # Filename font size
QTY_FONT_SIZE = 7         # Total Qty font size
STORE_FONT_SIZE = 12      # Store number font size
ORDER_FONT_SIZE = 7       # Order number font size
BARCODE_TEXT_SIZE = 6     # Human-readable barcode text
BARCODE_HEIGHT = 18       # Height of the barcode bars
ICON_HEIGHT = 18          # Height of the box icons
BLOCK_SPACING = 6         # Vertical gap between component blocks
LINE_SPACING = 2          # Vertical gap between lines within a block

# Barcode Helper Function
def _create_barcode_pdf_in_memory(data_string, width, height):
    """Generates a Code 128 barcode PDF in memory using ReportLab."""
    buffer = BytesIO()
    c = rl_canvas.Canvas(buffer, pagesize=(width, height))
    barcode = code128.Code128(data_string, barHeight=height, barWidth=1.4) 
    barcode_actual_width = barcode.width 
    x_centered = (width - barcode_actual_width) / 2
    barcode.drawOn(c, x_centered, 0)
    c.save()
    buffer.seek(0)
    return buffer

def create_header_page(pdf_path, order_number=None, segment=None, total_segments=None, total_quantity=None, background_color=None, store_number=None, half_box_icon_path=None, full_box_icon_path=None, box_value=None):
    """
    Creates a new PDF page in memory to serve as a header with FIXED layout dimensions.
    """
    header_doc = None
    src_doc = None
    try:
        header_doc = fitz.open()
        header_page = header_doc.new_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        
        if background_color and len(background_color) == 4:
            header_page.draw_rect(header_page.rect, color=background_color, fill=background_color)
            
        p1_rect, p2_rect = None, None
        trim_x_margin = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH) / 2
        trim_y_margin = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT) / 2
        trim_left_edge = trim_x_margin
        trim_right_edge = HEADER_PAGE_WIDTH - trim_x_margin
        trim_top_edge = trim_y_margin
        trim_bottom_edge = HEADER_PAGE_HEIGHT - trim_y_margin
        offset = 0.125 * 72 

        # --- 1. Determine Artwork Preview Area (Anchored to Bottom) ---
        try:
            src_doc = fitz.open(pdf_path)
            if src_doc.page_count > 0:
                page1 = src_doc[0]
                is_landscape_original = page1.rect.width > page1.rect.height

                if is_landscape_original:
                    available_width = HEADER_TRIM_WIDTH - (2 * offset)
                    scale_prev = available_width / page1.rect.width if page1.rect.width > 0 else 0
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]
                        scale2 = available_width / page2.rect.width if page2.rect.width > 0 else 0
                        scale_prev = min(scale_prev, scale2)

                    p1_w, p1_h = page1.rect.width * scale_prev, page1.rect.height * scale_prev
                    p1_x0 = trim_left_edge + (HEADER_TRIM_WIDTH - p1_w) / 2
                    current_y_bottom = trim_bottom_edge - offset + 36 
                    
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]
                        p2_w, p2_h = page2.rect.width * scale_prev, page2.rect.height * scale_prev
                        p2_x0 = trim_left_edge + (HEADER_TRIM_WIDTH - p2_w) / 2
                        p2_y1 = current_y_bottom
                        p2_y0 = p2_y1 - p2_h
                        p2_rect = fitz.Rect(p2_x0, p2_y0, p2_x0 + p2_w, p2_y1)
                        current_y_bottom = p2_y0 - offset

                    p1_y1 = current_y_bottom
                    p1_y0 = p1_y1 - p1_h
                    p1_rect = fitz.Rect(p1_x0, p1_y0, p1_x0 + p1_w, p1_y1)
                else:
                    scale_prev = 0.62
                    p1_w, p1_h = page1.rect.width * scale_prev, page1.rect.height * scale_prev
                    p1_y1 = trim_bottom_edge - offset + 40; p1_y0 = p1_y1 - p1_h
                    p1_x0 = trim_left_edge + offset
                    p1_rect = fitz.Rect(p1_x0, p1_y0, p1_x0 + p1_w, p1_y1)
                    
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]; p2_w, p2_h = page2.rect.width * scale_prev, page2.rect.height * scale_prev
                        p2_x1 = trim_right_edge - offset; p2_x0 = p2_x1 - p2_w
                        p2_y0 = trim_top_edge + 1.5 * 72 + 36
                        p2_rect = fitz.Rect(p2_x0, p2_y0, p2_x0 + p2_w, p2_y0 + p2_h)
        except Exception as e:
            print(f"  - Could not open/process preview doc: {e}")

        # --- 2. Fixed Sequential Layout (Top to Bottom) ---
        font_reg, font_bold = "helvetica", "helvetica-bold"
        current_y = trim_top_edge + offset

        # Block A: Filename / Total Qty
        filename_text = os.path.splitext(os.path.basename(pdf_path))[0]
        text_len = fitz.get_text_length(filename_text, fontname=font_bold, fontsize=FN_FONT_SIZE)
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + FN_FONT_SIZE), filename_text, fontname=font_bold, fontsize=FN_FONT_SIZE)
        current_y += FN_FONT_SIZE + LINE_SPACING

        qty_text = f"Total Qty: {total_quantity}" if total_quantity is not None else "Total Qty: N/A"
        text_len = fitz.get_text_length(qty_text, fontname=font_reg, fontsize=QTY_FONT_SIZE)
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + QTY_FONT_SIZE), qty_text, fontname=font_reg, fontsize=QTY_FONT_SIZE)
        current_y += QTY_FONT_SIZE + BLOCK_SPACING

        # Block B: Store/Order Info
        store_text = f"Store: {store_number}" if store_number else ""
        text_len = fitz.get_text_length(store_text, fontname=font_bold, fontsize=STORE_FONT_SIZE)
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + STORE_FONT_SIZE), store_text, fontname=font_bold, fontsize=STORE_FONT_SIZE)
        current_y += STORE_FONT_SIZE + LINE_SPACING

        order_text = f"Order: {order_number}" if order_number else ""
        text_len = fitz.get_text_length(order_text, fontname=font_reg, fontsize=ORDER_FONT_SIZE)
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + ORDER_FONT_SIZE), order_text, fontname=font_reg, fontsize=ORDER_FONT_SIZE)
        current_y += ORDER_FONT_SIZE + BLOCK_SPACING

# Block C: Barcode (Suppressed if no icon drawn)
        stacks_per_box = 2
        is_completion_stack = (segment % stacks_per_box == 0) or (segment == total_segments)
        will_draw_box_icon = False
        if total_segments and is_completion_stack:
            if (total_quantity == 250 and half_box_icon_path and os.path.exists(half_box_icon_path)) or \
               (total_quantity in [500, 1000] and full_box_icon_path and os.path.exists(full_box_icon_path)):
                will_draw_box_icon = True

        if will_draw_box_icon and box_value:
            barcode_canvas_w = 1.75 * 72 # 126pt
            barcode_x0 = (HEADER_PAGE_WIDTH - barcode_canvas_w) / 2
            
            # White Background Box for Barcode Scannability
            white_box_w = 136
            white_box_h = 20
            white_box_x0 = (HEADER_PAGE_WIDTH - white_box_w) / 2
            # Center vertically to the barcode height (BARCODE_HEIGHT is 18)
            white_box_y0 = current_y - ((white_box_h - BARCODE_HEIGHT) / 2)
            
            white_rect = fitz.Rect(white_box_x0, white_box_y0, white_box_x0 + white_box_w, white_box_y0 + white_box_h)
            header_page.draw_rect(white_rect, color=(1, 1, 1), fill=(1, 1, 1)) # Pure White
            
            # 1. Draw Barcode PDF (Now on top of the white box)
            rect = fitz.Rect(barcode_x0, current_y, barcode_x0 + barcode_canvas_w, current_y + BARCODE_HEIGHT)
            with fitz.open("pdf", _create_barcode_pdf_in_memory(box_value, barcode_canvas_w, BARCODE_HEIGHT)) as barcode_doc:
                header_page.show_pdf_page(rect, barcode_doc, 0)
            
            # 2. Increment Y by the barcode height
            current_y += BARCODE_HEIGHT + 2 # Small 2pt gap between bars and text
            
            # 3. Draw Human Readable text BELOW the bars
            text_len = fitz.get_text_length(box_value, fontname='helvetica', fontsize=BARCODE_TEXT_SIZE)
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + BARCODE_TEXT_SIZE), 
                                    box_value, fontname='helvetica', fontsize=BARCODE_TEXT_SIZE)
            
            # 4. Final increment to move to the next block
            current_y += BARCODE_TEXT_SIZE + BLOCK_SPACING
        else:
            # Maintain spacing even if hidden to keep headers consistent
            current_y += BARCODE_HEIGHT + 2 + BARCODE_TEXT_SIZE + BLOCK_SPACING

        # Block D: Icon/Stack Text
        if will_draw_box_icon:
            def place_icon_fixed(page, doc, x_pos, y_pos):
                 if not doc or doc.page_count == 0: return 0
                 icon_page = doc[0]
                 aspect_ratio = icon_page.rect.width / icon_page.rect.height if icon_page.rect.height > 0 else 1
                 target_w = ICON_HEIGHT * aspect_ratio
                 target_rect = fitz.Rect(x_pos, y_pos, x_pos + target_w, y_pos + ICON_HEIGHT)
                 page.show_pdf_page(target_rect, doc, 0)
                 return target_w

            half_box_doc, full_box_doc = None, None
            try:
                if total_quantity == 250:
                    half_box_doc = fitz.open(half_box_icon_path)
                    w = place_icon_fixed(header_page, half_box_doc, 0, -999) # Get width
                    place_icon_fixed(header_page, half_box_doc, (HEADER_PAGE_WIDTH - w)/2, current_y)
                elif total_quantity == 500:
                    full_box_doc = fitz.open(full_box_icon_path)
                    w = place_icon_fixed(header_page, full_box_doc, 0, -999)
                    place_icon_fixed(header_page, full_box_doc, (HEADER_PAGE_WIDTH - w)/2, current_y)
                elif total_quantity == 1000:
                    full_box_doc = fitz.open(full_box_icon_path)
                    w = place_icon_fixed(header_page, full_box_doc, 0, -999)
                    gap = 4
                    start_x = (HEADER_PAGE_WIDTH - (w * 2 + gap)) / 2
                    place_icon_fixed(header_page, full_box_doc, start_x, current_y)
                    place_icon_fixed(header_page, full_box_doc, start_x + w + gap, current_y)
            finally:
                if half_box_doc: half_box_doc.close()
                if full_box_doc: full_box_doc.close()
        elif total_segments and total_segments > 1:
            stack_text = f"Stack {segment} of {total_segments}"
            text_len = fitz.get_text_length(stack_text, fontname=font_reg, fontsize=10)
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - text_len) / 2, current_y + 10), stack_text, fontname=font_reg, fontsize=10)

        # --- 3. Finalize Previews ---
        if src_doc and src_doc.page_count > 0:
            def render_preview(page_index, rect):
                pix = src_doc[page_index].get_pixmap(dpi=144)
                header_page.insert_image(rect, stream=pix.tobytes("png"))
                header_page.draw_rect(rect, color=(0,0,0), width=0.5)
            if p1_rect: render_preview(0, p1_rect)
            if p2_rect and src_doc.page_count > 1: render_preview(1, p2_rect)

        # Finalize and return
        packet = BytesIO()
        header_doc.save(packet, garbage=4, deflate=True)
        packet.seek(0)
        final_header_page = PdfReader(packet).pages[0]
        trimbox_coords = [trim_x_margin, trim_y_margin, HEADER_PAGE_WIDTH - trim_x_margin, HEADER_PAGE_HEIGHT - trim_y_margin]
        final_header_page.trimbox = RectangleObject(trimbox_coords)
        return final_header_page
    finally:
        if header_doc: header_doc.close()
        if src_doc: src_doc.close()

# --- PRESERVED FUNCTIONS BELOW ---

def add_segmented_headers_to_pdf(orientation_check_path, target_pdf_path, order_number=None, total_quantity=None, background_color=None, store_number=None, half_box_icon_path=None, full_box_icon_path=None, box_values=None):
    if box_values is None: box_values = {}
    box_keys = sorted(box_values.keys())
    barcode_values = [box_values.get(k) for k in box_keys] 

    try:
        reader = PdfReader(target_pdf_path)
        total_pages = len(reader.pages)
        if total_pages == 0: return False

        num_segments = (total_pages + 499) // 500
        writer = PdfWriter()
        
        x_margin = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH) / 2
        y_margin = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT) / 2
        centered_trimbox = RectangleObject([x_margin, y_margin, HEADER_PAGE_WIDTH - x_margin, HEADER_PAGE_HEIGHT - y_margin])
        blank_header_page = PageObject.create_blank_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        blank_header_page.trimbox = centered_trimbox

        barcode_index = 0
        for i in range(num_segments):
            segment_num = i + 1
            will_draw_box_icon = False
            stacks_per_box = 2
            is_completion_stack = (segment_num % stacks_per_box == 0) or (segment_num == num_segments)

            if num_segments and is_completion_stack:
                if total_quantity == 250 and half_box_icon_path and os.path.exists(half_box_icon_path):
                    will_draw_box_icon = True
                elif total_quantity in [500, 1000] and full_box_icon_path and os.path.exists(full_box_icon_path):
                    will_draw_box_icon = True
            
            box_barcode_value = None
            if will_draw_box_icon:
                if barcode_index < len(barcode_values):
                    val = barcode_values[barcode_index]
                    if val and str(val).lower() != 'nan':
                        box_barcode_value = val
                    barcode_index += 1 
            
            text_header = create_header_page(
                orientation_check_path, order_number, segment_num, num_segments, total_quantity, 
                background_color, store_number, half_box_icon_path, full_box_icon_path, box_barcode_value
            )
            writer.add_page(text_header)
            writer.add_page(blank_header_page)
            start_index, end_index = i * 500, (i + 1) * 500
            for page in reader.pages[start_index:end_index]: writer.add_page(page)

        with open(target_pdf_path, "wb") as out_file: writer.write(out_file)
        return True
    except Exception as e:
        print(f"  - FAILED to add segmented headers: {e}"); traceback.print_exc(); return False

def sanitize_filename(filename):
    filename = str(filename).replace('/', '-')
    return re.sub(r'[\\:*?"<>|]', '', filename).strip()

def natural_keys(text):
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', str(text))]

def standardize_pdf_for_gang_run(pdf_path):
    try:
        reader_check = PdfReader(pdf_path)
        if not reader_check.pages or reader_check.pages[0].mediabox.width <= reader_check.pages[0].mediabox.height:
            return False
        writer = PdfWriter()
        reader = PdfReader(pdf_path)
        for i, original_page in enumerate(reader.pages):
            page_number = i + 1
            width = float(original_page.mediabox.width)
            height = float(original_page.mediabox.height)
            new_page = PageObject.create_blank_page(width=height, height=width)
            if page_number % 2 != 0:
                transform = Transformation().rotate(-90).translate(tx=0, ty=width)
                recalculate_box = lambda box: RectangleObject((box.lower_left[1], width - box.upper_right[0], box.upper_right[1], width - box.lower_left[0]))
            else:
                transform = Transformation().rotate(90).translate(tx=height, ty=0)
                recalculate_box = lambda box: RectangleObject((height - box.upper_right[1], box.lower_left[0], height - box.lower_left[1], box.upper_right[0]))
            boxes = ["mediabox", "cropbox", "bleedbox", "trimbox", "artbox"]
            for box in boxes:
                if hasattr(original_page, box): setattr(new_page, box, recalculate_box(getattr(original_page, box)))
            new_page.merge_transformed_page(original_page, transform)
            writer.add_page(new_page)
        with open(pdf_path, "wb") as out_file: writer.write(out_file)
        return True
    except Exception as e:
        print(f"  - Standardization failed: {e}"); traceback.print_exc(); return False

def process_dataframe(df, files_path, originals_path, sheet_name, color_palette_path=None, icon_paths=None):
    is_gang_run = GANG_RUN_TRIGGER in sheet_name.upper()
    if not is_gang_run:
        print(f"\nProcessing sheet '{sheet_name}' as STANDARD. No modifications needed."); return

    start_time = time.time()
    color_map = None
    loaded_palette = []
    try:
        if color_palette_path and os.path.exists(color_palette_path):
            palette_df = pd.read_csv(color_palette_path, encoding='utf-8-sig')
            if all(col in palette_df.columns for col in ['C', 'M', 'Y', 'K']):
                loaded_palette = [tuple(row) for row in palette_df[['C', 'M', 'Y', 'K']].to_numpy()]
    except Exception as e: print(f"  - WARNING: Palette error: {e}")

    if loaded_palette:
        sort_data = []
        for idx, row in df.iterrows():
            qty = int(pd.to_numeric(row.get("quantity_ordered"), errors='coerce') or 0)
            ticket = str(row.get("job_ticket_number", ""))
            sort_data.append({'original_index': idx, 'qty': qty, 'ticket': ticket})
        sort_data.sort(key=lambda x: (-x['qty'], natural_keys(x['ticket'])))
        color_map = {item['original_index']: loaded_palette[rank] for rank, item in enumerate(sort_data) if rank < len(loaded_palette)}

    process_rows(df, files_path, originals_path, is_gang_run, start_time, color_map, icon_paths)

def process_rows(rows, files_path, originals_path, is_gang_run, sheet_start_time, color_map, icon_paths):
    BOX_COLS = [f'box_{chr(65+i)}' for i in range(8)]
    for idx, row in rows.iterrows():
        try:
            file_base = sanitize_filename(str(row.get("job_ticket_number")))
            production_path = os.path.join(files_path, f"{file_base}.pdf")
            if not os.path.exists(production_path): continue

            if is_gang_run:
                box_values = {col: str(row.get(col)).strip() for col in BOX_COLS if pd.notna(row.get(col)) and str(row.get(col)).strip().lower() != 'nan'}
                os.makedirs(originals_path, exist_ok=True)
                archived_path = os.path.join(originals_path, f"{file_base}.pdf")
                shutil.copy2(production_path, archived_path)
                
                reader = PdfReader(archived_path)
                if len(reader.pages) in [1, 2]:
                    qty = int(pd.to_numeric(row.get("quantity_ordered"), errors='coerce') or 1)
                    writer = PdfWriter()
                    if len(reader.pages) == 1:
                        art = reader.pages[0]
                        blank = PageObject.create_blank_page(width=art.mediabox.width, height=art.mediabox.height)
                        for box in ["mediabox", "cropbox", "bleedbox", "trimbox", "artbox"]:
                            if hasattr(art, box): setattr(blank, box, getattr(art, box))
                        for _ in range(qty): writer.add_page(art); writer.add_page(blank)
                    else:
                        for _ in range(qty): writer.add_page(reader.pages[0]); writer.add_page(reader.pages[1])
                    with open(production_path, "wb") as f_out: writer.write(f_out)
                
                standardize_pdf_for_gang_run(production_path)
                store = str(row.get("cost_center", "")).split('-')[0].strip() if pd.notna(row.get("cost_center")) else ""
                add_segmented_headers_to_pdf(archived_path, production_path, str(row.get("order_number", "")), int(pd.to_numeric(row.get('quantity_ordered'), errors='coerce') or 0), color_map.get(idx), store, icon_paths.get('HALF_BOX_ICON_PATH'), icon_paths.get('FULL_BOX_ICON_PATH'), box_values)

            sys.stdout.write(f"Pre-pressing file {file_base}\n"); sys.stdout.flush()
        except Exception as e: print(f"Error row {idx}: {e}")

def main(input_excel_path, files_base_folder, originals_base_folder, central_config_json):
    central_config = json.loads(central_config_json)
    color_palette_path = central_config.get('COLOR_PALETTE_PATH')
    icon_paths = {'HALF_BOX_ICON_PATH': central_config.get('HALF_BOX_ICON_PATH'), 'FULL_BOX_ICON_PATH': central_config.get('FULL_BOX_ICON_PATH')}

    xls = pd.ExcelFile(input_excel_path)
    for sheet_name in xls.sheet_names:
        if GANG_RUN_TRIGGER not in sheet_name.upper(): continue
        dtype_map = {f'box_{chr(65+i)}': str for i in range(8)}
        df = pd.read_excel(xls, sheet_name=sheet_name, dtype=dtype_map)
        process_dataframe(df, os.path.join(files_base_folder, sanitize_filename(sheet_name)), os.path.join(originals_base_folder, sanitize_filename(sheet_name)), sheet_name, color_palette_path, icon_paths)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_excel_path")
    parser.add_argument("files_base_folder")
    parser.add_argument("originals_folder")
    parser.add_argument("central_config_json")
    args = parser.parse_args()
    main(args.input_excel_path, args.files_base_folder, args.originals_folder, args.central_config_json)