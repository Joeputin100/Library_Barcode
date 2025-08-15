import barcode
from barcode.writer import ImageWriter
import qrcode
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus.flowables import KeepInFrame
import io

LABEL_WIDTH = 2.625 * inch
LABEL_HEIGHT = 1.0 * inch
LABELS_PER_SHEET_WIDTH = 3
LABELS_PER_SHEET_HEIGHT = 10
PAGE_WIDTH, PAGE_HEIGHT = letter
LEFT_MARGIN = 0.1875 * inch
TOP_MARGIN = 0.5 * inch
HORIZONTAL_SPACING = 0.125 * inch
VERTICAL_SPACING = 0.0 * inch
GRID_SPACING = 0.1 * inch

def pad_inventory_number(inventory_num):
    return str(inventory_num).zfill(6)

def generate_barcode(inventory_num):
    padded_num = pad_inventory_number(inventory_num)
    EAN = barcode.get('code128', padded_num, writer=ImageWriter())
    buffer = io.BytesIO()
    EAN.write(buffer)
    buffer.seek(0)
    return ImageReader(buffer)

def generate_qrcode(inventory_num):
    padded_num = pad_inventory_number(inventory_num)
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(padded_num)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return ImageReader(buffer)

def _fit_text_to_box(c, text_lines, font_name, max_width, max_height, initial_font_size=10, min_font_size=5, alignment=TA_LEFT):
    styles = getSampleStyleSheet()
    style = styles['Normal']
    style.fontName = font_name
    style.alignment = alignment
    
    optimal_font_size = min_font_size
    text_block_height = 0

    for font_size in range(initial_font_size, min_font_size - 1, -1):
        style.fontSize = font_size
        current_text_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_width, max_height)
            current_text_height += height + (0.05 * inch)

        if current_text_height <= max_height:
            optimal_font_size = font_size
            text_block_height = current_text_height
            break
    else:
        optimal_font_size = min_font_size
        style.fontSize = min_font_size
        text_block_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_width, max_height)
            text_block_height += height + (0.05 * inch)

    return optimal_font_size, text_block_height

def create_label(c, x, y, book_data, label_type):
    title = book_data.get('Title', '')
    authors = book_data.get("Author", '')
    publication_year = book_data.get('Copyright Year', '')
    series_name = book_data.get('Series Info', '')
    series_number = book_data.get('Series Number', '')
    dewey_number = book_data.get('Call Number', '')
    inventory_number = pad_inventory_number(book_data.get('Holdings Barcode', ''))

    if label_type == 1 or label_type == 2:
        if len(title) > 26:
            title = title[:23] + '...'
        if series_name and len(series_name) > 26:
            series_name = series_name[:23] + '...'

    if label_type == 1:
        text_lines = [
            f"{title} - {authors} - {publication_year}",
        ]
        if series_name:
            text_lines.append(f"{series_name} {series_number}" if series_number else series_name)
        text_lines.append(f"<b>{inventory_number}</b> - <b>{dewey_number}</b>")

        max_text_width = LABEL_WIDTH - 10
        max_text_height = LABEL_HEIGHT - 10
        
        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = style.fontSize * 1.1

        current_y = y + LABEL_HEIGHT - 5

        if not series_name:
            current_y -= (1.5 * GRID_SPACING)

        for line_idx, line_text in enumerate(text_lines):
            if series_name and line_idx == 2:
                current_y -= (1 * GRID_SPACING)

            optimal_font_size_line = 18
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            if line_idx == len(text_lines) - 1:
                style.fontName = 'Courier-Bold'
            else:
                style.fontName = 'Courier'
            
            p = Paragraph(line_text, style)
            width, height = p.wrapOn(c, max_text_width, LABEL_HEIGHT * 2)
            current_y -= height
            p.drawOn(c, x + 5, current_y)
            current_y -= (0.02 * inch)

    elif label_type == 2:
        qr_code_size = LABEL_HEIGHT - GRID_SPACING
        qr_code_x = x + LABEL_WIDTH - qr_code_size
        qr_code_y = y + (LABEL_HEIGHT - qr_code_size) / 2

        qr_image = generate_qrcode(inventory_number)
        c.drawImage(qr_image, qr_code_x, qr_code_y, width=qr_code_size, height=qr_code_size)
        
        text_lines = [
            title,
            authors.split(',')[0] if authors else '',
        ]
        if series_name:
            text_lines.append(f"{series_name} #{series_number}" if series_number else series_name)
        text_lines.append(inventory_number)

        max_text_width = LABEL_WIDTH - qr_code_size - 10
        max_text_height = LABEL_HEIGHT - 10

        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = 1.5

        total_text_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height)
            total_text_height += height + (0.05 * inch)

        current_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height

        for line_idx, line_text in enumerate(text_lines):
            line_offset_y = 0
            if line_idx == 0:
                line_offset_y = 3 * GRID_SPACING
            elif line_idx == 1:
                line_offset_y = 2.5 * GRID_SPACING
            elif line_idx == 3:
                line_offset_y = -1.25 * GRID_SPACING

            optimal_font_size_line = 18
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            p = Paragraph(line_text, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height)
            current_y -= height
            p.drawOn(c, x + 5, current_y + line_offset_y)
            current_y -= (0.05 * inch)

    elif label_type == 3:
        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        line_height = 12
        total_text_height = len(lines) * line_height
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8)

        b_text = book_data.get('spine_label_id', 'B')
        b_font_size = 100
        while c.stringWidth(b_text, 'Helvetica-Bold', b_font_size) > LABEL_WIDTH and b_font_size > 10:
            b_font_size -= 1
        b_font_size *= 0.9

        c.setFont('Helvetica-Bold', b_font_size)
        b_text_width = c.stringWidth(b_text, 'Helvetica-Bold', b_font_size)
        b_x = x + LABEL_WIDTH - b_text_width
        b_y = y + (LABEL_HEIGHT - b_font_size * 0.8) / 2 + (0.5 * GRID_SPACING)

        c.drawString(b_x, b_y, b_text)

        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        line_height = 12
        total_text_height = len(lines) * line_height
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8)

        for i, line in enumerate(lines):
            text_width = c.stringWidth(line, 'Courier-Bold', 10)
            c.drawString(x + (LABEL_WIDTH - text_width) / 2, start_y - (i * line_height), line)

    elif label_type == 4:
        barcode_height = 7 * GRID_SPACING
        barcode_width = barcode_height * ((LABEL_WIDTH * 0.8 - 4 * GRID_SPACING) / (LABEL_HEIGHT * 0.6))
        barcode_x = x + LABEL_WIDTH - barcode_width
        barcode_y = y

        barcode_image = generate_barcode(inventory_number)
        c.drawImage(barcode_image, barcode_x, barcode_y, width=barcode_width, height=barcode_height)
        
        text_above_barcode_lines = [
            f"{title} by {authors.split(',')[0] if authors else ''}",
        ]
        max_text_above_width = LABEL_WIDTH - 10
        max_text_above_height = (y + LABEL_HEIGHT) - (barcode_y + barcode_height) - 5

        optimal_font_size_above, text_block_height_above = _fit_text_to_box(c, text_above_barcode_lines, 'Courier', max_text_above_width, max_text_above_height, initial_font_size=10, alignment=TA_CENTER)
        
        styles = getSampleStyleSheet()
        style_above = styles['Normal']
        style_above.fontName = 'Courier'
        style_above.fontSize = optimal_font_size_above
        style_above.leading = optimal_font_size_above * 1.2
        style_above.alignment = TA_CENTER

        current_y_above = y + LABEL_HEIGHT - 5
        for line in text_above_barcode_lines:
            p = Paragraph(line, style_above)
            width, height = p.wrapOn(c, max_text_above_width, max_text_above_height)
            p.drawOn(c, x + 5, current_y_above - height)
            current_y_above -= (height + (0.05 * inch))

        text_left_barcode_lines = []
        if series_name:
            text_left_barcode_lines.append(f"Vol. {series_number}" if series_number else series_name)

        max_text_left_width = barcode_x - (x + 5) - 5
        max_text_left_height = barcode_height

        optimal_font_size_left = 10
        if series_number and len(series_number) > 1:
            optimal_font_size_left -= (len(series_number) - 1)
        optimal_font_size_left = max(optimal_font_size_left, 5)

        optimal_font_size_left, text_block_height_left = _fit_text_to_box(c, text_left_barcode_lines, 'Courier', max_text_left_width, max_text_left_height, initial_font_size=optimal_font_size_left, alignment=TA_LEFT)

        style_left = styles['Normal']
        style_left.fontName = 'Courier'
        style_left.fontSize = optimal_font_size_left
        style_left.leading = optimal_font_size_left * 1.2
        style_left.alignment = TA_LEFT

        if text_left_barcode_lines:
            c.saveState()
            text_origin_x = x + 5
            vertical_offset = 0
            if series_number and len(series_number) > 1:
                vertical_offset = (len(series_number) - 1) * GRID_SPACING
            text_origin_y = y + (LABEL_HEIGHT - text_block_height_left) / 2 + text_block_height_left - (0.1 * inch) - (3.5 * GRID_SPACING)
            
            c.translate(text_origin_x, text_origin_y)
            c.rotate(90)
            
            current_rotated_y = 0
            for line in text_left_barcode_lines:
                p = Paragraph(line, style_left)
                width, height = p.wrapOn(c, max_text_left_width, max_text_left_height)
                current_rotated_y -= height
                p.drawOn(c, 0, current_rotated_y)
                current_rotated_y -= (0.05 * inch)
            c.restoreState()

def generate_pdf_labels(df):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    label_count = 0
    for index, row in df.iterrows():
        book_data = row.to_dict()
        for label_type in range(1, 5):
            row_num = (label_count // LABELS_PER_SHEET_WIDTH) % LABELS_PER_SHEET_HEIGHT
            col_num = label_count % LABELS_PER_SHEET_WIDTH

            x_pos = LEFT_MARGIN + col_num * (LABEL_WIDTH + HORIZONTAL_SPACING)
            y_pos = PAGE_HEIGHT - TOP_MARGIN - (row_num + 1) * (LABEL_HEIGHT + VERTICAL_SPACING)

            c.setDash(1, 2)
            c.setStrokeColorRGB(0, 0, 0)
            c.setLineWidth(0.5)
            c.roundRect(x_pos, y_pos, LABEL_WIDTH, LABEL_HEIGHT, 5)

            c.setDash()
            c.setStrokeColorRGB(0, 0, 0)
            c.setLineWidth(0.5)
            
            if col_num < LABELS_PER_SHEET_WIDTH - 1:
                c.line(x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos, x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos + LABEL_HEIGHT)

            if row_num < LABELS_PER_SHEET_HEIGHT - 1:
                c.line(x_pos, y_pos - VERTICAL_SPACING / 2, x_pos + LABEL_WIDTH, y_pos - VERTICAL_SPACING / 2)

            create_label(c, x_pos, y_pos, book_data, label_type)
            label_count += 1

            if label_count % (LABELS_PER_SHEET_WIDTH * LABELS_PER_SHEET_HEIGHT) == 0:
                c.showPage()
                c.setFont('Courier', 8)

    c.save()
    buffer.seek(0)
    return buffer.getvalue()
