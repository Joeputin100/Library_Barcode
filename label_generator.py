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
import shutil
import csv
import os

# Avery 5160 label dimensions (approximate, for layout)
LABEL_WIDTH = 2.625 * inch
LABEL_HEIGHT = 1.0 * inch
LABELS_PER_SHEET_WIDTH = 3
LABELS_PER_SHEET_HEIGHT = 10
PAGE_WIDTH, PAGE_HEIGHT = letter # 8.5 x 11 inches

# Margins for Avery 5160 (standard, adjust if needed)
LEFT_MARGIN = 0.1875 * inch
TOP_MARGIN = 0.5 * inch
HORIZONTAL_SPACING = 0.125 * inch # Space between labels horizontally
VERTICAL_SPACING = 0.0 * inch # Space between labels vertically (they touch)

# Debugging grid spacing
GRID_SPACING = 0.1 * inch

def pad_inventory_number(inventory_num):
    """Pads the inventory number with leading zeros to 6 digits."""
    return str(inventory_num).zfill(6)

def generate_barcode(inventory_num):
    """Generates a Code 128 barcode image for the given inventory number."""
    padded_num = pad_inventory_number(inventory_num)
    EAN = barcode.get('code128', padded_num, writer=ImageWriter())
    # Save to a BytesIO object to avoid writing to disk
    buffer = io.BytesIO()
    EAN.write(buffer)
    buffer.seek(0)
    return ImageReader(buffer)

def generate_qrcode(inventory_num):
    """Generates a QR code image for the given inventory number."""
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
    # Save to a BytesIO object
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return ImageReader(buffer)

def _fit_text_to_box(c, text_lines, font_name, max_width, max_height, initial_font_size=10, min_font_size=5, alignment=TA_LEFT):
    """
    Finds the largest font size that allows all text_lines to fit within max_width and max_height.
    Returns the optimal font size and the calculated height of the text block.
    """
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
            # Calculate wrapped height for each paragraph
            width, height = p.wrapOn(c, max_width, max_height) # c is needed for wrapOn
            current_text_height += height + (0.05 * inch) # Add line spacing

        if current_text_height <= max_height:
            optimal_font_size = font_size
            text_block_height = current_text_height
            break
    else: # If loop completes without breaking, no font size fits, use min_font_size
        optimal_font_size = min_font_size
        style.fontSize = min_font_size
        text_block_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_width, max_height)
            text_block_height += height + (0.05 * inch)

    return optimal_font_size, text_block_height

def create_label(c, x, y, book_data, label_type):
    """
    Renders content for a single label on the PDF canvas.
    c: reportlab canvas object
    x, y: bottom-left coordinates of the label
    book_data: dictionary containing book information
    label_type: 1, 2, 3, or 4
    """
    title = book_data.get('Title', '')
    authors = book_data.get("Author's Name", '')
    publication_year = book_data.get('Publication Year', '')
    series_name = book_data.get('Series Title', '')
    series_number = book_data.get('Series Volume', '')
    dewey_number = book_data.get('Call Number', '')
    inventory_number = pad_inventory_number(book_data.get('Holdings Barcode', ''))

    # Truncate title and series_name for Label 1 if they are too long
    if label_type == 1 or label_type == 2:
        if len(title) > 26:
            title = title[:23] + '...'
        if series_name and len(series_name) > 26:
            series_name = series_name[:23] + '...'

    if label_type == 1:
        # Label 1: title, authors, publication year, series, Dewey, inventory number
        text_lines = [
            f"{title} - {authors} - {publication_year}",
        ]
        if series_name:
            text_lines.append(f"{series_name} {series_number}" if series_number else series_name)
        text_lines.append(f"<b>{inventory_number}</b> - <b>{dewey_number}</b>") # Bold inventory and Dewey numbers

        # Dynamic font sizing for Label 1
        max_text_width = LABEL_WIDTH - 10 # 5 units margin on each side
        max_text_height = LABEL_HEIGHT - 10 # 5 units margin on top/bottom
        
        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = style.fontSize * 1.1 # Adjusted line spacing

        current_y = y + LABEL_HEIGHT - 5 # Start near top of label

        # Adjust initial Y position for non-series books
        if not series_name:
            current_y -= (1.5 * GRID_SPACING) # Move all lines down 1.5 blue box heights

        for line_idx, line_text in enumerate(text_lines):
            # For the third line (index 2) in series books, move down by 1 blue box height
            if series_name and line_idx == 2:
                current_y -= (1 * GRID_SPACING)

            # Dynamically adjust font size to fit within max_text_width without wrapping
            optimal_font_size_line = 18 # Start with a large font size
            # Use c.stringWidth for precise width calculation without word wrap
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            # For the last line (inventory and dewey), use bold font
            if line_idx == len(text_lines) - 1:
                style.fontName = 'Courier-Bold'
            else:
                style.fontName = 'Courier'
            
            p = Paragraph(line_text, style)
            # Use a very large height to prevent Paragraph from wrapping vertically,
            # as we've already ensured horizontal fit with optimal_font_size_line
            width, height = p.wrapOn(c, max_text_width, LABEL_HEIGHT * 2) 
            current_y -= height # Move down for current line
            p.drawOn(c, x + 5, current_y) # Draw from top down
            current_y -= (0.02 * inch) # Reduced line spacing

    elif label_type == 2:
        # Label 2: title, author, series, inventory number, QR code (half label)
        qr_code_size = LABEL_HEIGHT - GRID_SPACING # Crop by half a blue box on all sides
        qr_code_x = x + LABEL_WIDTH - qr_code_size # Flush with the right side of the label
        qr_code_y = y + (LABEL_HEIGHT - qr_code_size) / 2 # Vertically centered

        qr_image = generate_qrcode(inventory_number)
        c.drawImage(qr_image, qr_code_x, qr_code_y, width=qr_code_size, height=qr_code_size)
        

        text_lines = [
            title,
            authors.split(',')[0] if authors else '',
        ]
        if series_name:
            text_lines.append(f"{series_name} #{series_number}" if series_number else series_name)
        text_lines.append(inventory_number)

        # Calculate text area width (left of QR code) and height
        max_text_width = LABEL_WIDTH - qr_code_size - 10 # 10 units for margins
        max_text_height = LABEL_HEIGHT - 10 # 5 units margin on top/bottom

        styles = getSampleStyleSheet()
        style = styles['Normal']
        style.fontName = 'Courier'
        style.leading = 1.5 # Increased line spacing

        # Calculate vertical start position to center text block
        # This will be adjusted per line for independent sizing
        # Calculate total text height for vertical centering
        total_text_height = 0
        for line in text_lines:
            p = Paragraph(line, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height) # Use max_text_height to allow wrapping
            total_text_height += height + (0.05 * inch) # Add line spacing

        # Adjust initial Y position to vertically center the entire text block
        current_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height # Start from the top of the text block

        for line_idx, line_text in enumerate(text_lines):
            # Apply specific offsets
            line_offset_y = 0
            if line_idx == 0: # Row 1 (index 0)
                line_offset_y = 3 * GRID_SPACING # Move up
            elif line_idx == 1: # Row 2 (index 1)
                line_offset_y = 2.5 * GRID_SPACING # Move up
            elif line_idx == 3: # Row 4 (index 3)
                line_offset_y = -1.25 * GRID_SPACING # Move down

            # Dynamically adjust font size to fit within max_text_width without wrapping
            optimal_font_size_line = 18 # Start with a large font size
            while c.stringWidth(line_text, 'Courier', optimal_font_size_line) > max_text_width and optimal_font_size_line > 5:
                optimal_font_size_line -= 0.5

            style.fontSize = optimal_font_size_line
            p = Paragraph(line_text, style)
            width, height = p.wrapOn(c, max_text_width, max_text_height) # Allow wrapping within max_text_height
            current_y -= height # Move down for current line
            p.drawOn(c, x + 5, current_y + line_offset_y) # Draw from top down, apply offset
            current_y -= (0.05 * inch) # Increased line spacing

    elif label_type == 3:
        # Label 3 (Spine Label): Dewey, author (3 letters), year, inventory number (centered)
        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        # Calculate vertical position for centered text
        line_height = 12 # Approximate line height for font size 10
        total_text_height = len(lines) * line_height
        # Corrected vertical centering: y + (LABEL_HEIGHT - total_text_height) / 2
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8) # Adjust for baseline

        # Add giant spine label ID
        b_text = book_data.get('spine_label_id', 'B') # Use selected spine label ID
        # Calculate font size to make 'B' flush with label width
        # Start with a large font size and decrease until it fits
        b_font_size = 100 # Arbitrary large starting size
        while c.stringWidth(b_text, 'Helvetica-Bold', b_font_size) > LABEL_WIDTH and b_font_size > 10:
            b_font_size -= 1
        b_font_size *= 0.9 # Reduce size by 10%

        c.setFont('Helvetica-Bold', b_font_size)
        b_text_width = c.stringWidth(b_text, 'Helvetica-Bold', b_font_size)
        # Align 'B' flush with the right side of the label
        b_x = x + LABEL_WIDTH - b_text_width
        # Position 'B' vertically to be roughly centered, considering its height
        # This might need fine-tuning based on visual inspection
        b_y = y + (LABEL_HEIGHT - b_font_size * 0.8) / 2 + (0.5 * GRID_SPACING) # Approximate vertical centering, moved up by 0.5 blue box heights

        c.drawString(b_x, b_y, b_text)

        # Original text lines for Label 3
        c.setFont('Courier-Bold', 10)
        lines = [
            dewey_number,
            authors[:3].upper() if authors else '',
            str(publication_year),
            inventory_number
        ]
        
        # Calculate vertical position for centered text
        line_height = 12 # Approximate line height for font size 10
        total_text_height = len(lines) * line_height
        # Corrected vertical centering: y + (LABEL_HEIGHT - total_text_height) / 2
        start_y = y + (LABEL_HEIGHT - total_text_height) / 2 + total_text_height - (line_height * 0.8) # Adjust for baseline

        for i, line in enumerate(lines):
            text_width = c.stringWidth(line, 'Courier-Bold', 10)
            c.drawString(x + (LABEL_WIDTH - text_width) / 2, start_y - (i * line_height), line)

    elif label_type == 4:
        # Label 4: title, author, series, inventory number (text + barcode - 75% label)
        barcode_height = 7 * GRID_SPACING # Set barcode height to 7 blue box heights
        barcode_width = barcode_height * ((LABEL_WIDTH * 0.8 - 4 * GRID_SPACING) / (LABEL_HEIGHT * 0.6)) # Maintain aspect ratio
        barcode_x = x + LABEL_WIDTH - barcode_width # Flush with the right side of the label
        barcode_y = y # Flush with the bottom of the label

        barcode_image = generate_barcode(inventory_number)
        c.drawImage(barcode_image, barcode_x, barcode_y, width=barcode_width, height=barcode_height)
        

        # Text above barcode (Title, Author)
        text_above_barcode_lines = [
            f"{title} by {authors.split(',')[0] if authors else ''}", # Combined title and author
        ]
        # Define max_text_above_width before its usage
        max_text_above_width = LABEL_WIDTH - 10 # 10 units for margins
        max_text_above_height = (y + LABEL_HEIGHT) - (barcode_y + barcode_height) - 5 # Space from top of label to top of barcode

        optimal_font_size_above, text_block_height_above = _fit_text_to_box(c, text_above_barcode_lines, 'Courier', max_text_above_width, max_text_above_height, initial_font_size=10, alignment=TA_CENTER)
        
        styles = getSampleStyleSheet()
        style_above = styles['Normal']
        style_above.fontName = 'Courier'
        style_above.fontSize = optimal_font_size_above
        style_above.leading = optimal_font_size_above * 1.2 # Line spacing
        style_above.alignment = TA_CENTER # Center text horizontally

        current_y_above = y + LABEL_HEIGHT - 5 # Start near top of label
        for line in text_above_barcode_lines:
            p = Paragraph(line, style_above)
            width, height = p.wrapOn(c, max_text_above_width, max_text_above_height) # Wrap text within the box
            p.drawOn(c, x + 5, current_y_above - height) # Draw from top down
            current_y_above -= (height + (0.05 * inch)) # Move down for current line

        # Text to the left of barcode (Series)
        text_left_barcode_lines = []
        if series_name:
            text_left_barcode_lines.append(f"Vol. {series_number}" if series_number else series_name)

        max_text_left_width = barcode_x - (x + 5) - 5 # Space to the left of barcode
        max_text_left_height = barcode_height # Height of barcode area

        # Calculate optimal font size for the series number, reducing for longer numbers
        optimal_font_size_left = 10 # Initial font size
        if series_number and len(series_number) > 1:
            optimal_font_size_left -= (len(series_number) - 1) # Reduce by 1pt for each char after the first
        optimal_font_size_left = max(optimal_font_size_left, 5) # Ensure minimum font size

        optimal_font_size_left, text_block_height_left = _fit_text_to_box(c, text_left_barcode_lines, 'Courier', max_text_left_width, max_text_left_height, initial_font_size=optimal_font_size_left, alignment=TA_LEFT)

        style_left = styles['Normal']
        style_left.fontName = 'Courier'
        style_left.fontSize = optimal_font_size_left
        style_left.leading = optimal_font_size_left * 1.2 # Line spacing
        style_left.alignment = TA_LEFT

        # Position and rotate text to the left of the barcode
        if text_left_barcode_lines:
            c.saveState()
            # Translate to the bottom-left of the text area, then rotate
            text_origin_x = x + 5 # Start 5 units from left edge
            # Move down by 1 GRID_SPACING for every character after the first in series_number
            vertical_offset = 0
            if series_number and len(series_number) > 1:
                vertical_offset = (len(series_number) - 1) * GRID_SPACING
            text_origin_y = y + (LABEL_HEIGHT - text_block_height_left) / 2 + text_block_height_left - (0.1 * inch) - (3.5 * GRID_SPACING)
            
            c.translate(text_origin_x, text_origin_y)
            c.rotate(90) # Rotate 90 degrees counter-clockwise
            
            # Draw text after rotation and translation
            current_rotated_y = 0 # Relative to new origin
            for line in text_left_barcode_lines:
                p = Paragraph(line, style_left)
                # For rotated text, width and height parameters are swapped
                width, height = p.wrapOn(c, max_text_left_width, max_text_left_height) 
                current_rotated_y -= height # Move down for current line
                p.drawOn(c, 0, current_rotated_y) # Draw from new origin
                current_rotated_y -= (0.05 * inch) # Add line spacing
            c.restoreState()

def generate_pdf_sheet(book_data_list):
    """Generates a PDF with multiple sheets of Avery 5160 labels."""
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    # Limit to first 30 labels for debugging
    book_data_list_debug = book_data_list

    label_count = 0
    for book_data in book_data_list_debug:
        for label_type in range(1, 5): # Generate 4 labels for each book
            row = (label_count // LABELS_PER_SHEET_WIDTH) % LABELS_PER_SHEET_HEIGHT
            col = label_count % LABELS_PER_SHEET_WIDTH

            x_pos = LEFT_MARGIN + col * (LABEL_WIDTH + HORIZONTAL_SPACING)
            y_pos = PAGE_HEIGHT - TOP_MARGIN - (row + 1) * (LABEL_HEIGHT + VERTICAL_SPACING)

            

            # Draw dotted lines for label edges (perimeter of each label cell) with rounded corners
            c.setDash(1, 2) # Dotted line: 1 unit on, 2 units off
            c.setStrokeColorRGB(0, 0, 0) # Black color
            c.setLineWidth(0.5) # Line thickness
            c.roundRect(x_pos, y_pos, LABEL_WIDTH, LABEL_HEIGHT, 5) # 5 units for corner radius

            # Draw solid lines for buffer spaces (between labels, not overlapping dotted lines)
            c.setDash() # Solid line
            c.setStrokeColorRGB(0, 0, 0) # Black color
            c.setLineWidth(0.5) # Line thickness
            
            # Vertical solid lines in the horizontal spacing area
            if col < LABELS_PER_SHEET_WIDTH - 1:
                # Draw a single solid line in the middle of the horizontal spacing
                c.line(x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos, x_pos + LABEL_WIDTH + HORIZONTAL_SPACING / 2, y_pos + LABEL_HEIGHT)

            # Horizontal solid lines in the vertical spacing area
            if row < LABELS_PER_SHEET_HEIGHT - 1:
                # Draw a single solid line in the middle of the vertical spacing
                c.line(x_pos, y_pos - VERTICAL_SPACING / 2, x_pos + LABEL_WIDTH, y_pos - VERTICAL_SPACING / 2)

            create_label(c, x_pos, y_pos, book_data, label_type)
            label_count += 1

            if label_count % (LABELS_PER_SHEET_WIDTH * LABELS_PER_SHEET_HEIGHT) == 0:
                c.showPage() # Start a new page after a full sheet
                c.setFont('Courier', 8) # Reset font for new page

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


# Function to read book data from CSV
def read_book_data_from_csv(filepath):
    book_data = []
    with open(filepath, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert relevant fields to appropriate types
            row['publication_year'] = int(row['publication_year']) if row['publication_year'] else None
            row['series_number'] = row['series_number'] if row['series_number'] else None
            row['inventory_number'] = row['inventory_number'] if row['inventory_number'] else None
            book_data.append(row)
    return book_data