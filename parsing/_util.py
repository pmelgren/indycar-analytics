import fitz

def get_page_fills(page_drawings):
    return [
        {
            'rect': drawing['rect'],
            'fill': tuple(int(c * 255) for c in drawing['fill'])
        }
        for drawing in page_drawings
        if drawing['type'] == 'f' and drawing.get('fill')
    ]

def find_fill(span, filled_rects):
    span_rect = fitz.Rect(span["bbox"])
    y0, y1 = span_rect.y0, span_rect.y1

    # Limit to rectangles that vertically intersect the span
    for fr in filled_rects:
        draw_rect = fr['rect']
        if draw_rect.y1 >= y0 and draw_rect.y0 <= y1:
            if draw_rect.intersects(span_rect):
                return fr['fill']

    return None
