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

def parse_file(doc,fills=True):
    rows = []
    for p,page in enumerate(doc):
        try:
            ptext = page.get_text('dict')
            if fills:
                pfills = get_page_fills(page.get_drawings())
            
            # get column headers
            #columns = get_column_headers(ptext['blocks'][0],None)
            
            for b, block in enumerate(ptext['blocks']):
                if 'lines' in block.keys():
                    for l,line in enumerate(block['lines']):  
                        for span in line['spans']:
                            row = {
                                #'column':find_column_for_span(span['bbox'], columns),
                                'data':span['text'].strip(),
                                'bbox':span.get('bbox'),
                                'page':p,
                                'block':b,
                                'line':l,
                            }
                            if fills:
                                spanfill = find_fill(span,pfills)
                                row['fill'] = spanfill
                                
                            rows.append(row)
        except Exception as e:
            print(f'Error parsing page {p}. Skipping for now. \n{e}')

    return rows