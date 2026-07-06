import fitz  # PyMuPDF
import pandas as pd
import re
import os
import json
from indycar_analytics.util.pdf_utils import get_page_fills

def find_fill(span, filled_rects):
    span_rect = fitz.Rect(span["bbox"])
    y0, y1 = span_rect.y0, span_rect.y1

    # Limit to rectangles that vertically intersect the span
    for fr in filled_rects:
        draw_rect = fr['rect']
        if draw_rect.y1 >= y0 and draw_rect.y0 <= y1:
            if draw_rect.intersects(span_rect):
                fill = fr['fill']
                return f"{fill[0]},{fill[1]},{fill[2]}" if fill else None

    return None

def _span_tokens(line):
    """Return all whitespace-split tokens from a PDF line's spans."""
    return [t for s in line.get('spans', []) for t in s.get('text', '').split() if t]

def _is_car_row(block):
    """True if the block contains at least one line of all-digit tokens (car numbers)."""
    return any(
        all(t.isdigit() for t in _span_tokens(l))
        for l in block['lines'] if _span_tokens(l)
    )

def _is_uniform_block(block):
    """True if every line in the block has the same single token (position-label column in 2013 PDFs)."""
    all_tokens = [t for l in block['lines'] for t in _span_tokens(l)]
    return len(all_tokens) > 0 and len(set(all_tokens)) == 1

def parse_lap_chart_file(doc):
    dfs = []

    for page in doc:
        ptext = page.get_text('dict')
        page_fills = get_page_fills(page.get_drawings())

        # Step 1: find the block containing 'Drivers in Race:' — this holds the lap numbers.
        lap_block = next(
            (b for b in ptext['blocks']
             if b.get('lines') and any(
                 'Drivers in Race' in s.get('text', '')
                 for l in b['lines'] for s in l.get('spans', []))),
            None
        )
        if not lap_block:
            continue

        # Collect lap entries: (number_text, fill). The header line is either first (2014+)
        # or last (2013). Collect everything before/after it, then normalise to lap-1-first.
        before, after = [], []
        seen_header = False
        for l in lap_block['lines']:
            for s in l.get('spans', []):
                text = s.get('text', '').strip()
                if not text:
                    continue
                if 'Drivers in Race' in text:
                    seen_header = True
                    continue
                fill = find_fill(s, page_fills)
                for token in text.split():
                    (after if seen_header else before).append((token, fill))

        # If laps follow the header use them directly; if they precede it they are
        # in descending order (right-to-left in the PDF), so reverse.
        page_laps = after if after else list(reversed(before))
        lap_numbers = [lap for lap, _ in page_laps]
        lap_fills   = [fill for _, fill in page_laps]

        # Step 2: collect data-row blocks — any multi-line block where every line
        # contains only digit tokens (car numbers). Sort by y to assign position rank.
        # Explicitly exclude the lap_block so the header is never treated as a position row.
        lap_block_y = lap_block['bbox'][1]
        data_blocks = sorted(
            [b for b in ptext['blocks']
             if b is not lap_block and b.get('lines') and len(b['lines']) > 1
             and _is_car_row(b) and not _is_uniform_block(b)
             and b['bbox'][1] > lap_block_y],
            key=lambda b: b['bbox'][1]
        )

        # Build a y -> position lookup from standalone single-line position-label blocks
        # (single digit token at x ~182, same x as the position column).
        POS_LABEL_X = 182.8
        POS_LABEL_TOL = 8
        pos_label_by_y = {}
        for bl in ptext['blocks']:
            blines = bl.get('lines', [])
            if len(blines) != 1:
                continue
            tokens = _span_tokens(blines[0])
            spans = blines[0].get('spans', [])
            if (len(tokens) == 1 and tokens[0].isdigit() and spans
                    and abs(spans[0]['bbox'][0] - POS_LABEL_X) < POS_LABEL_TOL):
                pos_label_by_y[round(bl['bbox'][1], 1)] = int(tokens[0])

        def _is_pos_label_line(line):
            """True if this line is a position-label (single digit at x ~182)."""
            spans = line.get('spans', [])
            tokens = _span_tokens(line)
            return (len(tokens) == 1 and tokens[0].isdigit() and spans
                    and abs(spans[0]['bbox'][0] - POS_LABEL_X) < POS_LABEL_TOL)

        for seq_rank, b in enumerate(data_blocks, start=1):
            # Determine position: prefer explicit label over sequential rank.
            # 1. Check lines inside the block for an embedded position label.
            embedded_pos = next(
                (int(_span_tokens(l)[0]) for l in b['lines'] if _is_pos_label_line(l)),
                None
            )
            # 2. Check standalone label block at same y.
            y_key = round(b['bbox'][1], 1)
            position = embedded_pos or pos_label_by_y.get(y_key) or seq_rank

            # In 2013 lines run right-to-left (last lap first), so reverse.
            # Detect direction by comparing x of first vs last non-empty line.
            def line_x(l):
                spans = [s for s in l.get('spans', []) if s.get('text', '').strip()]
                return spans[0]['bbox'][0] if spans else None

            first_x = next((line_x(l) for l in b['lines'] if line_x(l) is not None), None)
            last_x  = next((line_x(l) for l in reversed(b['lines']) if line_x(l) is not None), None)
            lines = list(reversed(b['lines'])) if (first_x and last_x and first_x > last_x) else b['lines']

            lcars, fills = [], []
            for l in lines:
                if _is_pos_label_line(l):
                    continue  # skip position-label lines — not car data
                tokens = _span_tokens(l)
                if not tokens or not all(t.isdigit() for t in tokens):
                    continue  # skip driver-name lines
                for s in l.get('spans', []):
                    cars = s.get('text', '').split()
                    lcars += cars
                    fills += [find_fill(s, page_fills)] * len(cars)

            row_count = len(lcars)
            if not row_count:
                continue

            lap_values      = lap_numbers[:row_count] + [None] * max(0, row_count - len(lap_numbers))
            lap_fill_values = lap_fills[:row_count]   + [None] * max(0, row_count - len(lap_fills))

            dfs.append(pd.DataFrame({
                'Position': [position] * row_count,
                'Car':      lcars,
                'Color':    fills,
                'lap':      lap_values,
                'lap_fill': lap_fill_values,
            }))

    if dfs:
        return pd.concat(dfs).reset_index(drop=True)
    return pd.DataFrame()