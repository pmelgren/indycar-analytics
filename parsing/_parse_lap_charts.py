import fitz  # PyMuPDF
import pandas as pd
import re
import os
import json
from _util import get_page_fills

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

def parse_lap_chart_file(doc):    

    dfs = []
    drivers = []
    laps = []
    
    # each block represents a given position in the race
    for p,page in enumerate(doc):
        ptext = page.get_text('dict')
        fills = get_page_fills(page.get_drawings())
        
        for b in ptext['blocks'][1:]:
            lcars = []
            fills = []
            
            # skip if no lines
            if not b.get('lines'):
                continue
            
            # skip if block doesn't start with a valid driver name
            driver = b['lines'][0]['spans'][0].get('text')
            if not re.match(r'\d{1,2} - [A-Za-z|\']+, [A-Za-z|\s]+ \(\d{1,2}\)',driver):
                continue
            
            if p == 0:
                # add driver name to list of drivers
                drivers.append(driver)
        
            # get lap chart car positions and colors
            position = b['lines'][1]['spans'][0].get('text') # get track position
            for l in b['lines'][2:]:
                for s in l['spans']:
                    
                    # get car number in this position by lap
                    cars = s.get('text').strip().split(' ')
                    lcars += cars
                    # add fill color if any
                    fills += [find_fill(s, fills)]*len(cars) 
                    
            dfs.append(pd.DataFrame({'Position':[position]*len(lcars),
                                     'Car': lcars,
                                     'Color': fills,
                                     'Page': [p]*len(lcars)}))
            
    
        start_lap_counter = False
        for l in ['lines']:
            for s in l['spans']:
                
                if not start_lap_counter:
                    if re.match(r'Drivers in Race:',s.get('text').strip()):
                        start_lap_counter = True
                else:
                    laps.append({'Lap':s.get('text').strip(),
                                'Fill':find_fill(s, fills)})
    
                    
if __name__ == '_3_main__':
    
    for file in os.listdir("pdfs/section results/"):
        jsonfile = file.replace('.pdf','.json')
        if jsonfile not in os.listdir(os.path.join('parsedraw','lap charts')):
            print(f'Parsing {file}')
            doc = fitz.open(os.path.join('pdfs','lap charts',file))
            allrows = parse_lap_chart_file(doc)
    
            with open(os.path.join('parsedraw','lap charts',jsonfile), "w") as f:
                json.dump(allrows, f)
