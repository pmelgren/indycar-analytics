from ._util import get_page_fills, find_fill
                   
def parse_section_results_file(doc):
    rows = []
    for p,page in enumerate(doc):
        try:
            ptext = page.get_text('dict')
            pfills = get_page_fills(page.get_drawings())
            
            # get column headers
            #columns = get_column_headers(ptext['blocks'][0],None)
            
            for b, block in enumerate(ptext['blocks']):
                if 'lines' in block.keys():
                    for l,line in enumerate(block['lines']):  
                        for span in line['spans']:
                            fill = find_fill(span,pfills)
                            rows.append({
                                #'column':find_column_for_span(span['bbox'], columns),
                                'data':span['text'].strip(),
                                'fill':fill,
                                'bbox':span.get('bbox'),
                                'page':p,
                                'block':b,
                                'line':l,
                                
                            })
        except Exception as e:
            print(f'Error parsing page {p}. Skipping for now. \n{e}')

                    
    return rows