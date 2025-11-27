import numpy as np
import pandas as pd
import re
import camelot

def get_lap_chart_fill_mapping():
    {'Pit':(65, 105, 224) ,
    'Fastest Lap':(146, 111, 219),
    'Lapped':(210, 210, 210)}


def get_y0_bbox_coord(block_df):
    # determine page orientation
    x0std = block_df['bbox'].apply(lambda x: x[0]).std()
    x1std = block_df['bbox'].apply(lambda x: x[1]).std()
    return 1 if x1std < x0std else 0

def is_left_to_right(dfp):
    return dfp.loc[dfp.line==1,'bbox_x0'].median() < dfp.loc[dfp.line==2,'bbox_x0'].median()

def assign_column(x, headers):

    header_keys = list(headers.keys())
    header_values = np.array([headers[h] for h in header_keys])

    idx = np.argmin(np.abs(header_values - x))
    return header_keys[idx]

def get_regex_pattern():
    section = r'(?:Turn[s]? |I|SF|FS|PI|PO|Lap|BackStretch)\d{0,2}[Aa]?(?:[\/\-]?\d{1,2}[Aa]?)?(?: Entry| Exit)?'
    
    full_pattern = rf'{section}(?:\-| to ){section}|{section}'
    
    #pattern = re.compile(full_pattern, re.VERBOSE)
        
    return full_pattern

def get_legend_page(df):
    for p in range(df.page.max(),-1,-1):
        datalist = list(df.loc[df.page == p,'data'])
        if ('Name' in datalist) and ('Length' in datalist):
            return p
        
def parse_sections_table(df):
    lp = get_legend_page(df)
    
    st = []
    for i in df.loc[df.page >= lp,'data'].index:
        if re.match(r'\d.\d+ miles',df.loc[i,'data']):
            st.append({'Name':df.loc[i-1,'data'],'Data':df.loc[i,'data']})

    return st

def get_header_coords(dfh, st, dft_x0, dft_x1, l_to_r):

    #pattern = get_regex_pattern()
    pattern = r'|'.join([x['Name'] for x in st])
    
    headers = {}
    for i in dfh.index:
        
        rawtxt=dfh.loc[i,'data']
        txt=re.sub(r'Lap\s+T\/S','',rawtxt) # remove first Lap
        
        header_names = re.findall(pattern,txt)
        
        if l_to_r:
            left_edge = dft_x0 if 'T/S' in rawtxt else dfh.loc[i,'bbox_x0'] # handle if txt inclues Lap T/S
            right_edge = dfh.loc[i,'bbox_x1']
        else:
            left_edge = dft_x1 if 'T/S' in rawtxt else dfh.loc[i,'bbox_x1']
            right_edge = dfh.loc[i,'bbox_x0']  
        
        for i, h in enumerate(header_names):
            
            if h == 'BackStretch' and 'BackStretch' in headers.keys():
                h = 'BackStretch B'
            headers[h] = left_edge + (i*((right_edge-left_edge)/len(header_names)))
            
    return headers

def get_block_laps(dfb):
    dfl = dfb.loc[dfb.cell_type == 'Lap Number',['data','block']]
    dfl2 = dfl.copy()
    dfl2['block'] = dfl2['block']+1
    return pd.concat([dfl,dfl2]).set_index('block').rename(columns = {'data':'Lap'})

def get_fill_mapping():
    fmap = {'Green':(144, 237, 144),
            'Gray':(210, 210, 210),
            'Yellow':(255, 255, 0)}
    
    return pd.DataFrame({'Flag':list(fmap.keys()),'fill':list(fmap.values())})


def clean_section_results_page(dfp, st):
    
    # get car and driver if its a section page
    try:
        car, driver = dfp['data'].str.extract(r"Section Data for Car (\d{1,3}) - (.+)$").dropna().iloc[0]
    except IndexError:
        page = dfp.page.iloc[0]
        print(f'Skipping page {page} as it does not contain Section Data.')
        return pd.DataFrame({}) #empty df
    
    useblock = dfp.groupby('block')[['page']].count().idxmax().iloc[0]
    iy0 = get_y0_bbox_coord(dfp.loc[dfp.block == useblock])
    
    dfp['bbox_y0'] = dfp.bbox.apply(lambda x: int(x[iy0]))
    dfp['bbox_x0'] = dfp.bbox.apply(lambda x: int(x[1-iy0]))
    dfp['bbox_y1'] = dfp.bbox.apply(lambda x: int(x[iy0+2]))
    dfp['bbox_x1'] = dfp.bbox.apply(lambda x: int(x[3-iy0]))    
    
    # get just the blocks with data (no header or page data)
    dfb = dfp.loc[dfp.block.isin(dfp.loc[dfp.data.isin(('T','S')),'block'])]
    dfb = dfb.sort_values(['block','line']).copy()
    
    # assign cell types
    dfb.loc[(dfb.line==0) & (dfb.data.str.match(r'[0-9]{1,3}')),'cell_type'] = 'Lap Number'
    dfb.loc[dfb.data == 'T','cell_type'] = 'Time'
    dfb.loc[dfb.data == 'S','cell_type'] = 'Speed'    
    dfb['cell_type'] = dfb.cell_type.ffill() # fill in time and speed 
    dfb.loc[dfb.data.isin(('T','S')),'cell_type'] = 'T/S'
    
    # add lap numbers
    dfb.loc[dfb.cell_type == 'Lap Number','Lap'] = dfb.loc[dfb.cell_type == 'Lap Number','data']
    dfb['Lap'] = dfb['Lap'].ffill()
    
    # only keep time and speed data
    dfd = dfb.loc[dfb.cell_type.isin(('Time','Speed'))].copy()
    
    # add headers
    l_to_r = is_left_to_right(dfb.loc[dfb.block == dfb.block.min()])  
    dfh = dfp.loc[dfp.block < dfb.block.min()].copy()
    headers = get_header_coords(dfh, st, 
                                dfd.bbox_x0.min(), dfd.bbox_x1.max(), l_to_r)
    
    dfd['Section'] = dfd[f'bbox_x{0 if l_to_r else 1}'].apply(lambda x: assign_column(x,headers))
    
    # add car and driver info
    dfd['Car'] = car
    dfd['Driver'] = driver
    
    # add flags based on fills
    df_fill = get_fill_mapping()
    dfd['fill'] = dfd.fill.apply(lambda x: tuple(x))
    dfd = dfd.merge(df_fill,on='fill')
    
    # clean and reshape data
    dfd['data'] = dfd.data.astype(float)
    dfd['Lap'] = dfd.Lap.astype(int)
    
    dffinal = (dfd
               .pivot(columns = ['cell_type'], values = ['data'],
                      index=['Car','Driver','Lap','Section','Flag'])
               .reset_index()
               )
    dffinal.columns = [l1 if l1 != '' else l0 for l0, l1 in dffinal.columns]
    return dffinal
        
def clean_results_pdf(file):
    tables = camelot.read_pdf(file, pages="1", flavor="stream")  
    for t in tables:
        if (t.df == 'Pos').max().max():
            df = t.df
            break
    
    # identify header and first column based on 'Pos' (always first col header)
    hdr = (df == 'Pos').any(axis=1).idxmax()
    firstcol = (df == 'Pos').any(axis=0).idxmax()
    
    headernames = df.loc[hdr]
    
    # handle when word-wrapped header cols end up in 2 different rows
    if 'Down' in list(df.loc[hdr]):
        if ('Laps' in list(df.loc[hdr-1])) | ('Time' in list(df.loc[hdr-1])):
            headernames = df.apply(lambda x: ' '.join([x[hdr-1],x[hdr]]).strip(),axis=0)
            
    if 'Car Driver' in headernames.values:
        cdidx = headernames[headernames == 'Car Driver'].idxmax()
        if headernames[cdidx+1] == '':
            headernames[cdidx] = 'Car'
            headernames[cdidx] = 'Driver'
            
    # find the last row
    if df.iloc[hdr+1,firstcol] != '1':
        raise Exception("Error somewhere in parsing file {file}. You're on your own bro.")
        
    nextpos = 1
    for i in range(hdr+1,len(df.index)):
        if df.iloc[i,firstcol] != str(nextpos):
            lastrow = i
            break
        else:
            nextpos+=1
    else:
        lastrow = df.index.max()
            
    dfret = df.iloc[hdr+1:lastrow+1,firstcol:]
    dfret.columns = headernames
    
    return dfret    