import numpy as np
import pandas as pd
import re
import camelot
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


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

    pattern = r'|'.join([x['Name'] for x in st])
    
    # fall back to regex pattern if st names don't appear in header text
    if not any(re.search(pattern, row['data']) for _, row in dfh.iterrows()):
        pattern = get_regex_pattern()
    
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
    
    # OLD FORMAT: T/S labels are in a separate block from the numeric data
    useblock_has_data = dfp.loc[dfp.block == useblock, 'data'].str.match(r'^\d+\.\d+$').any()
    if useblock not in dfb.block.unique() and useblock_has_data:
        dfd = dfp.loc[dfp.block == useblock].copy()
        dfb_ts = dfb.loc[dfb.data.isin(('T','S'))]
        
        # the T/S block has one fixed coordinate (section column) and one varying coordinate (lap axis)
        ts_axis = 'bbox_x0' if dfb_ts['bbox_x0'].std() > dfb_ts['bbox_y0'].std() else 'bbox_y0'
        sec_axis = 'bbox_y0' if ts_axis == 'bbox_x0' else 'bbox_x0'
        
        # cell_type from T/S block via the lap axis
        ts_map = dfb_ts.drop_duplicates(ts_axis).set_index(ts_axis)['data'].map({'T': 'Time', 'S': 'Speed'})
        dfd['cell_type'] = dfd[ts_axis].map(ts_map)
        dfd = dfd.dropna(subset=['cell_type'])
        
        # lap via nearest match on the lap axis from the separate single-entry lap blocks
        lap_blocks = dfp.loc[dfp['data'].str.match(r'^[0-9]+$') & (dfp.block != useblock)]
        lap_vals, lap_nums = lap_blocks[ts_axis].values, lap_blocks['data'].values
        dfd['Lap'] = dfd[ts_axis].apply(lambda x: lap_nums[np.argmin(np.abs(lap_vals - x))])
        
        # section headers are in the last block; sections run along sec_axis
        dfh = dfp.loc[dfp.block == dfp.block.max()].copy()
        pat = r'|'.join([x['Name'] for x in st])
        headers = {n: row[sec_axis] for _, row in dfh.iterrows() for n in re.findall(pat, row['data'])}
        dfd['Section'] = dfd[sec_axis].apply(lambda y: assign_column(y, headers))
    
    else:
        # NEW FORMAT: T and S appear within each lap block as row-level separators
        dfb.loc[dfb.data.str.match(r'^[0-9]{1,3}$'),'cell_type'] = 'Lap Number'
        dfb.loc[dfb.data == 'T','cell_type'] = 'Time'
        dfb.loc[dfb.data == 'S','cell_type'] = 'Speed'    
        dfb['cell_type'] = dfb.cell_type.ffill() # fill in time and speed 
        dfb.loc[dfb.data.isin(('T','S')),'cell_type'] = 'T/S'
        
        # add lap numbers
        dfb.loc[dfb.cell_type == 'Lap Number','Lap'] = dfb.loc[dfb.cell_type == 'Lap Number','data']
        dfb['Lap'] = dfb['Lap'].ffill()
        
        # only keep time and speed data
        dfd = dfb.loc[dfb.cell_type.isin(('Time','Speed'))].copy()
        
        # expand rows where the PDF parser merged multiple values into one cell
        multi = dfd.data.str.match(r'^\d+\.?\d* \d', na=False)
        if multi.any():
            expanded = []
            for _, row in dfd.loc[multi].iterrows():
                vals = row.data.split()
                n = len(vals)
                for i, v in enumerate(vals):
                    r = row.copy()
                    r['data'] = v
                    r['bbox_x0'] = row.bbox_x0 + i * (row.bbox_x1 - row.bbox_x0) / n
                    r['bbox_x1'] = row.bbox_x0 + (i+1) * (row.bbox_x1 - row.bbox_x0) / n
                    expanded.append(r)
            dfd = pd.concat([dfd.loc[~multi], pd.DataFrame(expanded)]).sort_values(['block','line'])
        
        # add headers
        l_to_r = is_left_to_right(dfb.loc[dfb.block == dfb.block.min()])  
        dfh = dfp.loc[dfp.block < dfb.block.min()].copy()
        headers = get_header_coords(dfh, st, dfd.bbox_x0.min(), dfd.bbox_x1.max(), l_to_r)
        dfd['Section'] = dfd[f'bbox_x{0 if l_to_r else 1}'].apply(lambda x: assign_column(x, headers))
    
    # add car and driver info
    dfd['Car'] = car
    dfd['Driver'] = driver
    
    # add flags based on nearest-color match to fill mapping
    df_fill = get_fill_mapping()
    fill_arr = np.array(list(df_fill['fill']))
    dfd = dfd.loc[dfd.fill.apply(bool)].copy()  # drop rows with no fill color
    dfd['fill'] = dfd.fill.apply(lambda x: tuple(x))
    dfd['Flag'] = dfd['fill'].apply(lambda x: df_fill['Flag'].iloc[np.argmin(((fill_arr - x)**2).sum(axis=1))])
    
    # clean and reshape data
    dfd['data'] = pd.to_numeric(dfd.data, errors='coerce')
    dfd = dfd.dropna(subset=['data'])  # drop rows with un-parseable values (PDF merge artifacts)
    dfd['Lap'] = dfd.Lap.astype(int)
    dfd = dfd.drop_duplicates(subset=['Car','Driver','Lap','Section','Flag','cell_type'])
    
    dffinal = (dfd
               .pivot(columns = ['cell_type'], values = ['data'],
                      index=['Car','Driver','Lap','Section','Flag'])
               .reset_index()
               )
    dffinal.columns = [l1 if l1 != '' else l0 for l0, l1 in dffinal.columns]
    return dffinal
        
def parse_results_pdf(file):
    tables = camelot.read_pdf(file, pages="1", flavor="stream")  
    for t in tables:
        if (t.df.iloc[:-1] == 'Pos').max().max(): # header can bleed into last row of another table
            col = 'Pos'
        elif (t.df.iloc[:-1] == 'Rank').max().max():
            col = 'Rank'
        elif (t.df.iloc[:-1] == 'P').max().max():
            col = 'P'
        elif (t.df.iloc[:-1] == 'Pos  SP').max().max():
            col = 'Pos  SP'
        else:
            continue
        df = t.df
        break
    
    # identify header and first column based on 'Pos' (always first col header)
    hdr = (df == col).any(axis=1).idxmax()
    firstcol = (df == col).any(axis=0).idxmax()
    
    if col == 'Pos  SP':
        df.iloc[hdr,firstcol] = 'SP'
        df.iloc[hdr,firstcol-1] = 'Pos'
        col = 'Pos'
        firstcol = firstcol-1
    
    # verify that the first row is Pos 1
    if df.iloc[hdr+1,firstcol] != '1':
        raise Exception("Error somewhere in parsing file {file}. You're on your own bro.")

    # find the last row        
    nextpos = 1
    
    for i in range(hdr+1, len(df.index)):
        current_val = df.iloc[i, firstcol]
        
        if current_val == str(nextpos):
            lastrow = i 
            nextpos += 1
        # skip a blank row where the next row continues the pattern
        elif current_val == '' and i + 1 < len(df.index) and df.iloc[i+1, firstcol] == str(nextpos):
            continue  # Skip blank row and keep going
        else:
            break
        
    ###### Find and Fix header names #####
    headernames = df.loc[hdr]
    
    # handle when word-wrapped header cols end up in 2 different rows
    if 'Down' in list(df.loc[hdr]):
        if ('Laps' in list(df.loc[hdr-1])) | ('Time' in list(df.loc[hdr-1])):
            headernames = df.apply(lambda x: ' '.join([x[hdr-1],x[hdr]]).strip(),axis=0)
    
    # prepare output df
    dfret = df.iloc[hdr+1:lastrow+1,firstcol:]
    dfret.columns = headernames
    dfret = dfret.loc[dfret[col] != ''].copy()
    
    return dfret
    
def clean_results_df(df):
    
    df[df ==''] = np.nan
    df.dropna(axis=1, how='all', inplace=True)
    
    # standardize column names
    df.columns = [x.replace('Driver Name','Driver').replace('\n',' ').replace('  ',' ')
                  for x in df.columns]
    df.columns = df.columns.str.replace('^P$','Pos',regex=True) 
    
    # car and driver header name can get combined
    if 'Car Driver' in df.columns:
        colnames = list(df.columns)
        i = colnames.index('Car Driver')
    
        if colnames[i + 1] == '':
            colnames[i] = 'Car'
            colnames[i + 1] = 'Driver'
            df.columns = colnames
        else:
            colnames[i] = 'Car'
            df.columns = colnames
            df['Driver'] = ''
            
    # fix Camelot bleed between Car / Driver cols
    fix = df['Car'].astype(str).str.extract(r'^(?P<Car>\d+[T]{0,1})\s+(?P<Driver>.+)$')
    fix2 = df['Driver'].astype(str).str.extract(r'^(?P<Car>\d+[T]{0,1})\s+(?P<Driver>.+)$')
    
    # Coalesce the extracted values, prioritizing fix, then fix2, then original
    df['Driver'] = fix['Driver'].fillna(fix2['Driver']).fillna(df['Driver'])
    df['Car'] = fix['Car'].fillna(fix2['Car']).fillna(df['Car']).infer_objects(copy=False)
    
    # standardize CAET column
    if 'C/E/T' in df.columns:
        df.rename(columns = {'C/E/T':'C/A/E/T'}, inplace=True)
    elif 'C/A/E/T' not in df.columns:
        df['C/A/E/T'] = ''
        
    if 'kit/Engine' in df.columns:
        df['C/A/E/T'] = df['kit/Engine'].apply(lambda x: 'D/'+x[0]+'/'+x[0]+'/F')
        df.drop(columns = 'kit/Engine',inplace=True)

    # fix camelot bleed between driver and c/E/T columns 
    caet_fix = df['Driver'].str.extract(r'^(?P<Driver>.+?)\s+(?P<CET>D/[^ ]+)$')
    df['Driver'] = caet_fix['Driver'].combine_first(df['Driver'])
    df['C/A/E/T'] = df['C/A/E/T'].combine_first(caet_fix['CET'])
    
    df['C/A/E/T'] = df['C/A/E/T'].apply(lambda x: x[:2]+'-/'+x[-3:] if len(x) == 5 else x)
            
    # fix issues with Running / Reason Out column
    df.rename(columns={'Running/Reason Out':'Running / Reason Out'}, inplace=True)
    
    # fix when Avg Speed and Running/Reason Out get combined
    if 'Running / Reason Out' in df.columns:
        pattern = r'^(\d+(?:\.\d+)?)\s+(.+)$'
        matches_rro = df['Running / Reason Out'].str.extract(pattern)
        matches_as = df['Avg Speed'].str.extract(pattern)
        matches = matches_rro.combine_first(matches_as)
    
        df.loc[matches[0].notna(), 'Avg Speed'] = matches[0].astype(float)
        df.loc[matches[1].notna(), 'Running / Reason Out'] = matches[1]
    
        # standardize values
        df.loc[df['Running / Reason Out'].isin(['Off course','Off-Course']),'Running / Reason Out'] = 'Off Course'
        df.loc[df['Running / Reason Out'] == 'DSQ','Running / Reason Out'] = 'DQ'
        
    # clean up numeric dtypes
    for col in ['Pos','SP','Lap','Laps Down','Pit Stops','Pts',
                'Total Pts','Standings','Rank','Best Lap','Total Laps',
                'Avg Speed','Speed']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
    
    # drop any column which is all blank
    df = df.drop(columns = df.columns[(df == '').all(axis=0)])
    
    return df    