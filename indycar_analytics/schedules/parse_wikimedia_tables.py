import pywikibot
import wikitextparser as wtp
import pandas as pd
import time

INDYNXT = True

site = pywikibot.Site('en','wikipedia')

schedule_dfs = []
entry_dfs = []
year_start = 2002 if INDYNXT else 1996
year_end = 2027

for y in range(year_start,year_end):
    stry= '1996–97' if y == 1997 else str(y)
    if INDYNXT:
        if y <= 2005:
            page = pywikibot.Page(site,f'{y}_Infiniti_Pro_Series')
        elif y <= 2007:
            page = pywikibot.Page(site,f'{y}_Indy_Pro_Series')
        elif y <= 2022:
            page = pywikibot.Page(site,f'{y}_Indy_Lights')
        else:
            page = pywikibot.Page(site,f'{y}_Indy_NXT')
    else:
        if y <= 2002:
            page = pywikibot.Page(site,f'{stry}_Indy_Racing_League')
        else:
            page = pywikibot.Page(site,f'{stry}_IndyCar_Series')
        
    parsed = wtp.parse(page.text)
    
    for s in parsed.sections:
        if str(s.title).strip().lower() in ['confirmed entries','drivers and teams','teams and drivers','team and driver chart']:
            print(y)
            tableindex = 1 if (INDYNXT and y >= 2024) else 0
            t = s.tables[tableindex].data()
            df = pd.DataFrame(t[1:],columns = t[0])
            df['Season']=str(y)
            df = df.drop(columns = df.columns[df.columns.duplicated()])
            
            if not df.empty:
                entry_dfs.append(df)
    
        elif str(s.title).strip().lower() == 'schedule':
            tableindex = 1 if (INDYNXT and y >= 2024) else (0 if (INDYNXT or y <= 2004) else 1)
            t = s.tables[tableindex].data()

            df = pd.DataFrame(t[1:],columns = t[0])
            if y == 2006 and not INDYNXT:
                df = df[df['Round'].astype(str).str.strip().str.isdigit()]
            df['Season'] = stry
            df = df.drop(columns = df.columns[df.columns.duplicated()])
            
            print(f'DF has {len(df.index)} Rows.')
            if not df.empty:
                schedule_dfs.append(df)
                
    time.sleep(7)
    
df_sched = pd.concat(schedule_dfs)
schedule_filename = 'Schedules_raw_IndyNXT.csv' if INDYNXT else 'Schedules_raw.csv'
df_sched.to_csv(schedule_filename)

df_entry = pd.concat(entry_dfs)
entries_filename = 'Entries_raw_IndyNXT.csv' if INDYNXT else 'Entries_raw.csv'
df_entry.to_csv(entries_filename)
