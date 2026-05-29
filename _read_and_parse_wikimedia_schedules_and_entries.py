import pywikibot
import wikitextparser as wtp
import pandas as pd
import time

site = pywikibot.Site('en','wikipedia')

schedule_dfs = []
entry_dfs = []
for y in range(1996,2027):
    if y <= 2002:
        if y == 1997:
            page = pywikibot.Page(site,'1996–97_Indy_Racing_League')
        else:
            page = pywikibot.Page(site,f'{y}_Indy_Racing_League')
    else:
        page = pywikibot.Page(site,f'{y}_IndyCar_Series')
        
    parsed = wtp.parse(page.text)
    
    for s in parsed.sections:
        if str(s.title).strip().lower() in ['confirmed entries','drivers and teams']:
            t = s.tables[0].data()
            df = pd.DataFrame(t[1:],columns = t[0])
            df['Season']=y
            df = df.drop(columns = df.columns[df.columns.duplicated()])
            
            if not df.empty:
                entry_dfs.append(df)
    
        elif str(s.title).strip().lower() == 'schedule       ':
    
            t = s.tables[-1].data()
            df = pd.DataFrame(t[1:],columns = t[0])
            df['Season'] = y
            df = df.drop(columns = df.columns[df.columns.duplicated()])
            
            if not df.empty:
                schedule_dfs.append(df)
                
    time.sleep(7)
    
df_sched = pd.concat(schedule_dfs)
df_sched.to_csv('Schedules_raw.csv')

df_entry = pd.concat(entry_dfs)
df_entry.to_csv('Entries_raw.csv')
