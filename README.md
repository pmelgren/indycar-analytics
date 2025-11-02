# INDYCAR-analytics
Data parsing and analytics functions for working with indycar data.

## Basic Usage
Begin by downloading INDYCAR session reports here: https://indycar.com/results and save each file under the appropriated sub-directory in the /pdfs folder of this repo.

To parse the data of a section times pdf you can run the following:
from parsing._section_results_main import parse_and_clean_section_results
parse_and_clean_section_results(['2017_Toyota_Grand_Prix_of_Long_Beach.pdf'])

Then load the data for analysis:
import pandas as pd
df = pd.read_parquet('cleandata/section results/2017_Toyota_Grand_Prix_of_Long_Beach.pq')

Run some example analysis:

# get laps completed for each driver in this race:
df.groupby('Driver')['Lap'].max().sort_values(ascending=False)

# get the fastest lap in the race
df = df.reset_index()
df.loc[df.loc[df.Section == 'Lap','Time'].idxmin()]

For more detailed analysis check out the notebooks folder:
 * Build Overtake Data.ipynb - notebook to determine when on-track overtakes occur and to detect trends in on-track overtakes.
 * Position Movement.ipynb - notebook to explore all position changes with the eventual goal of classifying all position changes and answering the question of how a driver went from one position to another.


