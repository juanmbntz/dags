import pandas as pd
from datetime import timedelta, datetime 

dates = ['20-Sep-24','94-May-12']

date_objects = [datetime.strptime(x, '%y-%b-%d').strftime('%Y-%m-%d') for x in dates]

print(date_objects)
