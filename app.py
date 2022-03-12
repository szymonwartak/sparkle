import re
import boto3
import csv
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

# st.button('asdf')

df = pd.DataFrame()

s3 = boto3.resource('s3')
bucket = s3.Bucket('ark-scrapes')
for object in bucket.objects.filter(Prefix="ARKK/", Delimiter="/"):
  if re.match('ARKK/\d+', object.key):
    # print(f'READING {object.key}')
    s1 = object.get()['Body'].read().decode('utf-8')
    lines = s1.splitlines()
    reader = csv.DictReader(lines)
    parsed_csv = pd.DataFrame(list(reader))
    df = pd.concat([df, parsed_csv[:-1]])

df_full = df
# df = df_full
df = df[['date', 'company', 'weight (%)']].rename(columns={'weight (%)': 'weight'})
df['date'] = pd.to_datetime(df['date'])
df['weight'] = df['weight'].str.rstrip('%').astype('float') / 100.0

latest_date = df['date'].max()
df1 = None
weeks_interval = 1
offset = 0
while df1 is None or df1.size == 0:
  dt1 = latest_date - timedelta(weeks=weeks_interval, days=-offset)
  df1 = df[df['date'] == dt1]
  offset += 1

df2 = df[df['date'] == latest_date]
print(f'range: {dt1} --> {latest_date}')

removed = set(df1.company).difference(set(df2.company))
added = set(df2.company).difference(set(df1.company))
same = set(df2.company).intersection(set(df1.company))
df1_same = df1[df1.company.isin(same)]
df2_same = df2[df2.company.isin(same)]

df_merged = pd.merge(df1[df1.company.isin(same)], df2[df2.company.isin(same)], on='company')

# adjust calc for changes in share price
df_merged['weight_change'] = df_merged.weight_y - df_merged.weight_x

st.markdown('''
# ARKK portfolio changes
''')
st.write(df_merged[['company', 'weight_change']].sort_values(by=['weight_change'], ascending=False))