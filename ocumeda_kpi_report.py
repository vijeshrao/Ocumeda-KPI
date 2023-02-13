import requests
import pandas as pd
from math import ceil
from tqdm import tqdm
import random

url = 'https://atlantis-staging1.prosrv.eu'
AUTH = {
	'email' : '',
	'password' : ''
}


def login(url=url, auth=AUTH):
    # Already added when you pass json= but not when you pass data=
    # 'Content-Type': 'application/json',
    headers = {'accept': '*/*'}
    endpoint = '/api/auth/login'
    response = requests.post(
        url + endpoint, params={'refresh': '5'}, headers=headers, json=auth)
    if response.status_code not in [200, 201]:
        raise ValueError(response.status_code)
    access_token = response.json()['access_token']
    refresh_token = response.json()['refresh_token']
    return access_token, refresh_token

ACCESS_TOKEN, _ = login()

def get_entity(kind, id, url=url):
    assert kind in ['exam', 'company', 'store',
                    'doctor', 'patient', 'optician', 'doctor']
    endpoint = f'/api/{kind}/{id}'
    headers = {'Authorization': 'bearer ' + ACCESS_TOKEN}
    response = requests.get(url + endpoint, headers=headers)
    entity = response.json()
    return entity

def get_decade(age):
    if age < 30:
        decade = '0-29'
    elif age >= 30 and age < 40:
        decade = '30-39'
    elif age >= 40 and age < 50:
        decade = '40-49'
    elif age >= 50 and age < 60:
        decade = '50-59'
    elif age >= 60 and age < 70:
        decade = '60-69'
    elif age >= 70 and age < 80:
        decade = '70-79'
    elif age >= 80 and age < 90:
        decade = '80-89'
    elif age >= 90:
        decade = '90+'
    else:
        raise ValueError(f'invalid age {age}')
    return decade

def get_bulk(kind, url=url, dataframe=False):
    assert kind in ['exam', 'company', 'store',
                    'doctor', 'patient', 'optician', 'doctor', 'report', 'audit-log']
    endpoint = f'/api/{kind}'
    headers = {'Authorization': 'bearer ' + ACCESS_TOKEN}
    response = requests.get(url + endpoint, params={
        'take': '100', 'skip': '0', 'orderBy': 'id', 'order': 'desc'}, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.status_code)
    rows_per_call = 100
    n_calls = ceil(response.json()['count'] / rows_per_call)
    data = []
    for i in tqdm(range(n_calls)):
        skip = i * rows_per_call
        response = requests.get(url + endpoint, params={
            'take': str(rows_per_call), 'skip': str(skip), 'orderBy': 'id', 'order': 'desc'}, headers=headers)
        if response.status_code not in [200, 201]:
            raise ValueError(response.status_code)
        data += response.json()['data']
    if dataframe:
        df = pd.json_normalize(data)
        cols = ['Store.name', 'createdAt', 'User.birthdate', 'User.Country.name_de', 'findings']
        return df[cols]
    else:
        return data

def analyse_data(df):
    # temporary solution since there are 'findings' in the demo data
    df['findings'] = df['findings'].apply(lambda x: [random.choice(['Normal', 'Suspicious', 'Severe'])])
    df['age'] = (pd.to_datetime(df['createdAt']) -
                pd.to_datetime(df['User.birthdate'])).astype('<m8[Y]')
    df['decade'] = df['age'].apply(get_decade)
    df['Zeitskala'] = pd.to_datetime(df['createdAt']).dt.isocalendar().week
    df['Jahr'] = pd.to_datetime(df['createdAt']).dt.isocalendar().year

    df_decade_binary = pd.get_dummies(df['decade'])
    df_findings_binary = pd.get_dummies(df['findings'].apply(lambda x: x[0]))
    df = pd.concat([df, df_findings_binary, df_decade_binary], axis=1)

    df.drop(columns=['decade', 'findings'], inplace=True)
    df = df.groupby(['Store.name', 'User.Country.name_de', 'Zeitskala', 'Jahr']).sum()
    return df

df = get_bulk(kind='exam', dataframe=True)
df = analyse_data(df)
df.to_csv('screening_kpi.csv')
