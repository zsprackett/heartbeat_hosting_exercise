#!/usr/bin/env python

import csv
import pprint
import redis
from dateutil.parser import parse
from ipwhois import IPWhois

# https://support.sugarcrm.com/Knowledge_Base/Email/Configuring_Your_SMTP_Server_to_Work_With_Sugar_Cloud/
sugarcloud = {
    '52.64.34.40': 'SugarCloud AU',
    '52.64.128.40': 'SugarCloud AU',
    '35.182.68.172': 'SugarCloud CA',
    '35.183.168.13': 'SugarCloud CA',
    '52.59.58.195': 'SugarCloud DE',
    '3.122.45.38': 'SugarCloud DE',
    '52.58.98.157': 'SugarCloud DE',
    '52.59.58.195': 'SugarCloud EU',
    '3.122.45.38': 'SugarCloud EU',
    '52.58.98.157': 'SugarCloud EU',
    '52.16.253.121': 'SugarCloud EU',
    '52.18.148.31': 'SugarCloud EU',
    '52.221.83.215': 'SugarCloud SG',
    '54.151.168.230': 'SugarCloud SG',
    '54.169.3.37': 'SugarCloud SG',
    '18.133.81.250': 'SugarCloud UK',
    '3.10.88.56': 'SugarCloud UK',
    '35.176.133.186': 'SugarCloud UK',
    '52.10.234.30': 'SugarCloud US',
    '52.89.22.243': 'SugarCloud US'
}

public_cloud = {
    'AMAZON-02 US': 'Amazon Cloud',
    'AMAZON-AES US': 'Amazon Cloud',
    'MICROSOFT-CORP-MSN-AS-BLOCK US': 'Microsoft Cloud',
    'GOOGLE-CLOUD-PLATFORM US': 'Google Cloud',
    'SOFTLAYER US': 'IBM Cloud',
    'DIGITALOCEAN-ASN US': 'Digital Ocean Cloud'
}

accounts = {}

with open('2023-03-07_Raw_Heartbeat_Data.csv', newline='', encoding='utf-8-sig') as csvfile:
    rows  = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for row in rows:
        if not row['Account_ID'] in accounts:
            accounts[row['Account_ID']] = []
        accounts[row['Account_ID']].append(row)

rows = []
# select the single best heartbeat for each account
for account_id in accounts:
    #print("id:%s count:%d" % (account_id, len(accounts[account_id])))
    hb = accounts[account_id][0]
    if (len(accounts[account_id])) > 1:
        user_count = 0
        hb_date = None
        is_cloud = False
        for heartbeat in accounts[account_id]:
            if not is_cloud and heartbeat['Cloud_Instance_Name'] != '':
                is_cloud = True

            if is_cloud and heartbeat['Cloud_Instance_Name'] == '':
                #print("Skipping non-cloud heartbeat")
                continue

            if int(heartbeat['Logged-in Users (Last 30 Days)']) > user_count:
                #print("Found more users")
                hb = heartbeat
                user_count = int(heartbeat['Logged-in Users (Last 30 Days)'])
                hb_date = parse(heartbeat['Last Update'])
            elif int(heartbeat['Logged-in Users (Last 30 Days)']) == user_count:
                if hb_date < parse(heartbeat['Last Update']):
                    #print("Found newer heartbeat")
                    hb = heartbeat
                    hb_date = parse(heartbeat['Last Update'])
    rows.append(hb)

# for each row, lookup the hostname
r = redis.Redis(host='localhost', port=6379, db=0)
for row in rows:
    asn = r.get(row['SOAP Client IP'])
    if not asn:
         print("Looking up %s" % row['SOAP Client IP'])
         try:
             obj = IPWhois(row['SOAP Client IP'])
             w = obj.lookup_rdap(depth=1)
             asn = w['asn_description'].replace(',','')
         except:
             asn = ''
         r.set(row['SOAP Client IP'], asn)
    row['ISP'] = asn.decode('utf-8')

    row['Hosting'] = ''

    # is this a SugarCloud instance?
    if row['SOAP Client IP'] in sugarcloud:
        row['Hosting'] = sugarcloud[row['SOAP Client IP']]
    elif row['Cloud_Instance_Name']:
        row['Hosting'] = 'SugarCloud Unknown'
    # is this a public cloud instance?
    elif row['ISP'] in public_cloud:
        row['Hosting'] = public_cloud[row['ISP']]

# sort by partner id
partners = {}
for row in rows:
    if not row['Partner_ID'] in partners:
         partners[row['Partner_ID']] = []
    partners[row['Partner_ID']].append(row)

for partner in partners:
    hosting_isps = {}
    for row in partners[partner]:
        # ignore direct instances
        if row['Partner_ID'] == '':
            continue

        # ignore instances that already have hosting set
        if row['Hosting'] != '':
            continue
        if not row['ISP'] in hosting_isps:
            hosting_isps[row['ISP']] = 1
        else:
            hosting_isps[row['ISP']] = hosting_isps[row['ISP']] + 1

    for isp in hosting_isps:
        # if this partner has more than 3 customers using this ISP it's probably partner hosts
        if hosting_isps[isp] > 3:
            for row in partners[partner]:
                if row['ISP'] == isp:
                    row['Hosting'] = "Partner Hosted: %s [%s]" % (row['Partner Name'], isp)

for row in rows:
    if row['Hosting'] != '':
        row['Cloud'] = 'x'
    else:
        row['Cloud'] = ''

with open('output.csv', 'w', newline='', encoding='utf-8-sig') as output:
    writer = csv.writer(output)
    header_sent = False
    for row in rows:
        if not header_sent:
            writer.writerow(row.keys())
            header_sent = True
        writer.writerow(row.values())
