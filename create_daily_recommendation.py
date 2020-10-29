import dotenv
import firebase_admin

from firebase_admin import firestore
from firebase_admin import db
import secrets

from airtable import Airtable
import json
import pandas as pd
import os

import datetime
from pytz import timezone, utc
from timezonefinder import TimezoneFinder
import dateutil

df = pd.read_csv('data/addresses.csv')
tf = TimezoneFinder()

def summarize(submission_id):
    data = at.get(submission_id)['fields']
    if 'abstract' in data:
        return data['abstract'][:255]
    else:
        return ""


def infer_timerange(data):
    if 'grid.' in data['institution']:
        grid_location = data['institution'].find('grid.')
        grid = data['institution'][grid_location:-1]
        
        assert grid[:4] == 'grid'

        institute = df.query(f'grid_id == "{grid}"')
        if institute.empty:
            the_timezone = timezone('utc')
        else:
            institute = institute.iloc[0]
            the_timezone = timezone(tf.timezone_at(lng=institute.lng, lat=institute.lat))
    else:
        the_timezone = timezone('utc')

    timerange_start = datetime.datetime(2020, 10, 29, 8, 0, 0, tzinfo=the_timezone)
    timerange_end   = datetime.datetime(2020, 10, 29, 20, 0, 0, tzinfo=the_timezone)

    return timerange_start, timerange_end, the_timezone


def localize(the_str, timezone):
    fmt = '%Y-%m-%d %H:%M:%S (%Z'
    if str(timezone) == 'UTC':
        appendix = ')'
    else:
        appendix = ' ' + str(timezone) + ')'
    return dateutil.parser.parse(the_str + 'Z').astimezone(timezone).strftime(fmt) + appendix

def main():
    day = 'Thursday'

    # Find the recommendations for the day that we need
    with open('data/nmc3_thursday_recommendation_talks.json', 'r') as f:
        data = json.load(f)

    storage_bucket_prod = 'neuromatch-e6422'  # @param{type:"string"}
    prod_app = firebase_admin.initialize_app(options={'projectId': storage_bucket_prod, 
                                                      'storageBucket': storage_bucket_prod})
    db = firestore.client(app=prod_app)

    emails = []

    n = 0
    for user in data:

        items = db.collection('users_2020_3').where('email', '==', user['email']).get()
        if not items:
            continue

        assert len(items) >= 1 and len(items) <= 3

        day_start, day_end, local_tz = infer_timerange(items[0].to_dict())

        id = items[0].id

        prefs = db.collection('preferences_2020_3').document(id).get().to_dict()
        if prefs is None:
            continue
    
        starred = prefs['submission_ids']

        recs = user['recommendations']

        ranked_recs = []
        for rec in recs:
            event_start = dateutil.parser.parse(rec['starttime'] + 'Z')
            if (event_start <= day_start or 
                event_start >= day_end):
                continue

            if rec['submission_id'] in starred:
                rec['source'] = 'Based on your starred items'
                score = 1
            else:
                rec['source'] = 'A keynote event'
                score = score_map[rec['submission_id']]

            # Use a minus sign so score and start time have the same polarity
            ranked_recs.append((-score, rec['starttime'], rec['submission_id']))

        if sorted(ranked_recs)[0][0] != -1:
            print('No recommendations, skipping')
            continue

        rec_map = {rec['submission_id']: rec for rec in recs}

        # Sort recommendations into different sources
        ranked = [rec_map[submission_id] for _, _, submission_id in sorted(ranked_recs)]
        
        summary_1 = summarize(ranked[0]['submission_id'])
        summary_2 = summarize(ranked[1]['submission_id'])
        summary_3 = summarize(ranked[2]['submission_id'])

        url_base = 'https://neuromatch.io/abstract?submission_id='

        email = {
            'to_name': user['fullname'],
            'to_email': user['email'],
            'from_name': 'Neuromatch conference',
            'from_email': 'no-reply@neuromatch.io',
            'day': day,
            'name': user['fullname'],
            'top_talk': ranked[0]['title'],
            'talk_name_1': ranked[0]['title'],
            'talk_time_1': localize(ranked[0]['starttime'], local_tz),
            'talk_author_1': ranked[0]['fullname'],
            'talk_source_1' : ranked[0]['source'],
            'talk_summary_1': summary_1,
            'talk_link_1': url_base + ranked[0]['submission_id'],
            'talk_name_2': ranked[1]['title'],
            'talk_time_2': localize(ranked[1]['starttime'], local_tz),
            'talk_author_2': ranked[1]['fullname'],
            'talk_source_2' : ranked[1]['source'],
            'talk_summary_2': summary_2,
            'talk_link_2': url_base + ranked[1]['submission_id'],
            'talk_name_3': ranked[2]['title'],
            'talk_time_3': localize(ranked[2]['starttime'], local_tz),
            'talk_author_3': ranked[2]['fullname'],
            'talk_source_3' : ranked[2]['source'],
            'talk_summary_3': summary_3,
            'talk_link_3': url_base + ranked[2]['submission_id'],
            'categories': ['recommendations']
        }

        emails.append(email)

        n += 1

    df = pd.DataFrame(emails)
    with open('data/wednesday_recs.csv', 'w') as f:
        df.to_csv(f)

    print(len(df))


if __name__ == "__main__":

    scores = {
        'Keynote Event': 0.8,
        'Special Event': 0.8,
        'Traditional talk': 0.0,
        'Interactive talk': 0.0,
    }

    dotenv.load_dotenv()
    at_app_key = os.getenv('AT_APP_KEY_GREY')
    at_api_key = os.getenv('AT_API_KEY')
    table_name = 'submissions'

    at = Airtable(at_app_key, table_name, at_api_key)
    records = at.get_all()

    score_map = {}
    for record in records:
        score_map[record['id']] = scores[record['fields']['talk_format']]

    main()