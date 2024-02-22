import io
import pandas as pd
import requests
from dateutil.parser import isoparse
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from mage_ai.data_preparation.shared.secrets import get_secret_value
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_notes(stop_date):
    """ Get notes sent since a specific date. """
    token = get_secret_value('sharkey_api_token')
    url = 'https://posting.expert/api/notes'
    req_data = {
        "reply": False,
        "renote": False,
        "limit": 100
    }

    while (res := requests.post(url, json=req_data)):
        data = res.json()
        if len(data) == 0:
            break
        for note in data:
            yield (last_note := note)
        # Keep pulling data until notes created before the stop date are reached.
        if isoparse(last_note['createdAt']) < stop_date:
            break

        req_data.update({'untilId': last_note['id']})

@data_loader
def load_data_from_api(*args, **kwargs):
    stop_date = datetime.now(tz=timezone.utc) - timedelta(days=1)
    df = pd.json_normalize([note for note in get_notes(stop_date)], sep='_')[['id','createdAt','user_username','user_host']]
    # Change createdAt to a datetime object
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    # Filter out notes from before the stop date
    df = df[df['createdAt'] > stop_date]
    # Local posts do not have host data and need to be filled.
    df['user_host'] = df['user_host'].fillna('posting.expert')

    return df


@test
def test_output(output, *args) -> None:
    """
    Ensure output data fits the model.
    """
    class Model(BaseModel):
        id: str
        createdAt: datetime
        user_username: str
        user_host: str
    
    for record in output.to_dict('records'):
        Model(**record)
