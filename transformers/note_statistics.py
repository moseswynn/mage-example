if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
from pydantic import BaseModel
from datetime import datetime, timezone

def most_active(dataframe: pd.DataFrame, min_cols: list, group: list) -> pd.Series:
    return dataframe[min_cols]\
        .groupby(by=group)\
        .count()\
        .reset_index()\
        .rename(columns={'id': 'note_count'})\
        .sort_values(by='note_count', ascending=False)\
        .reset_index()[group+['note_count']].loc[0]


@transformer
def transform(data, *args, **kwargs):
    # Instance with the most posts.
    most_active_instance = most_active(
        data,
        ['user_host','id'],
        ['user_host']
    )

    # User with the most posts.
    most_active_user = most_active(
        data,
        ['user_username','user_host','id'],
        ['user_username','user_host']
    )

    # Results
    return pd.DataFrame([dict(
        total_posts=(total_posts := data.count()['id']),
        average_daily_posts=round(total_posts/7),
        most_active_instance=most_active_instance.user_host,
        most_active_instance_count=most_active_instance.note_count,
        most_active_user=f'@{most_active_user.user_username}@{most_active_user.user_host}',
        most_active_user_count=most_active_user.note_count,
        last_update=datetime.now(tz=timezone.utc)
    )])

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    class Model(BaseModel):
        total_posts: int
        average_daily_posts: int
        most_active_instance: str
        most_active_instance_count: int
        most_active_user: str
        most_active_user_count: int
        last_update: datetime

    for record in output.to_dict('records'):
        Model(**record)