from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

def format_change_event(event: Dict) -> Dict:
    output = {
        'operationType': (operation := event['operationType'])
    }
    if operation == 'insert':
        output.update({'fulldocument': event['fullDocument']})
    else:
        output.update({'id': event['documentKey']['id']})

    return output

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    return list(map(format_change_event, messages))
