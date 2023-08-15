import argparse
import logging
import requests
#Squirro Coding Challenge - Calvin Fuss
log = logging.getLogger(__name__)

API_ENDPOINT = "https://api.nytimes.com/svc/search/v2/articlesearch.json"


def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        self.api_key = None

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)
        self.api_key = self.args.api_key

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, batch_size):
        params = {
            'api-key': self.api_key,
            'q': self.args.query,
            'page': 0  # This can be incremented for subsequent batches
        }

        response = requests.get(API_ENDPOINT, params=params)
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")

        data = response.json()
        docs = data.get("response", {}).get("docs", [])

        flattened_docs = [flatten_dict(doc) for doc in docs]
        yield flattened_docs[:batch_size]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the source
        """
        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]
        return schema


if __name__ == "__main__":
    config = {
        "api_key": "",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()
    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    source.connect()

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f" - {item['_id']} - {item['headline.main']}")

    source.disconnect()
