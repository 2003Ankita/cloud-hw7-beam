import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import os
from google.cloud import storage

BUCKET_NAME = "pagerank-bu-ap178152"
PREFIX = "webgraph_v2/"  # keep small for local

def get_files():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=PREFIX)

    data = []
    for i, blob in enumerate(blobs):
        content = blob.download_as_text()
        data.append(content)

    return data


def extract_bigrams(text):
    # clean + tokenize
    words = re.findall(r'\b\w+\b', text.lower())

    # create bigrams
    return [(words[i] + " " + words[i+1], 1) for i in range(len(words)-1)]


def run():
    options = PipelineOptions([
        "--runner=DirectRunner"
    ])

    files_data = get_files()

    with beam.Pipeline(options=options) as p:

        bigrams = (
            p
            | "Create" >> beam.Create(files_data)
            | "Extract Bigrams" >> beam.FlatMap(extract_bigrams)
            | "Count Bigrams" >> beam.CombinePerKey(sum)
            | "Top 5" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        bigrams | beam.Map(lambda x: print("Top Bigrams:", x))


if __name__ == "__main__":
    run()