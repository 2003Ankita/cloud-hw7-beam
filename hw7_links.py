import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import os
from google.cloud import storage

BUCKET_NAME = "pagerank-bu-ap178152"
PREFIX = "webgraph_v2/"

def get_files():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=PREFIX)

    data = []
    for blob in blobs:
        content = blob.download_as_text()
        filename = os.path.basename(blob.name)
        data.append((filename, content))

    return data


def extract_links(file_tuple):
    filename, content = file_tuple
    links = re.findall(r'href="([^"]+)"', content)
    links = [os.path.basename(link) for link in links]
    return (filename, links)


def run():
    options = PipelineOptions([
        "--runner=DirectRunner"
    ])

    files_data = get_files()   # 🔥 outside Beam (important)

    with beam.Pipeline(options=options) as p:

        file_links = (
            p
            | "Create" >> beam.Create(files_data)
            | "Extract Links" >> beam.Map(extract_links)
        )

        outgoing = (
            file_links
            | "Outgoing Count" >> beam.Map(lambda x: (x[0], len(x[1])))
            | "Top Outgoing" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        outgoing | beam.Map(lambda x: print("Top Outgoing:", x))

        incoming = (
            file_links
            | "Incoming Pairs" >> beam.FlatMap(lambda x: [(t, 1) for t in x[1]])
            | "Sum Incoming" >> beam.CombinePerKey(sum)
            | "Top Incoming" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        incoming | beam.Map(lambda x: print("Top Incoming:", x))


if __name__ == "__main__":
    run()