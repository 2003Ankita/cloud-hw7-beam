import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
import re
import os

BUCKET_PATH = "gs://pagerank-bu-ap178152/webgraph_v2/*"

def extract_links(file):
    content = file.read_utf8()
    filename = os.path.basename(file.metadata.path)

    links = re.findall(r'href="([^"]+)"', content)
    links = [os.path.basename(link) for link in links]

    return (filename, links)


def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:

        file_links = (
            p
            | "Match Files" >> fileio.MatchFiles(BUCKET_PATH)
            | "Read Files" >> fileio.ReadMatches()
            | "Extract Links" >> beam.Map(extract_links)
        )

        outgoing = (
            file_links
            | "Outgoing Count" >> beam.Map(lambda x: (x[0], len(x[1])))
            | "Top Outgoing" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        outgoing | beam.Map(print)

        incoming = (
            file_links
            | "Incoming Pairs" >> beam.FlatMap(lambda x: [(t, 1) for t in x[1]])
            | "Sum Incoming" >> beam.CombinePerKey(sum)
            | "Top Incoming" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        incoming | beam.Map(print)


if __name__ == "__main__":
    run()