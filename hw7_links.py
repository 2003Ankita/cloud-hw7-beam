import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

BUCKET_NAME = "pagerank-bu-ap178152"
PREFIX = "webgraph_v2/*"

# Extract outgoing links
def extract_outgoing(line):
    # Example pattern: href="..."
    links = re.findall(r'href="([^"]+)"', line)
    return len(links)

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read files" >> beam.io.ReadFromText(f"gs://{BUCKET_NAME}/{PREFIX}")
            | "Count outgoing per line" >> beam.Map(lambda x: extract_outgoing(x))
            | "Top 5 outgoing" >> beam.combiners.Top.Of(5)
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()