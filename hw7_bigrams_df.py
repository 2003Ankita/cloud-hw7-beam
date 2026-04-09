import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
import re

BUCKET_PATH = "gs://pagerank-bu-ap178152/webgraph_v2/*"

def extract_bigrams(file):
    text = file.read_utf8()
    words = re.findall(r'\b\w+\b', text.lower())
    return [(words[i] + " " + words[i+1], 1) for i in range(len(words)-1)]


def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:

        bigrams = (
            p
            | "Match Files" >> fileio.MatchFiles(BUCKET_PATH)
            | "Read Files" >> fileio.ReadMatches()
            | "Extract Bigrams" >> beam.FlatMap(extract_bigrams)
            | "Count" >> beam.CombinePerKey(sum)
            | "Top 5" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        bigrams | "Print Bigrams" >> beam.Map(print)


if __name__ == "__main__":
    run()