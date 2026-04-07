import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

BUCKET_NAME = "pagerank-bu-ap178152"
PREFIX = "webgraph_v2/*"

def run():
    options = PipelineOptions()

    with beam.Pipeline(runner="DirectRunner", options=options) as p:
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(
                f"gs://{BUCKET_NAME}/{PREFIX}"
            )
            | "Sample" >> beam.combiners.Sample.FixedSizeGlobally(5)
            | "Flatten" >> beam.FlatMap(lambda x: x)
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()