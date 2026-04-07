import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

BUCKET_NAME = "pagerank-bu-ap178152"
PREFIX = "webgraph_v2/*"

def run():
    options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1"
])

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(
                f"gs://{BUCKET_NAME}/{PREFIX}"
            )
            | "Sample" >> beam.combiners.Sample.FixedSizeGlobally(5)
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()