import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
import re
import os

BUCKET_PATH = "gs://pagerank-bu-ap178152/webgraph_v2/*"

# Extract links from file content
def extract_links(file):
    content = file.read_utf8()

    filename = os.path.basename(file.metadata.path)

    # find all href links
    links = re.findall(r'href="([^"]+)"', content)

    # normalize links (just filenames like 123.html)
    links = [os.path.basename(link) for link in links]

    return (filename, links)


def run():
    options = PipelineOptions([
    "--runner=DirectRunner"
    ])

    with beam.Pipeline(options=options) as p:

        files = (
            p
            | "Match Files" >> fileio.MatchFiles(BUCKET_PATH)
            | "Read Files" >> fileio.ReadMatches()
        )

        # (file, [outgoing_links])
        file_links = files | "Extract Links" >> beam.Map(extract_links)

        # -------------------------
        # OUTGOING LINKS
        # -------------------------
        outgoing = (
            file_links
            | "Outgoing Count" >> beam.Map(lambda x: (x[0], len(x[1])))
            | "Top 5 Outgoing" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        outgoing | "Print Outgoing" >> beam.Map(lambda x: print("Top Outgoing:", x))

        # -------------------------
        # INCOMING LINKS
        # -------------------------
        incoming = (
            file_links
            | "Create Incoming Pairs" >> beam.FlatMap(
                lambda x: [(target, 1) for target in x[1]]
            )
            | "Sum Incoming" >> beam.CombinePerKey(sum)
            | "Top 5 Incoming" >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        incoming | "Print Incoming" >> beam.Map(lambda x: print("Top Incoming:", x))


if __name__ == "__main__":
    run()