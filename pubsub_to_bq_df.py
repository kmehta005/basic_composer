import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

class ParseJsonMessage(beam.DoFn):
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            yield {
                "error_code": message.get("error_code"),
                "message": message.get("message"),
                "source": message.get("source", "unknown"),
                "timestamp": message.get("timestamp")
            }
        except Exception as e:
            # You could route bad messages to a dead-letter table or log
            print(f"Error parsing message: {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--runner', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    args, beam_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "Parse JSON" >> beam.ParDo(ParseJsonMessage())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=args.output_table,
                schema="error_code:STRING,message:STRING,source:STRING,timestamp:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
