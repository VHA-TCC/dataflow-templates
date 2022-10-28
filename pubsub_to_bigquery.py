import logging

from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

class MountingRow(beam.DoFn):
    def __init__(self, schema: dict, callback_handler):
        self.schema = schema
        self.callback_handler = callback_handler

    def converter(self, field: str, datatype: str):
        def to_date(date):
            from datetime import datetime

            if type(date) == datetime:
                return date
            return datetime.strptime(date.replace('/', '-'), '%Y-%m-%d %H:%M:%S.%f')

        def to_int(integer) -> int:
            integer = integer.replace(',', '').replace('.', '')
            return int(integer)
        
        def to_float(float_number) -> float:
            number = float_number.replace(',', '.')
            return float(number)

        types = {
            "FLOAT": to_float,
            "INTEGER": to_int,
            "STRING": str,
            "DATETIME": to_date,
        }

        return_obj = types[datatype](field)  

        return return_obj

    def process(self, row):

        out_dict = {}

        for index, item in enumerate(self.schema):
            try:
                if(field := row[index]) is not None and field != '':
                    out_dict[item.get('name')] = self.converter(field, item.get('type'))
                else:
                    out_dict[item.get('name')] = None
            except Exception as err:
                err_obj = self.callback_handler(row, f'Quantidade de campos divergente! Bigquery: {len(self.schema)} - Recebido: {len(row)}')
                yield err_obj
                return

        yield out_dict


class Printing(beam.DoFn):
    def process(self, row):
        logging.info(row)
        yield row


class OutputDelivery(beam.DoFn):
    def process(self, row):
        if row.get('error'):
            logging.error(f"Bad Record: {row}")
            yield beam.pvalue.TaggedOutput('fail', row)
        else:
            yield row


def get_schema(target: str):	
	project_id, dataset_table = target.split(':')
	dataset, table = dataset_table.split('.')

	client = bigquery.Client()
	dataset = client.dataset(dataset, project_id)
	table_ref = dataset.table(table)
	table = client.get_table(table_ref)

	return [{"mode": field.mode, "name": field.name, "type": field.field_type} for field in table.schema]

def to_err(row: dict, err: str):
	logging.error(err)
	return {"error": str(err), "row": str(row)}

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--bigquery_table_id')
        parser.add_argument('--pubsub_topic_id')

def run(argv=None):

    pipeline_options = PipelineOptions(save_main_session=True, streaming=True)
    my_options = pipeline_options.view_as(MyOptions)
    pipeline = beam.Pipeline(options=pipeline_options)

    table_id = my_options.bigquery_table_id
    schema = get_schema(table_id)
    topic = my_options.pubsub_topic_id

    rows = (
		pipeline
		| "Data ingest" >> beam.io.ReadFromPubSub(topic=topic)
        | "Decoding" >> beam.Map(lambda x: x.decode('utf-8'))
		| "Split fields" >> beam.Map(lambda row: row.split(';'))
		| "Stripping fields" >> beam.Map(lambda row: [field.strip() for field in row])
		| "Mounting row" >> beam.ParDo(MountingRow(schema, to_err)))
	
    results = (
		rows
		| "Splitting Delivery" >> beam.ParDo(OutputDelivery()).with_outputs('fail',
                                                                            main='records'))
		
    fail_records = results.fail
    good_records = results.records

    good_records_output = (
		good_records
		| "Inbound good records" >> beam.ParDo(Printing())
		| "Bigquery good records" >> beam.io.WriteToBigQuery(
			table=table_id,
			schema=lambda x: {"fields": [{"mode": "NULLABLE", "name": item.get('name'), "type": item.get('type')} for item in schema]},
			custom_gcs_temp_location="gs://car-sensor-dataflow/temp"))

    bad_records_output = (
		fail_records
		| "Inbound bad records" >> beam.ParDo(Printing())
		| "Bigquery bad records" >> beam.io.WriteToBigQuery(
			table=f"{table_id}_errors",
			schema=lambda x: {
				"fields": [
					{"mode": "NULLABLE", "name": "error", "type": "STRING"}, 
					{"mode": "NULLABLE", "name": "row", "type": "STRING"}]},
			create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
			custom_gcs_temp_location="gs://car-sensor-dataflow/temp"))

    result = pipeline.run()

    result.wait_until_finish()

    logging.info('Returning with status result')

    pipeline.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()