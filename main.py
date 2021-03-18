from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from googleapiclient.discovery import build
from google.oauth2 import service_account
import re
import logging

logger = logging.getLogger()


def get_sequence_type(seq):
    pattern = r'^[12]\d{3}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$'
    if all(item == '' for item in seq):
        return 'STRING'
    try:
        for item in seq:
            if item:
                float(item.replace(',', '.'))
        return 'FLOAT'
    except ValueError:
        pass

    for item in seq:
        if item and not re.match(pattern, item):
            return 'STRING'
    return 'DATE'


def get_spreadsheet_data(event, context):
    logger.info('execution is started')
    bigquery_client = bigquery.Client()
    project_id = event['attributes']['project_id']
    dataset_id = event['attributes']['dataset_id']
    table_id = event['attributes']['table_id'] if 'table_id' in event['attributes'] else None

    excluded_sheets = event['attributes']['excluded_sheets'].split('|') if 'excluded_sheets' in event['attributes'] else None
    included_sheets = event['attributes']['included_sheets'].split('|') if 'included_sheets' in event['attributes'] else None

    spreadsheet_id = event['attributes']['spreadsheet_id']

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    service_account_file = 'access.json'

    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    spreadsheet_name = sheet_metadata.get('properties').get('title')
    if not table_id:
        table_id = ''.join(
            [char for char in spreadsheet_name.replace(' ', '_') if char.isalnum() or char == '_']).lower()

    dataset_ref = f'{project_id}.{dataset_id}'
    table_ref = f'{dataset_ref}.{table_id}'

    try:
        bigquery_client.get_dataset(dataset_ref)  # Make an API request.
        logger.info("Dataset {} already exists".format(dataset_ref))
    except NotFound:
        logger.info("Dataset {} is not found".format(dataset_ref))
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = bigquery_client.create_dataset(dataset)
        logger.info(f'dataset is created {dataset_ref}')

    try:
        bigquery_client.delete_table(table_ref)  # Make an API request.
        logger.info("Table {} is dropped".format(table_ref))
    except NotFound:
        logger.info("Table {} is not found.".format(table_ref))

    if not included_sheets:
        sheets = sheet_metadata.get('sheets')
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
    else:
        sheet_names = included_sheets

    if excluded_sheets:
        for sheet in excluded_sheets:
            sheet_names.remove(sheet)

    logger.info(sheet_names)
    sheet = service.spreadsheets()
    column_names = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_names[0]).execute()['values'][0]
    for i in range(len(column_names)):
        column_names[i] = ''.join([char for char in column_names[i].replace(' ', '_').replace('$', 'usd') if
                                   char.isalnum() or char == '_']).lower()

    dicts_to_bq = []
    for sheet_name in sheet_names:
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
        values = result.get('values', [])
        logger.info(f'{sheet_name}, {len(values) - 1}')
        wrong_format = 0
        for row in values[1:]:
            row_to_add = {column_name: '' for column_name in column_names}
            row_to_add['sheet_name'] = sheet_name
            if len(row) != len(column_names):
                wrong_format += 1
                if wrong_format > 5:
                    logger.info(f'More than 5 rows with wrong format (lengths of row and column_names do not match, {sheet_name} sheet is skipped')
                    break
                else:
                    continue

            for i in range(len(row)):
                row_to_add[column_names[i]] = row[i]

            dicts_to_bq += [row_to_add]

    column_types = {}
    for column in column_names:
        sequence = [row[column] for row in dicts_to_bq]
        column_types[column] = get_sequence_type(sequence)

    for column in column_names:
        if column_types[column] == 'FLOAT':
            for row in dicts_to_bq:
                if row[column]:
                    row[column] = float(row[column].replace(',', '.'))
                else:
                    row[column] = None

        else:
            for row in dicts_to_bq:
                if not row[column]:
                    row[column] = None

    schema = [bigquery.SchemaField('sheet_name', 'STRING')]
    dummy_name = 1
    for column_name in column_names:
        if column_name:
            schema.append(bigquery.SchemaField(column_name, column_types[column_name]))
        else:
            schema.append(bigquery.SchemaField('dummy_name' + str(dummy_name), column_types[column_name]))
            dummy_name += 1

    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)
    logger.info(f'{table_ref} table is created')
    logger.info(column_types)

    num_items = len(dicts_to_bq)
    start = 0
    end = 5000
    while start < num_items:
        errors = bigquery_client.insert_rows_json(table_ref, dicts_to_bq[start:end])
        if not errors:
            logger.info(f"New rows have been added to {table_ref} table")
        else:
            logger.info("Encountered errors while inserting rows: {}".format(errors))
        start = end
        end += 5000
