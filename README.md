# googlesheets_to_bq
git clone https://github.com/tensionworks/googlesheets_to_bq

cd spreadsheets_to_bq/

gcloud pubsub topics create spreadsheets_to_bq_run

gcloud functions deploy get_spreadsheet_data --runtime python37 --trigger-topic spreadsheets_to_bq_run --timeout=540 --memory=1024MB

gcloud beta scheduler jobs create pubsub googlesheet_to_bq --time-zone "Europe/Kiev" --schedule "0 4 * * *" --topic spreadsheets_to_bq_run --message-body "get_spreadsheet_data" --attributes project_id="test-at-1",dataset_id="spreadsheet_test",spreadsheet_id="1Xiv6LBJ91D_NIuTydNv-ZG6sWqu2TIUwy-6E_81oHnM",excluded_sheets="Шаблон|Цели и информация"
