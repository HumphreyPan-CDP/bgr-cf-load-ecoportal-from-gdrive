gcloud functions deploy load-ecoportal-from-gdrive `
--project=bgr-d-wmp2-cad2 `
--gen2 `
--runtime=python312 `
--region=australia-southeast1 `
--source=. `
--entry-point=load_flie_from_gdrive `
--trigger-http `
--memory=2000MB

{
  "gdrive_folder_id": "112RIRRZNqPeU2oa2BgXlWxjkYglIpUrM",
        "landing_project":"bgr-d-landing-b23c",
        "target_bucket_name" : "bgr-d-landing-file-history",
        "target_archive_bucket_name" : "bgr-d-ecoportal-archive",
        "target_tables":"incidents,meetings",
        "target_dataset_name" : "ecoportal",
        "target_default_partition" : "1900-01-01"
}


dev folder: [{'webViewLink': 'https://drive.google.com/drive/folders/1V9txunznrUuhMiNNfE57V8_5lUj-g316', 'id': '1V9txunznrUuhMiNNfE57V8_5lUj-g316', 'name': 'DEV', 'modifiedTime': '2024-04-09T21:22:39.778Z'}]

NPD:
[{'webViewLink': 'https://drive.google.com/drive/folders/1aneXjhOi-iHrdkXKVwX4kr94vKQ9_eBc', 'id': '1aneXjhOi-iHrdkXKVwX4kr94vKQ9_eBc', 'name': 'NPD', 'modifiedTime': '2024-04-09T21:27:01.033Z'}]

prod
[{'webViewLink': 'https://drive.google.com/drive/folders/112RIRRZNqPeU2oa2BgXlWxjkYglIpUrM', 'id': '112RIRRZNqPeU2oa2BgXlWxjkYglIpUrM', 'name': 'PROD', 'modifiedTime': '2024-04-08T22:54:30.531Z'}]