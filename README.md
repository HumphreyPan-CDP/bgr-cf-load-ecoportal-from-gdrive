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
