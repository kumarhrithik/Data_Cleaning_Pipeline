from app import create_app

app = create_app()

if __name__ == "__main__":
    from app.tasks import DataPipelineTask, task_registry, check_missing_values, check_duplicates,  check_date_formats, check_email_format, check_gender_consistency, check_name_consistency, check_age_validity

    task_registry['check_missing_values'] = DataPipelineTask('check_missing_values', check_missing_values)
    task_registry['check_duplicates'] = DataPipelineTask('check_duplicates', check_duplicates)
    task_registry['check_date_formats'] = DataPipelineTask('check_date_formats', check_date_formats)
    task_registry['check_email_format'] = DataPipelineTask('check_email_format', check_email_format)
    task_registry['check_gender_consistency'] = DataPipelineTask('check_gender_consistency', check_gender_consistency)
    task_registry['check_name_consistency'] = DataPipelineTask('check_name_consistency', check_name_consistency)
    task_registry['check_age_validity'] = DataPipelineTask('check_age_validity', check_age_validity)

    app.run(debug=True)