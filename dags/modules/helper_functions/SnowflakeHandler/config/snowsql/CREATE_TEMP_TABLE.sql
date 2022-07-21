CREATE TEMPORARY TABLE {}
    USING TEMPLATE (
        SELECT array_agg(object_construct(*))
        FROM TABLE(
                infer_schema(
                location=>'@{}/{}',
                file_format=>'{}'
                )
        )
    );