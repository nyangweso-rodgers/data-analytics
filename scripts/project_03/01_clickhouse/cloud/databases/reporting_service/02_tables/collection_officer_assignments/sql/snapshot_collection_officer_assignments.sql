CREATE TABLE reporting_service.collection_officer_assignments_202609152043
ENGINE = MergeTree()
ORDER BY (id)
AS SELECT * FROM reporting_service.collection_officer_assignments;