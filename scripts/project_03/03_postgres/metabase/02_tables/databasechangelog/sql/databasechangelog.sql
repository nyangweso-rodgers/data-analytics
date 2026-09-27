SELECT --id, author, filename, dateexecuted, orderexecuted, exectype, md5sum, description, "comments", tag, liquibase, contexts, labels, deployment_id
count(*)
FROM public.databasechangelog;