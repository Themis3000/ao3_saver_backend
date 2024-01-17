CREATE TABLE "queue"(
    "job_id" SERIAL NOT NULL,
    "work_id" INTEGER NOT NULL,
    "submitted_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "updated" BIGINT NOT NULL,
    "submitted_by_name" VARCHAR(255) NOT NULL,
    "submitted_by_id" VARCHAR(255) NOT NULL,
    "format" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "queue" ADD PRIMARY KEY("job_id");
CREATE INDEX "queue_work_id_index" ON
    "queue"("work_id");
CREATE TABLE "storage"(
    "storage_id" SERIAL NOT NULL,
    "work_id" INTEGER NOT NULL,
    "location_type" VARCHAR(255) NOT NULL,
    "location_id" INTEGER NOT NULL,
    "uploaded_time" BIGINT NOT NULL,
    "updated_time" BIGINT NOT NULL,
    "location" BIGINT NOT NULL,
    "patch_of" INTEGER NULL,
    "retrieved_from" VARCHAR(255) NOT NULL,
    "format" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "storage" ADD PRIMARY KEY("storage_id");
CREATE INDEX "storage_work_id_index" ON
    "storage"("work_id");
CREATE TABLE "works"(
    "work_id" INTEGER NOT NULL,
    "title" TEXT NULL,
    "img_enabled" BOOLEAN NOT NULL
);
ALTER TABLE
    "works" ADD PRIMARY KEY("work_id");
CREATE TABLE "dispatches"(
    "dispatch_id" SERIAL NOT NULL,
    "dispatched_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "dispatched_to_name" VARCHAR(255) NOT NULL,
    "dispatched_to_id" VARCHAR(255) NOT NULL,
    "job_id" INT NOT NULL,
    "fail_reported" BOOLEAN NOT NULL DEFAULT FALSE,
    "fail_status" SMALLINT NULL,
    "report_code" SMALLINT NOT NULL
);
ALTER TABLE
    "dispatches" ADD PRIMARY KEY("dispatch_id");
CREATE INDEX "dispatches_job_id_index" ON
    "dispatches"("job_id");
ALTER TABLE
    "storage" ADD CONSTRAINT "storage_work_id_foreign" FOREIGN KEY("work_id") REFERENCES "works"("work_id");
ALTER TABLE
    "storage" ADD CONSTRAINT "storage_patch_of_foreign" FOREIGN KEY("patch_of") REFERENCES "storage"("storage_id");
ALTER TABLE
    "dispatches" ADD CONSTRAINT "dispatches_job_id_foreign" FOREIGN KEY("job_id") REFERENCES "queue"("job_id") ON DELETE CASCADE;