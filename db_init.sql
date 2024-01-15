CREATE TABLE "queue"(
    "job_id" SERIAL NOT NULL,
    "work_id" INTEGER NOT NULL,
    "updated" BIGINT NOT NULL,
    "submitted_by" VARCHAR(255) NOT NULL,
    "format" VARCHAR(255) NOT NULL,
    "disbatched" BOOLEAN NOT NULL,
    "disbatched_to" VARCHAR(255) NULL,
    "disbatched_time" BIGINT NULL
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
ALTER TABLE
    "storage" ADD CONSTRAINT "storage_work_id_foreign" FOREIGN KEY("work_id") REFERENCES "works"("work_id");
ALTER TABLE
    "storage" ADD CONSTRAINT "storage_patch_of_foreign" FOREIGN KEY("patch_of") REFERENCES "storage"("storage_id");