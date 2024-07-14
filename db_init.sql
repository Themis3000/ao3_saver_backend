CREATE TABLE "queue"(
    "job_id" SERIAL NOT NULL,
    "work_id" INTEGER NOT NULL,
    "submitted_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "updated" BIGINT NOT NULL,
    "submitted_by_id" VARCHAR(255) NOT NULL,
    "format" VARCHAR(255) NOT NULL,
    "complete" boolean default false not null,
    "success" boolean default false not null
);
ALTER TABLE
    "queue" ADD PRIMARY KEY("job_id");
create index queue_complete_index
    on queue (complete);
CREATE INDEX "queue_work_id_index" ON
    "queue"("work_id");
CREATE TABLE "works_storage"(
    "storage_id" SERIAL NOT NULL,
    "work_id" INTEGER NOT NULL,
    "uploaded_time" BIGINT NOT NULL,
    "updated_time" BIGINT NOT NULL,
    "location" VARCHAR(255) NOT NULL,
    "patch_of" INTEGER NULL,
    "retrieved_from" VARCHAR(255) NOT NULL,
    "format" VARCHAR(255) NOT NULL,
    "title" VARCHAR(255) NULL,
    "img_enabled" BOOLEAN NOT NULL DEFAULT True,
    "sha1" CHAR(40) not null
);
ALTER TABLE
    works_storage ADD PRIMARY KEY("storage_id");
CREATE INDEX "storage_work_id_index" ON
    works_storage("work_id");
CREATE TABLE "dispatches"(
    "dispatch_id" SERIAL NOT NULL,
    "dispatched_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "dispatched_to_name" VARCHAR(255) NOT NULL,
    "job_id" INT NOT NULL,
    "fail_reported" BOOLEAN NOT NULL DEFAULT FALSE,
    "fail_status" SMALLINT NULL,
    "report_code" SMALLINT NOT NULL,
    "complete" boolean default FALSE not NULL,
    "found_as_duplicate" boolean default false not null
);
ALTER TABLE
    "dispatches" ADD PRIMARY KEY("dispatch_id");
CREATE INDEX "dispatches_job_id_index" ON
    "dispatches"("job_id");
create index dispatches_fail_reported_index
    on dispatches (fail_reported);
ALTER TABLE
    works_storage ADD CONSTRAINT "storage_patch_of_foreign" FOREIGN KEY("patch_of") REFERENCES works_storage("storage_id");
ALTER TABLE
    "dispatches" ADD CONSTRAINT "dispatches_job_id_foreign" FOREIGN KEY("job_id") REFERENCES "queue"("job_id");

create table object_store
(
    sha1        varchar(40)   not null,
    location    varchar(255)  not null
);

alter table object_store
    add constraint object_store_pk
        primary key (sha1);

create table object_index
(
    object_id       serial                  not null
        constraint object_index_pk
            primary key,
    request_url     varchar(2000)           not null,
    sha1            char(40)                not null,
    etag            varchar(255),
    mimetype        varchar(255)            not null,
    associated_work integer                 not null,
    creation_time   TIMESTAMP default NOW() not null
);

create index object_index_associated_work_index
    on object_index (associated_work);

create index object_index_sha1_index
    on object_index (sha1);

alter table object_index
    add constraint object_index_object_store_sha1_fk
        foreign key (sha1) references object_store;

alter table works_storage
    add constraint works_storage_pk
        unique (work_id, updated_time, format);

create unique index queue_work_id_updated_format_uindex
    on queue (work_id, updated, format)
    where complete = false;
