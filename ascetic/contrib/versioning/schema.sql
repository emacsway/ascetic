BEGIN;
CREATE TABLE "versioning_revision" (
    "id" serial NOT NULL PRIMARY KEY,
    "object_id" varchar(255) NOT NULL,
    "content_type_id" varchar(255) NOT NULL,
    "stamp" integer NOT NULL,
    "reverted" boolean NOT NULL,
    "sha1" varchar(40) NOT NULL,
    "delta" text NOT NULL,
    "created_at" timestamp with time zone NOT NULL,
    "comment" varchar(255) NOT NULL,
    "editor_id" integer,
    "editor_ip" inet,
    UNIQUE ("object_id", "content_type_id", "stamp")
)
;
CREATE INDEX "versioning_revision_object_id" ON "versioning_revision" ("object_id");
CREATE INDEX "versioning_revision_object_id_like" ON "versioning_revision" ("object_id" varchar_pattern_ops);
CREATE INDEX "versioning_revision_content_type_id" ON "versioning_revision" ("content_type_id");
CREATE INDEX "versioning_revision_stamp" ON "versioning_revision" ("stamp");
CREATE INDEX "versioning_revision_reverted" ON "versioning_revision" ("reverted");
CREATE INDEX "versioning_revision_sha1" ON "versioning_revision" ("sha1");
CREATE INDEX "versioning_revision_sha1_like" ON "versioning_revision" ("sha1" varchar_pattern_ops);
CREATE INDEX "versioning_revision_created_at" ON "versioning_revision" ("created_at");
CREATE INDEX "versioning_revision_editor_id" ON "versioning_revision" ("editor_id");
COMMIT;
