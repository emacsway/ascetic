BEGIN;
CREATE TABLE "versioning_version" (
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
CREATE INDEX "versioning_version_object_id" ON "versioning_version" ("object_id");
CREATE INDEX "versioning_version_object_id_like" ON "versioning_version" ("object_id" varchar_pattern_ops);
CREATE INDEX "versioning_version_content_type_id" ON "versioning_version" ("content_type_id");
CREATE INDEX "versioning_version_stamp" ON "versioning_version" ("stamp");
CREATE INDEX "versioning_version_reverted" ON "versioning_version" ("reverted");
CREATE INDEX "versioning_version_sha1" ON "versioning_version" ("sha1");
CREATE INDEX "versioning_version_sha1_like" ON "versioning_version" ("sha1" varchar_pattern_ops);
CREATE INDEX "versioning_version_created_at" ON "versioning_version" ("created_at");
CREATE INDEX "versioning_version_editor_id" ON "versioning_version" ("editor_id");
COMMIT;
