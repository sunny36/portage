-- The reconciler merges file_record (Store.List) with a connector listing,
-- both ordered by key. Object stores order keys by UTF-8 bytes, so List
-- sorts with COLLATE "C"; this index serves that order and the
-- key > afterKey cursor regardless of the database's default collation.
CREATE INDEX IF NOT EXISTS file_record_key_c_idx
    ON file_record (pipeline_id, key COLLATE "C");
