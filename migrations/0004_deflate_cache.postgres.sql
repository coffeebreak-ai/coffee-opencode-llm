-- Deflated tool results are cached by content hash. The same shell or MCP output
-- may arrive again on later turns; the cached text keeps the deflator off the
-- request path.
CREATE TABLE deflate_cache (
    hash          TEXT PRIMARY KEY,
    text          TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
