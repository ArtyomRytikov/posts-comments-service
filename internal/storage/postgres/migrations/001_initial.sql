CREATE TABLE posts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id text NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    comments_enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE comments (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id bigint NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    parent_id bigint,
    author_id text NOT NULL,
    body text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT comments_body_length CHECK (char_length(body) <= 2000),
    CONSTRAINT comments_not_own_parent CHECK (parent_id IS NULL OR parent_id <> id),
    CONSTRAINT comments_post_id_id_unique UNIQUE (post_id, id),
    CONSTRAINT comments_parent_same_post FOREIGN KEY (post_id, parent_id)
        REFERENCES comments (post_id, id)
);

-- The unique constraint above also indexes the flat post feed by (post_id, id).
-- This index serves both root comments (parent_id IS NULL) and direct replies.
CREATE INDEX comments_post_parent_id_id_idx ON comments (post_id, parent_id, id);
