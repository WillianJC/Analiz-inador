-- ============================================================
--  PitonScraper – Azure SQL Database
--  Tablas para comentarios de YouTube, TikTok y Reddit
-- ============================================================

-- ------------------------------------------------------------
--  1. Plataformas (catálogo)
-- ------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'platforms' AND type = 'U'
)
BEGIN
    CREATE TABLE dbo.platforms (
        platform_id   TINYINT      NOT NULL,
        platform_name NVARCHAR(20) NOT NULL,
        CONSTRAINT PK_platforms PRIMARY KEY (platform_id),
        CONSTRAINT UQ_platforms_name UNIQUE (platform_name)
    );

    INSERT INTO dbo.platforms (platform_id, platform_name) VALUES
        (1, 'YouTube'),
        (2, 'TikTok'),
        (3, 'Reddit');
END;
GO

-- ------------------------------------------------------------
--  2. Sesiones de scraping
--     Una sesión representa una ejecución del scraper sobre
--     una URL concreta.
-- ------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'scrape_sessions' AND type = 'U'
)
BEGIN
    CREATE TABLE dbo.scrape_sessions (
        session_id    INT              NOT NULL IDENTITY(1,1),
        platform_id   TINYINT          NOT NULL,
        source_url    NVARCHAR(2048)   NOT NULL,   -- URL ingresada
        source_id     NVARCHAR(256)    NULL,        -- video_id / post_id
        subreddit     NVARCHAR(128)    NULL,        -- solo Reddit
        sort_mode     NVARCHAR(20)     NULL,        -- 'popular'/'recent'/NULL
        comments_limit INT             NULL,
        total_fetched INT              NOT NULL DEFAULT 0,
        scraped_at    DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_scrape_sessions PRIMARY KEY (session_id),
        CONSTRAINT FK_sessions_platform
            FOREIGN KEY (platform_id) REFERENCES dbo.platforms(platform_id)
    );
END;
GO

-- ------------------------------------------------------------
--  3. Comentarios (tabla principal)
-- ------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'comments' AND type = 'U'
)
BEGIN
    CREATE TABLE dbo.comments (
        comment_id      BIGINT         NOT NULL IDENTITY(1,1),
        session_id      INT            NOT NULL,
        platform_id     TINYINT        NOT NULL,
        -- Campos comunes a las tres plataformas
        author          NVARCHAR(255)  NULL,
        body            NVARCHAR(MAX)  NULL,
        votes           INT            NOT NULL DEFAULT 0,
        reply_count     INT            NOT NULL DEFAULT 0,
        published_at    DATETIME2      NULL,        -- fecha/hora del comentario
        -- Identificador nativo de la plataforma
        native_id       NVARCHAR(128)  NULL,
        -- Metadatos de importación
        imported_at     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_comments PRIMARY KEY (comment_id),
        CONSTRAINT FK_comments_session
            FOREIGN KEY (session_id) REFERENCES dbo.scrape_sessions(session_id)
            ON DELETE CASCADE,
        CONSTRAINT FK_comments_platform
            FOREIGN KEY (platform_id) REFERENCES dbo.platforms(platform_id)
    );

    -- Índice por sesión (consultas frecuentes por sesión)
    CREATE NONCLUSTERED INDEX IX_comments_session
        ON dbo.comments (session_id);

    -- Índice por plataforma + autor (búsquedas de análisis)
    CREATE NONCLUSTERED INDEX IX_comments_platform_author
        ON dbo.comments (platform_id, author);

    -- Índice de texto completo en body (opcional, requiere Full-Text Search habilitado)
    -- CREATE FULLTEXT INDEX ON dbo.comments(body) KEY INDEX PK_comments;
END;
GO

-- ------------------------------------------------------------
--  4. Vista de consulta cómoda
-- ------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_comments AS
SELECT
    c.comment_id,
    p.platform_name                          AS platform,
    s.source_url,
    s.source_id,
    s.subreddit,
    s.scraped_at,
    c.author,
    c.body,
    c.votes,
    c.reply_count,
    c.published_at,
    c.native_id
FROM dbo.comments        c
JOIN dbo.scrape_sessions s ON s.session_id  = c.session_id
JOIN dbo.platforms       p ON p.platform_id = c.platform_id;
GO

-- ------------------------------------------------------------
--  5. Stored procedure: iniciar sesión de scraping
--     Devuelve el session_id generado para usarlo al insertar
--     comentarios desde la aplicación.
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_begin_session
    @platform_name  NVARCHAR(20),
    @source_url     NVARCHAR(2048),
    @source_id      NVARCHAR(256)  = NULL,
    @subreddit      NVARCHAR(128)  = NULL,
    @sort_mode      NVARCHAR(20)   = NULL,
    @comments_limit INT            = NULL,
    @session_id     INT            OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pid TINYINT;
    SELECT @pid = platform_id FROM dbo.platforms WHERE platform_name = @platform_name;

    IF @pid IS NULL
        THROW 50001, 'Plataforma no reconocida. Use YouTube, TikTok o Reddit.', 1;

    INSERT INTO dbo.scrape_sessions
        (platform_id, source_url, source_id, subreddit, sort_mode, comments_limit)
    VALUES
        (@pid, @source_url, @source_id, @subreddit, @sort_mode, @comments_limit);

    SET @session_id = SCOPE_IDENTITY();
END;
GO

-- ------------------------------------------------------------
--  6. Stored procedure: insertar comentarios en lote
--     Acepta un JSON con el array de comentarios generado
--     por la aplicación Python.
--
--  Formato esperado del JSON (igual que la exportación):
--  [
--    { "author": "...", "text": "...", "votes": 5,
--      "reply_count": 2, "time": "2025-01-01 12:00",
--      "cid": "abc123" },
--    ...
--  ]
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_insert_comments_json
    @session_id  INT,
    @platform_id TINYINT,
    @json_data   NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.comments
        (session_id, platform_id, author, body, votes, reply_count, published_at, native_id)
    SELECT
        @session_id,
        @platform_id,
        j.author,
        j.body,
        ISNULL(j.votes, 0),
        ISNULL(j.reply_count, 0),
        TRY_CAST(j.time_str AS DATETIME2),
        j.native_id
    FROM OPENJSON(@json_data)
    WITH (
        author      NVARCHAR(255)  '$.author',
        body        NVARCHAR(MAX)  '$.text',
        votes       INT            '$.votes',
        reply_count INT            '$.reply_count',
        time_str    NVARCHAR(50)   '$.time',
        native_id   NVARCHAR(128)  '$.cid'
    ) AS j;

    -- Actualizar contador en la sesión
    UPDATE dbo.scrape_sessions
    SET total_fetched = (
        SELECT COUNT(*) FROM dbo.comments WHERE session_id = @session_id
    )
    WHERE session_id = @session_id;
END;
GO

-- ------------------------------------------------------------
--  7. Ejemplos de uso
-- ------------------------------------------------------------

/*
-- Iniciar sesión YouTube:
DECLARE @sid INT;
EXEC dbo.usp_begin_session
    @platform_name  = 'YouTube',
    @source_url     = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
    @source_id      = 'dQw4w9WgXcQ',
    @sort_mode      = 'popular',
    @comments_limit = 200,
    @session_id     = @sid OUTPUT;
SELECT @sid AS session_id;

-- Insertar comentarios (JSON exportado por la app):
EXEC dbo.usp_insert_comments_json
    @session_id  = @sid,
    @platform_id = 1,
    @json_data   = N'[{"author":"User1","text":"Great video!","votes":42,"reply_count":3,"time":"2025-04-15 10:30","cid":"UgxABC"}]';

-- Consultar todos los comentarios de una sesión:
SELECT * FROM dbo.vw_comments WHERE source_id = 'dQw4w9WgXcQ';

-- Top 10 comentarios con más likes en Reddit:
SELECT TOP 10 author, votes, body
FROM dbo.vw_comments
WHERE platform = 'Reddit'
ORDER BY votes DESC;
*/
