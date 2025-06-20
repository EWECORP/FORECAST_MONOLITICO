USE [data-sync]
GO

DECLARE	@return_value int

EXEC	@return_value = [dbo].[SP_CNX_M_3_ARTICULOS]

SELECT	'Return Value' = @return_value

GO


ALTER DATABASE [data-sync] SET RECOVERY SIMPLE;
DBCC SHRINKFILE (data_sync_log, 1);
ALTER DATABASE [data-sync] SET RECOVERY FULL;


USE [data-sync];
GO
EXEC sp_helppublication;

SELECT 
    p.publication,
    a.article,
    a.source_object,
    s.subscriber,
    s.subscriber_db
FROM distribution.dbo.MSpublications p
JOIN distribution.dbo.MSarticles a ON p.publication_id = a.publication_id
JOIN distribution.dbo.MSsubscriptions s ON a.article_id = s.article_id
WHERE p.publisher_db = 'data-sync';


EXEC sp_who2

