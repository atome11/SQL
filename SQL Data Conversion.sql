/****************************************************************************
Created by Jordan M.
****************************************************************************/
CREATE SCHEMA clean
GO


CREATE FUNCTION [clean].[PARSE_DECIMAL] 
( @String AS VARCHAR(max) )
RETURNS DECIMAL(18,6)
    AS
    BEGIN 

	/****************************************************************************
	Script permettant de convertir une chaîne de caractères en décimal 
	selon plusieurs formats possibles
	****************************************************************************/
		
		DECLARE @Sign VARCHAR(1);
		SET @Sign = CASE WHEN CHARINDEX('-', @String) > 0 THEN '-' ELSE '' END;

		SET @String = REPLACE(REPLACE(LTRIM(RTRIM(@String)), '-', ''), ' ', '');
		
		DECLARE @LastIndexComma INT;
		SET @LastIndexComma = CHARINDEX(',', REVERSE(@String));
		
		DECLARE @LastIndexPoint INT;
		SET @LastIndexPoint = CHARINDEX('.', REVERSE(@String));

		IF (@LastIndexComma > 0 AND @LastIndexPoint > 0)
		BEGIN
			IF (@LastIndexComma < @LastIndexPoint) --Séparateur décimal virgule, séparateur millier point
			BEGIN
				SET @String = REPLACE(@String, '.', '');
				SET @String = REPLACE(@String, ',', '.');
			END
			ELSE --Séparateur décimal point, séparateur millier virgule
			BEGIN
				SET @String = REPLACE(@String, ',', '')
			END;

		END;
		
		SET @String = @Sign + REPLACE(@String, ',', '.')


        RETURN 	TRY_CONVERT(DECIMAL(18,6), @String)
    END

GO



---------------------------------------------------------------------------------------------------------------------------------------------------




CREATE PROCEDURE [clean].[Detect_Column_Datatypes]
	@Schema sysname, --Définit le schéma de la table à tester
	@Table sysname, --Définit le nom de la table à tester
	@TopNRows INT, --Définit le nombre de lignes à tester dabs la table
	@MinSuccessRate DECIMAL(3,2) --Définit le taux de conversion minimum acceptable pour choisir le type de données
AS

/****************************************************************************
	Script permettant de détecter les colonnes convertibles en datetime ou numeric
	dans une table, et le format de date

	Les résultats sont inscrits dans la table clean.DATA_CONVERSION

****************************************************************************/


DECLARE @SQL NVARCHAR(MAX); --contient la requête SQL dynamique


/****************************************************************************
	Etape 1 : test de conversion en date
****************************************************************************/


/****************************************************************************
	Table des différents formats de date à tester
****************************************************************************/
IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'DATE_FORMATS'
) 
BEGIN 
       DROP TABLE clean.DATE_FORMATS 
END

CREATE TABLE clean.DATE_FORMATS(DATE_FORMAT INT);
INSERT INTO clean.DATE_FORMATS(DATE_FORMAT)
SELECT 100 AS DATE_FORMAT UNION ALL
SELECT 101 AS DATE_FORMAT UNION ALL 
SELECT 102 AS DATE_FORMAT UNION ALL 
SELECT 103 AS DATE_FORMAT UNION ALL 
SELECT 104 AS DATE_FORMAT UNION ALL 
SELECT 105 AS DATE_FORMAT UNION ALL 
SELECT 106 AS DATE_FORMAT UNION ALL 
SELECT 107 AS DATE_FORMAT UNION ALL 
SELECT 108 AS DATE_FORMAT UNION ALL 
SELECT 109 AS DATE_FORMAT UNION ALL 
SELECT 110 AS DATE_FORMAT UNION ALL 
SELECT 111 AS DATE_FORMAT UNION ALL 
SELECT 112 AS DATE_FORMAT UNION ALL 
SELECT 113 AS DATE_FORMAT UNION ALL 
SELECT 114 AS DATE_FORMAT UNION ALL 
SELECT 120 AS DATE_FORMAT UNION ALL 
SELECT 121 AS DATE_FORMAT UNION ALL 
SELECT 126 AS DATE_FORMAT UNION ALL 
SELECT 127 AS DATE_FORMAT UNION ALL 
SELECT 130 AS DATE_FORMAT UNION ALL 
SELECT 131 AS DATE_FORMAT;





/****************************************************************************
	On réalise le test sur les N premières lignes de la table
****************************************************************************/
IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'TopN'
) 
BEGIN 
       DROP TABLE clean.TopN 
END


SET @SQL = N'SELECT TOP ' + CAST(@TopNRows AS NVARCHAR(10)) + ' * INTO clean.[TopN] FROM [' + @Schema + '].[' + @Table + ']'

EXECUTE sp_executesql @SQL;





/****************************************************************************
	Table de résultat des tests, retourne les colonnes 
****************************************************************************/
IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'DATA_CONVERSION'
) 
BEGIN 
       DROP TABLE clean.DATA_CONVERSION 
END


CREATE TABLE clean.DATA_CONVERSION(
	COLUMN_ID INT,
	SCHEMA_NAME VARCHAR(128),
	TABLE_NAME VARCHAR(128),
	COLUMN_NAME VARCHAR(128),
	DATA_TYPE VARCHAR(128),
	FORMAT VARCHAR(128),
	SUCCESS_RATE DECIMAL(18,6)
)

SET @SQL = N'INSERT INTO clean.DATA_CONVERSION (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE, FORMAT, SUCCESS_RATE)' 
	+ N'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ''DATETIME'' AS DATA_TYPE, DATE_FORMAT, SUCCESS_RATE FROM ('
	+ N'SELECT ROW_NUMBER() OVER(PARTITION BY SCHEMA_NAME, COLUMN_NAME ORDER BY SUCCESS_RATE DESC, DATE_FORMAT) SCORE, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME,	DATE_FORMAT, SUCCESS_RATE FROM ( ';

SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 10)
FROM
(
	SELECT DISTINCT ((
		SELECT N'SELECT '''
			+ @Schema
			+ N''' AS SCHEMA_NAME, '''
			+ @Table
			+ N''' AS TABLE_NAME, '''
			+ sc.name
			+ N''' AS COLUMN_NAME, '
			+ N'D.DATE_FORMAT, '
			+ N'COUNT(TRY_CONVERT(DATETIME, P.['
			+ sc.name
			+ N'], D.DATE_FORMAT)) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 1.0 AS SUCCESS_RATE '
			+ N'FROM [clean].[DATE_FORMATS] D, [clean].[TopN] P ' 
			+ N'WHERE LEN(LTRIM(RTRIM(P.['
			+ sc.name
			+ N']))) >= 8 '
			+ N'GROUP BY D.DATE_FORMAT  UNION ALL '
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		WHERE t.name = 'TopN'
		AND s.name = 'clean'
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A;

SET @SQL += N' ) A WHERE SUCCESS_RATE > ' + CAST(@MinSuccessRate AS NVARCHAR(6)) + N' ) B WHERE SCORE = 1';


EXECUTE sp_executesql @SQL;





/****************************************************************************
	Etape 2 : test de conversion en int
****************************************************************************/


SET @SQL = N'INSERT INTO clean.DATA_CONVERSION (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE, FORMAT, SUCCESS_RATE)' 
	+ N'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ''INT'' AS DATA_TYPE, '''' AS FORMAT, SUCCESS_RATE FROM ('
	+ N'SELECT ROW_NUMBER() OVER(PARTITION BY SCHEMA_NAME, COLUMN_NAME ORDER BY SUCCESS_RATE DESC) SCORE, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, SUCCESS_RATE FROM ( ';

SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 10)
FROM
(
	SELECT DISTINCT ((
		SELECT N'SELECT '''
			+ @Schema
			+ N''' AS SCHEMA_NAME, '''
			+ @Table
			+ N''' AS TABLE_NAME, '''
			+ sc.name
			+ N''' AS COLUMN_NAME, '
			+ N'COUNT(TRY_CONVERT(INT, P.['
			+ sc.name
			+ N'])) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 1.0 AS SUCCESS_RATE '
			+ N'FROM [clean].[TopN] P ' 
			+ N'WHERE LEN(LTRIM(RTRIM(P.['
			+ sc.name
			+ N']))) >= 1 '
			+ N'UNION ALL '
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		LEFT JOIN clean.DATA_CONVERSION dc
		ON @Schema = dc.SCHEMA_NAME
		AND @Table = dc.TABLE_NAME
		AND sc.name = dc.COLUMN_NAME
		WHERE t.name = 'TopN'
		AND s.name = 'clean'
		AND dc.COLUMN_NAME IS NULL
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

SET @SQL += N' ) A WHERE SUCCESS_RATE > ' + CAST(@MinSuccessRate AS NVARCHAR(6)) + N' ) B WHERE SCORE = 1';


EXECUTE sp_executesql @SQL;





/****************************************************************************
	Etape 3 : test de conversion en bigint
****************************************************************************/


SET @SQL = N'INSERT INTO clean.DATA_CONVERSION (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE, FORMAT, SUCCESS_RATE)' 
	+ N'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ''BIGINT'' AS DATA_TYPE, '''' AS FORMAT, SUCCESS_RATE FROM ('
	+ N'SELECT ROW_NUMBER() OVER(PARTITION BY SCHEMA_NAME, COLUMN_NAME ORDER BY SUCCESS_RATE DESC) SCORE, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, SUCCESS_RATE FROM ( ';

SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 10)
FROM
(
	SELECT DISTINCT ((
		SELECT N'SELECT '''
			+ @Schema
			+ N''' AS SCHEMA_NAME, '''
			+ @Table
			+ N''' AS TABLE_NAME, '''
			+ sc.name
			+ N''' AS COLUMN_NAME, '
			+ N'COUNT(TRY_CONVERT(BIGINT, P.['
			+ sc.name
			+ N'])) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 1.0 AS SUCCESS_RATE '
			+ N'FROM [clean].[TopN] P ' 
			+ N'WHERE LEN(LTRIM(RTRIM(P.['
			+ sc.name
			+ N']))) >= 1 '
			+ N'UNION ALL '
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		LEFT JOIN clean.DATA_CONVERSION dc
		ON @Schema = dc.SCHEMA_NAME
		AND @Table = dc.TABLE_NAME
		AND sc.name = dc.COLUMN_NAME
		WHERE t.name = 'TopN'
		AND s.name = 'clean'
		AND dc.COLUMN_NAME IS NULL
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

SET @SQL += N' ) A WHERE SUCCESS_RATE > ' + CAST(@MinSuccessRate AS NVARCHAR(6)) + N' ) B WHERE SCORE = 1';


EXECUTE sp_executesql @SQL;





/****************************************************************************
	Etape 4 : test de conversion en decimal
****************************************************************************/


SET @SQL = N'INSERT INTO clean.DATA_CONVERSION (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE, FORMAT, SUCCESS_RATE)' 
	+ N'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ''DECIMAL'' AS DATA_TYPE, ''18,6'' AS FORMAT, SUCCESS_RATE FROM ('
	+ N'SELECT ROW_NUMBER() OVER(PARTITION BY SCHEMA_NAME, COLUMN_NAME ORDER BY SUCCESS_RATE DESC) SCORE, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, SUCCESS_RATE FROM ( ';

SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 10)
FROM
(
	SELECT DISTINCT ((
		SELECT N'SELECT '''
			+ @Schema
			+ N''' AS SCHEMA_NAME, '''
			+ @Table
			+ N''' AS TABLE_NAME, '''
			+ sc.name
			+ N''' AS COLUMN_NAME, '
			+ N'COUNT([clean].[PARSE_DECIMAL]( P.['
			+ sc.name
			+ N'])) / CASE WHEN COUNT(*) = 0 THEN 1.0 ELSE COUNT(*) END * 1.0 AS SUCCESS_RATE '
			+ N'FROM [clean].[TopN] P ' 
			+ N'WHERE LEN(LTRIM(RTRIM(P.['
			+ sc.name
			+ N']))) >= 1 '
			+ N'UNION ALL '
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		LEFT JOIN clean.DATA_CONVERSION dc
			ON @Schema = dc.SCHEMA_NAME
			AND @Table = dc.TABLE_NAME
			AND sc.name = dc.COLUMN_NAME
		WHERE t.name = 'TopN'
		AND s.name = 'clean'
		AND dc.COLUMN_NAME IS NULL
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

SET @SQL += N' ) A WHERE SUCCESS_RATE > ' + CAST(@MinSuccessRate AS NVARCHAR(6)) + N' ) B WHERE SCORE = 1';


EXECUTE sp_executesql @SQL;





/****************************************************************************
	Etape 5 : dimensionnement des varchar
****************************************************************************/


SET @SQL = N'INSERT INTO clean.DATA_CONVERSION (SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE, FORMAT, SUCCESS_RATE)' 
	+ N'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ''VARCHAR'' AS DATA_TYPE, CAST(CASE 
		WHEN FORMAT <= 10 THEN 10
		WHEN FORMAT <= 20 THEN 20
		WHEN FORMAT <= 50 THEN 50
		WHEN FORMAT <= 100 THEN 100
		WHEN FORMAT <= 200 THEN 200
		ELSE 1000
	END AS VARCHAR(4)) AS FORMAT, 1 AS SUCCESS_RATE FROM (';

SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 10)
FROM
(
	SELECT DISTINCT ((
		SELECT N'SELECT ''' 
			+ @Schema
			+ ''' AS SCHEMA_NAME, '''
			+ @Table
			+ ''' AS TABLE_NAME, '''
			+ sc.name 
			+ ''' as COLUMN_NAME, MAX(LEN(LTRIM(RTRIM([' 
			+ sc.name 
			+ '])))) as FORMAT FROM [' 
			+ @Schema
			+ '].['
			+ @Table
			+ '] UNION ALL ' 
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		LEFT JOIN clean.DATA_CONVERSION dc
			ON @Schema = dc.SCHEMA_NAME
			AND @Table = dc.TABLE_NAME
			AND sc.name = dc.COLUMN_NAME
		WHERE t.name = 'TopN'
		AND s.name = 'clean'
		AND dc.COLUMN_NAME IS NULL
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

SET @SQL += N' ) A ';


EXECUTE sp_executesql @SQL;


/* Mise à jour du numéro de colonne, utilisé pour ordonner les colonnes telles que dans la table source */

update DC
set DC.COLUMN_ID =  COL.COLUMN_ID
from [clean].[DATA_CONVERSION] DC INNER JOIN (
SELECT s.name as schema_name, t.name as table_name, sc.name as column_name, sc.column_id
		FROM sys.tables AS t
		INNER JOIN sys.columns AS sc 
			ON t.object_id = sc.object_id
		INNER JOIN sys.schemas AS S
			ON T.schema_id = s.schema_id
		WHERE t.name = @Table
		AND s.name = @Schema
) COL
on DC.SCHEMA_NAME = COL.SCHEMA_NAME
and DC.TABLE_NAME = COL.TABLE_NAME
and DC.COLUMN_NAME = COL.COLUMN_NAME



IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'DATE_FORMATS'
) 
BEGIN 
	DROP TABLE clean.DATE_FORMATS;
END


/****************************************************************************
	Etape 6 : On liste les lignes pour lesquelles des erreurs de conversion ont été détectées
****************************************************************************/
  

IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'DATA_CONVERSION_ERRORS'
) 
BEGIN 
	DROP TABLE [clean].[DATA_CONVERSION_ERRORS];
END

CREATE TABLE [clean].[DATA_CONVERSION_ERRORS](
	[COLUMN_ID] [int] NULL,
	[SCHEMA_NAME] [varchar](128) NULL,
	[TABLE_NAME] [varchar](128) NULL,
	[COLUMN_NAME] [varchar](128) NULL,
	[DATA_TYPE] [varchar](128) NULL,
	[FORMAT] [varchar](128) NULL,
	[CONVERSION_ERROR_VALUE] [varchar](8000) NULL
);


SELECT @SQL = SQLQuery
FROM
(
	SELECT DISTINCT ((
		SELECT 
		   ' INSERT INTO [clean].[DATA_CONVERSION_ERRORS]([COLUMN_ID], [SCHEMA_NAME], [TABLE_NAME], [COLUMN_NAME], [DATA_TYPE], [FORMAT], [CONVERSION_ERROR_VALUE] ) ' 
		 + ' SELECT ' + CAST(dc.COLUMN_ID AS VARCHAR(20))
		 + ',''' + dc.[SCHEMA_NAME] + ''''
		 + ',''' + dc.TABLE_NAME + ''''
		 + ',''' + dc.COLUMN_NAME + ''''
		 + ',''' + dc.DATA_TYPE + ''''
		 + ',''' + dc.[FORMAT] + ''''
		 + ',[' + dc.[COLUMN_NAME] + '] AS CONVERSION_ERROR_VALUE'
		 + ' FROM [clean].[TopN] '
		 + ' WHERE '  +
			CASE
				WHEN dc.[DATA_TYPE] = 'DECIMAL' THEN 
					'[clean].[PARSE_DECIMAL](['
					+ 
					+ dc.[COLUMN_NAME]
					+ ']) '
				ELSE
					'TRY_CONVERT('
					+ dc.[DATA_TYPE]
					+ CASE 
						WHEN dc.[DATA_TYPE] = 'VARCHAR' THEN
							'(' + dc.[FORMAT] + ')'
						ELSE ''
					END 
					+ ',[' 
					+ dc.[COLUMN_NAME]
					+ ']'
					+
					+ ' ' 
					+ CASE 
						WHEN dc.[DATA_TYPE] = 'DATETIME' THEN
							', ' + dc.[FORMAT] 
						ELSE ''
					END
					+ ') '
				END
			+ ' IS NULL '
			+ ' AND [' + dc.[COLUMN_NAME] + '] IS NOT NULL '
		FROM clean.DATA_CONVERSION dc
		ORDER BY dc.COLUMN_ID
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

EXECUTE sp_executesql @SQL;

IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = 'TopN'
) 
BEGIN 
	DROP TABLE clean.TopN;
END


GO


---------------------------------------------------------------------------------------------------------------------------------------------------



CREATE PROCEDURE [clean].[Convert_Column_Datatypes]
	@Schema sysname, --Définit le schéma de la table à tester
	@Table sysname --Définit le nom de la table à tester
AS

/****************************************************************************
	Script permettant de convertir les données d'une table source dans une nouvelle table
	en fonction des paramètres spécifiés dans la table clean.DATA_CONVERSION générée par 
	la procedure stockée clean.Detect_Column_Datatypes

	Les résultats sont inscrits dans la table clean.[Nom table source]

****************************************************************************/

DECLARE @SQL NVARCHAR(MAX); --contient la requête SQL dynamique


/****************************************************************************
	Etape 7 : création de la table cleanée
****************************************************************************/
IF EXISTS 
(
        SELECT     * 
        FROM       INFORMATION_SCHEMA.TABLES 
        WHERE      TABLE_SCHEMA = 'clean' 
        AND        TABLE_NAME = LTRIM(RTRIM(@Table))
) 
BEGIN 
	SET @SQL = 'DROP TABLE [clean].[' + LTRIM(RTRIM(@Table)) + ']';
	EXECUTE sp_executesql @SQL;
END

/****************************************************************************
	Etape 8 : insertion des données dans la table cleanée
****************************************************************************/


SELECT @SQL = 'SELECT '
SELECT @SQL += LEFT(SQLQuery,  LEN(SQLQuery) - 1)
FROM
(
	SELECT DISTINCT ((
		SELECT 
			CASE
				WHEN dc.[DATA_TYPE] = 'DECIMAL' THEN 
					'[clean].[PARSE_DECIMAL](['
					+ 
					+ dc.[COLUMN_NAME]
					+ ']) ['
					+ LTRIM(RTRIM(dc.[COLUMN_NAME])) 
					+ '], '
				ELSE
					'TRY_CONVERT('
					+ dc.[DATA_TYPE]
					+ CASE 
						WHEN dc.[DATA_TYPE] = 'VARCHAR' THEN
							'(' + dc.[FORMAT] + ')'
						ELSE ''
					END 
					+ ',[' 
					+ dc.[COLUMN_NAME]
					+ ']'
					+
					+ ' ' 
					+ CASE 
						WHEN dc.[DATA_TYPE] = 'DATETIME' THEN
							', ' + dc.[FORMAT] 
						ELSE ''
					END
					+ ') ['
					+ LTRIM(RTRIM(dc.[COLUMN_NAME])) 
					+ '], '
				END
		FROM clean.DATA_CONVERSION dc
		ORDER BY dc.COLUMN_ID
		FOR XML PATH(''), TYPE)
	).value('.', 'NVARCHAR(MAX)') AS SQLQuery
) A

SET @SQL += ' INTO [clean].[' + LTRIM(RTRIM(@Table)) + ']'; 

SET @SQL += ' FROM [' + @Schema + '].[' + @Table + ']'; 

EXECUTE sp_executesql @SQL;

GO


