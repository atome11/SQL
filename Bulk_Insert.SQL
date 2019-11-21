USE [DB]
GO
/****** Object:  UserDefinedFunction [dbo].[null_cat]    Script Date: 20/11/2019 16:03:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************
Bulk insert data

Arguments
@path		-- path (won't work remotely)
@filename	-- filename
@table		-- table usually equal to filename
@schema		-- schema

Retuns
@SQLQuery -- to be executed in a statement

****************************************************************************/
ALTER FUNCTION [dbo].[P2P_BULK_INSERT] (@path NVARCHAR(500),
								@filename NVARCHAR(100),
								@table NVARCHAR(100),
								@schema AS NVARCHAR(100)
								)
RETURNS NVARCHAR(4000)
AS  
BEGIN
	DECLARE @SQLQuery AS NVARCHAR(4000);
	SET @SQLQuery = 'BULK INSERT '+@schema+'.'+@table+' FROM ''' + @path + @filename +'_1.txt'' 
			WITH (  
		  FIRSTROW = 2,
		  CODEPAGE=''1252'',
		  FIELDTERMINATOR = ''|***|'',  
		  ROWTERMINATOR = ''\n'' )'
	RETURN @SQLQuery
END;
