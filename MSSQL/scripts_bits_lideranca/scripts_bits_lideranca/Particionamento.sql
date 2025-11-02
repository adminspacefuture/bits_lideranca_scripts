

/*

INDECIS GLOBAIS
INDICES LOCAIS
PARTICIONAMENTO DE INDICES
DROP DE PARTICIONS - EXPURGOS

*/

USE LAB_BITS_LIDERANCA
GO


SELECT * FROM SYS.partition_functions
GO
SELECT * FROM SYS.partition_parameters
GO
SELECT * FROM SYS.partition_range_values
GO
SELECT * FROM SYS.partition_schemes
GO
SELECT * FROM SYS.partitions
GO
SELECT * FROM SYS.destination_data_spaces
GO
SELECT * FROM sys.objects
GO
SELECT * FROM SYS.allocation_units
GO
SELECT * FROM sys.dm_db_partition_stats


SELECT * FROM sys.objects o WHERE o.type = 'U';

SELECT 
    o.name AS Tabela,
    i.name AS Indice,
    ps.name AS EsquemaParticao,
    pf.name AS FuncaoParticao
FROM sys.indexes i
JOIN sys.objects o ON i.object_id = o.object_id
JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
WHERE o.type = 'U';

SELECT 
    o.name AS Tabela,
    p.partition_number,
    p.rows
FROM sys.partitions p
JOIN sys.objects o ON p.object_id = o.object_id
WHERE o.type = 'U'
ORDER BY o.name, p.partition_number;




USE master










-- ###################################33






--ALTER DATABASE LAB_BITS_LIDERANCA SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--DROP DATABASE LAB_BITS_LIDERANCA;


CREATE DATABASE LAB_BITS_LIDERANCA
ON PRIMARY
(
	NAME = N'LAB_BITS_LIDERANCA_DATA',
	FILENAME = N'D:\Dev\SGBD\MSSQL\DISCO\DATA\LAB_BITS_LIDERANCA.mdf'
)
LOG ON
(

	NAME = N'LAB_BITS_LIDERANCA_LOG',
	FILENAME = N'D:\Dev\SGBD\MSSQL\DISCO\LOG\LAB_BITS_LIDERANCA.ldf'
)
GO


USE LAB_BITS_LIDERANCA
GO

 
ALTER DATABASE LAB_BITS_LIDERANCA ADD FILEGROUP fglabbitslideranca01;
ALTER DATABASE LAB_BITS_LIDERANCA ADD FILEGROUP fglabbitslideranca02;
ALTER DATABASE LAB_BITS_LIDERANCA ADD FILEGROUP fglabbitslideranca03;
ALTER DATABASE LAB_BITS_LIDERANCA ADD FILEGROUP fglabbitslideranca04;
ALTER DATABASE LAB_BITS_LIDERANCA ADD FILEGROUP fglabbitslideranca05;

--LIMPA OS DADOS DO ARQUIVO
--DBCC SHRINKFILE (partlabbitslideranca01, EMPTYFILE);
--DBCC SHOW_STATISTICS('Vendas', 'VENDAS_PK')

ALTER DATABASE LAB_BITS_LIDERANCA
ADD FILE
(
	NAME = partlabbitslideranca01,
	FILENAME =  N'D:\Dev\SGBD\MSSQL\DISCO\DATA\partlabbitslideranca01.ndf'
) TO FILEGROUP fglabbitslideranca01;
GO

ALTER DATABASE LAB_BITS_LIDERANCA
ADD FILE
(
	NAME = partlabbitslideranca02,
	FILENAME =  N'D:\Dev\SGBD\MSSQL\DISCO\DATA\partlabbitslideranca02.ndf'
)  TO FILEGROUP fglabbitslideranca02;
GO




ALTER DATABASE LAB_BITS_LIDERANCA
ADD FILE
(
	NAME = partlabbitslideranca03,
	FILENAME =  N'D:\Dev\SGBD\MSSQL\DISCO\DATA\partlabbitslideranca03.ndf'
) TO FILEGROUP fglabbitslideranca03;
GO


ALTER DATABASE LAB_BITS_LIDERANCA
ADD FILE
(
	NAME = partlabbitslideranca04,
	FILENAME =  N'D:\Dev\SGBD\MSSQL\DISCO\DATA\partlabbitslideranca04.ndf'
)TO FILEGROUP fglabbitslideranca04;
GO

ALTER DATABASE LAB_BITS_LIDERANCA 
ADD FILE	
(
	NAME = partlabbitslideranca05,
	FILENAME = N'D:\Dev\SGBD\MSSQL\DISCO\DATA\partlabbitslideranca05.ndf'
) TO FILEGROUP fglabbitslideranca05;
GO


CREATE PARTITION FUNCTION pf_VendasPorAno (DATE)
AS RANGE RIGHT FOR VALUES (
'2021-01-01', 
'2022-01-01',
'2023-01-01', 
'2024-01-01', 
'2025-01-01');


CREATE PARTITION SCHEME ps_VendasPorAno
AS PARTITION pf_VendasPorAno
TO (
	fglabbitslideranca01,
	fglabbitslideranca02,
	fglabbitslideranca03,
	fglabbitslideranca04,
	fglabbitslideranca05,
	[PRIMARY]
);
GO
/*
-- PARTITION SCHEME
ALL TO - TODOS VÃO CRIAR EM UMA SÓ FILEGROUP
TO - PARA CADA PARTIÇÃO PRECISA TER UM FILEGROUP

*/


SELECT * FROM SYS.filegroups
SELECT * FROM SYS.database_files


DROP TABLE DBO.Vendas;
-- Criar tabela base de exemplo (sem partição ainda)
CREATE TABLE Vendas (
    IdVenda INT IDENTITY(1,1) NOT NULL,
    DataVenda DATE NOT NULL,
    Valor DECIMAL(10,2) NOT NULL,
    ClienteId INT NOT NULL,
    ProdutoId INT NOT NULL,
	CONSTRAINT VENDAS_PK PRIMARY KEY CLUSTERED (DataVenda,IdVenda)
)
 ON ps_VendasPorAno(DataVenda);
GO


-- Inserir 1 milhão de registros com datas aleatórias entre 2020 e 2025
WITH Gerador AS (
    SELECT TOP (2000000)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM vendas  a
    CROSS JOIN sys.objects b
)
INSERT INTO Vendas (DataVenda, Valor, ClienteId, ProdutoId)
SELECT 
    DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 1825, '2020-01-01') AS DataVenda, -- datas entre 2020 e 2025
    CAST(50 + (RAND(CHECKSUM(NEWID())) * 450) AS DECIMAL(10,2)) AS Valor,   -- valores aleatórios entre 50 e 500
    ABS(CHECKSUM(NEWID())) % 1000 + 1 AS ClienteId,                         -- 1000 clientes
    ABS(CHECKSUM(NEWID())) % 200 + 1 AS ProdutoId                           -- 200 produtos
FROM Gerador;
GO

SELECT COUNT(1) FROM Vendas;

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO
SELECT * FROM Vendas WHERE DataVenda = '2022-01-01';
GO
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;


SET SHOWPLAN_TEXT ON;
GO
SELECT * FROM Vendas WHERE DataVenda = '2022-01-01';
GO
SET SHOWPLAN_TEXT OFF;

 
  -- https://chatgpt.com/c/690665cf-94f0-832a-a07a-cff4f6730766
  SELECT 
    $PARTITION.pf_VendasPorAno(DataVenda) AS ParticaoUsada,
    COUNT(*) AS Linhas
FROM Vendas
WHERE DataVenda = '2022-01-01'
GROUP BY $PARTITION.pf_VendasPorAno(DataVenda);

USE LAB_BITS_LIDERANCA

SELECT * FROM Vendas


SELECT 
    $PARTITION.pf_VendasPorAno(DataVenda) AS ParticaoUsada,
	year(DataVenda) as y,
    count(1)
FROM Vendas
-- WHERE DataVenda = '2024-02-01'
GROUP BY $PARTITION.pf_VendasPorAno(DataVenda),
year(DataVenda)
ORDER BY 1;


-- Ver partições com nome do filegroup e boundaries
SELECT 
    pf.name AS FuncaoParticao,
    prv.boundary_id AS NumParticao,
    fg.name AS Filegroup,
    prv.value AS LimiteInferior
FROM sys.partition_functions pf
JOIN sys.partition_range_values prv 
    ON pf.function_id = prv.function_id
JOIN sys.partition_schemes ps 
    ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces dds 
    ON ps.data_space_id = dds.partition_scheme_id 
   AND prv.boundary_id + 1 = dds.destination_id
JOIN sys.filegroups fg 
    ON dds.data_space_id = fg.data_space_id
WHERE pf.name = 'pf_VendasPorAno'
ORDER BY NumParticao;

WITH Particoes AS (
    SELECT 
        $PARTITION.pf_VendasPorAno(DataVenda) AS NumParticao,
        YEAR(DataVenda) AS Ano,
        COUNT(*) AS Qtde
    FROM Vendas
    GROUP BY $PARTITION.pf_VendasPorAno(DataVenda), YEAR(DataVenda)
)
SELECT 
    p.NumParticao,
    f.name AS Filegroup,
    pf.name AS FuncaoParticao,
    prv.value AS LimiteInferior,
    p.Ano,
    p.Qtde
FROM Particoes p
LEFT JOIN sys.partition_functions pf 
    ON pf.name = 'pf_VendasPorAno'
LEFT JOIN sys.partition_range_values prv 
    ON pf.function_id = prv.function_id 
   AND p.NumParticao = prv.boundary_id + 1
LEFT JOIN sys.partition_schemes ps 
    ON ps.function_id = pf.function_id
LEFT JOIN sys.destination_data_spaces dds 
    ON ps.data_space_id = dds.partition_scheme_id 
   AND p.NumParticao = dds.destination_id
LEFT JOIN sys.filegroups f 
    ON dds.data_space_id = f.data_space_id
ORDER BY p.NumParticao;






