-- Criação de DW - Luan e Victor
-- Script de criação de todas as tabelas do DW:

-- validação por last update
------------------------------------------------------------------------------------

-- Dimensão: tempo
CREATE TABLE tempo (
    id INT IDENTITY(1,1) PRIMARY KEY,
    mes INT NOT NULL,
    ano INT NOT NULL
);

-- Dimensão: localizacao
CREATE TABLE localizacao (
    id INT PRIMARY KEY,
    cidade NVARCHAR(100) NOT NULL
);

-- Dimensão: funcionário
CREATE TABLE funcionario (
    id INT PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL,
    id_localizacao INT NOT NULL,
    FOREIGN KEY (id_localizacao) REFERENCES localizacao(id)
);

-- Dimensão: cliente
CREATE TABLE cliente (
    id INT PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL,
    id_localizacao INT NOT NULL,
    FOREIGN KEY (id_localizacao) REFERENCES localizacao(id)
);

-- Dimensão: filme
CREATE TABLE filme (
    id INT PRIMARY KEY,
    nome NVARCHAR(200) NOT NULL
);

-- Dimensão: categoria
CREATE TABLE categoria (
    id INT PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL
);

-- Dimensão: ator
CREATE TABLE ator (
    id INT PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL
);


-- Tabela Fato: empréstimo
CREATE TABLE emprestimo (
    id INT IDENTITY(1,1) PRIMARY KEY,
    id_tempo INT,
    id_localizacao INT,
    id_funcionario INT,
    id_cliente INT,
    id_filme INT,
    id_categoria INT,
    id_ator INT,
    quantidade_emprestimos INT,
    valor_total DECIMAL(10, 2),
    valor_total_nao_pago FLOAT,
    qtd_emprestimos_nao_pagos INT,
    FOREIGN KEY (id_tempo) REFERENCES tempo(id),
    FOREIGN KEY (id_localizacao) REFERENCES localizacao(id),
    FOREIGN KEY (id_funcionario) REFERENCES funcionario(id),
    FOREIGN KEY (id_cliente) REFERENCES cliente(id),
    FOREIGN KEY (id_filme) REFERENCES filme(id),
    FOREIGN KEY (id_categoria) REFERENCES categoria(id),
    FOREIGN KEY (id_ator) REFERENCES ator(id)
);

-------------------------------------------------- ETLS -------------------------------------------------------------------

--TEMPO

CREATE OR ALTER PROCEDURE ETL_tempo
AS
BEGIN
DECLARE @v_min_date DATETIME;
DECLARE @v_max_date DATETIME;
DECLARE @v_current_date DATETIME;

SELECT
    @v_min_date = MIN(rental_date),
    @v_max_date = MAX(rental_date)
FROM DBO.dbo.rental;

SET @v_current_date = @v_min_date;

WHILE @v_current_date <= @v_max_date
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM DW.dbo.tempo
        WHERE ano = YEAR(@v_current_date)
        AND mes = MONTH(@v_current_date)
    )
    BEGIN
        INSERT INTO DW.dbo.tempo (ano, mes)
        VALUES (
            YEAR(@v_current_date),
            MONTH(@v_current_date)
        );
    END

    SET @v_current_date = DATEADD(MONTH, 1, @v_current_date);
END
END

exec ETL_tempo;

select * from DW.dbo.tempo;

------------------------------------------------------------------------------------

--CATEGORIA

CREATE OR ALTER PROCEDURE ETL_categoria
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.categoria AS target
        USING (
            SELECT category_id AS id, name AS nome FROM DBO.dbo.category
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND target.nome <> source.nome THEN
            UPDATE SET target.nome = source.nome
        WHEN NOT MATCHED THEN
            INSERT (id, nome) VALUES (source.id, source.nome);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

exec ETL_categoria;

select * from DW.dbo.categoria;


------------------------------------------------------------------------------------

--ATOR

CREATE OR ALTER PROCEDURE ETL_ator
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.ator AS target
        USING (
            SELECT actor_id AS id, CONCAT(first_name, ' ', last_name) AS nome FROM DBO.dbo.actor
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND target.nome <> source.nome THEN
            UPDATE SET target.nome = source.nome
        WHEN NOT MATCHED THEN
            INSERT (id, nome) VALUES (source.id, source.nome);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;


exec ETL_ator;

select * from DW.dbo.ator;


------------------------------------------------------------------------------------

--FILME
CREATE OR ALTER PROCEDURE ETL_filme
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.filme AS target
        USING (
            SELECT film_id AS id, title AS nome FROM DBO.dbo.film
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND target.nome <> source.nome THEN
            UPDATE SET target.nome = source.nome
        WHEN NOT MATCHED THEN
            INSERT (id, nome) VALUES (source.id, source.nome);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;


exec ETL_filme;

select * from DW.dbo.filme;


------------------------------------------------------------------------------------

-- LOCALIZAÇÃO

CREATE OR ALTER PROCEDURE ETL_localizacao
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.localizacao AS target
        USING (
            SELECT 
                c.city_id AS id, 
                CONCAT(c.city, ' - ', co.country) AS cidade
            FROM dbo.dbo.city c
            INNER JOIN dbo.dbo.country co ON c.country_id = co.country_id
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND target.cidade <> source.cidade THEN
            UPDATE SET target.cidade = source.cidade
        WHEN NOT MATCHED THEN
            INSERT (id, cidade) VALUES (source.id, source.cidade);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;


exec ETL_localizacao;

select * from DW.dbo.localizacao;

--------------------------------------------------------------------------------------

-- CLIENTE
CREATE OR ALTER PROCEDURE ETL_cliente
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.cliente AS target
        USING (
            SELECT c.customer_id AS id, 
                   CONCAT(c.first_name, ' ', c.last_name) AS nome, 
                   ci.city_id AS id_localizacao
            FROM dbo.dbo.customer c
            INNER JOIN dbo.dbo.address a ON c.address_id = a.address_id
            INNER JOIN dbo.dbo.city ci ON a.city_id = ci.city_id
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND (target.nome <> source.nome OR target.id_localizacao <> source.id_localizacao) THEN
            UPDATE SET target.nome = source.nome, target.id_localizacao = source.id_localizacao
        WHEN NOT MATCHED THEN
            INSERT (id, nome, id_localizacao) VALUES (source.id, source.nome, source.id_localizacao);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;


exec ETL_cliente;

select * from DW.dbo.cliente;

-----------------------------------------------------------------------------------------------------

-- FUNCIONÁRIO

CREATE OR ALTER PROCEDURE ETL_Funcionario
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        MERGE INTO DW.dbo.funcionario AS target
        USING (
            SELECT s.staff_id AS id, 
                   CONCAT(s.first_name, ' ', s.last_name) AS nome, 
                   ci.city_id AS id_localizacao
            FROM dbo.dbo.staff s
            INNER JOIN dbo.dbo.address a ON s.address_id = a.address_id
            INNER JOIN dbo.dbo.city ci ON a.city_id = ci.city_id
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND (target.nome <> source.nome OR target.id_localizacao <> source.id_localizacao) THEN
            UPDATE SET target.nome = source.nome, target.id_localizacao = source.id_localizacao
        WHEN NOT MATCHED THEN
            INSERT (id, nome, id_localizacao) VALUES (source.id, source.nome, source.id_localizacao);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

exec ETL_Funcionario;

select * from DW.dbo.funcionario;

