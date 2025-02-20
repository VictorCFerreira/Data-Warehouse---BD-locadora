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
    id INT IDENTITY(1,1) PRIMARY KEY,
    cidade NVARCHAR(100) NOT NULL
);

-- Dimensão: funcionário
CREATE TABLE funcionario (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL,
    id_localizacao INT NOT NULL,
    FOREIGN KEY (id_localizacao) REFERENCES localizacao(id)
);

-- Dimensão: cliente
CREATE TABLE cliente (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL,
    id_localizacao INT NOT NULL,
    FOREIGN KEY (id_localizacao) REFERENCES localizacao(id)
);

-- Dimensão: filme
CREATE TABLE filme (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nome NVARCHAR(200) NOT NULL
);

-- Dimensão: categoria
CREATE TABLE categoria (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nome NVARCHAR(100) NOT NULL
);


-- Dimensão: ator
CREATE TABLE ator (
    id INT IDENTITY(1,1) PRIMARY KEY,
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

CREATE PROCEDURE ETL_tempo
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

CREATE PROCEDURE ETL_categoria
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Inserir as categorias somente se não existirem
        INSERT INTO DW.dbo.categoria (nome)
        SELECT
            f.name AS nome
        FROM DBO.dbo.category f
        WHERE NOT EXISTS (
            SELECT 1
            FROM DW.dbo.categoria c
            WHERE c.nome = f.name
        );

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

CREATE PROCEDURE ETL_ator
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.ator (nome)
        SELECT
            CONCAT(a.first_name, ' ', a.last_name) AS nome
        FROM DBO.dbo.actor a   WHERE NOT EXISTS (
            SELECT 1
            FROM DW.dbo.ator dwa
            WHERE dwa.nome = CONCAT(a.first_name, ' ', a.last_name)
        );
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

CREATE PROCEDURE ETL_filme
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.filme (nome)
        SELECT
            f.title AS nome
        FROM DBO.dbo.film f
        WHERE NOT EXISTS (
            SELECT 1
            FROM DW.dbo.filme c
            WHERE c.nome = f.title
        );

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

CREATE PROCEDURE ETL_localizacao
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.localizacao (cidade)
        SELECT c.city AS cidade
        FROM dbo.dbo.city c
        WHERE NOT EXISTS (
            SELECT 1 FROM DW.dbo.localizacao dwl WHERE dwl.cidade = c.city
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

EXEC ETL_localizacao;

SELECT * FROM DW.dbo.localizacao;
--------------------------------------------------------------------------------------

-- CLIENTE
CREATE PROCEDURE ETL_cliente
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.cliente (nome, id_localizacao)
        SELECT CONCAT(c.first_name, ' ', c.last_name) AS nome, dwl.id AS id_localizacao
        FROM dbo.dbo.customer c
        INNER JOIN dbo.dbo.address a ON c.address_id = a.address_id
        INNER JOIN dbo.dbo.city ci ON a.city_id = ci.city_id
        INNER JOIN DW.dbo.localizacao dwl ON ci.city = dwl.cidade
        WHERE NOT EXISTS (
            SELECT 1 FROM DW.dbo.cliente cl WHERE cl.nome = CONCAT(c.first_name, ' ', c.last_name)
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

EXEC ETL_cliente;

SELECT * FROM DW.dbo.cliente;

-----------------------------------------------------------------------------------------------------

—- FUNCIONÁRIO

CREATE PROCEDURE ETL_Funcionario
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.funcionario (nome, id_localizacao)
        SELECT CONCAT(s.first_name, ' ', s.last_name) AS nome, dwl.id AS id_localizacao
        FROM dbo.dbo.staff s
        INNER JOIN dbo.dbo.address a ON s.address_id = a.address_id
        INNER JOIN dbo.dbo.city ci ON a.city_id = ci.city_id
        INNER JOIN DW.dbo.localizacao dwl ON ci.city = dwl.cidade
        WHERE NOT EXISTS (
            SELECT 1 FROM DW.dbo.funcionario f WHERE f.nome = CONCAT(s.first_name, ' ', s.last_name)
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

EXEC ETL_Funcionario;

SELECT * FROM DW.dbo.funcionario;
