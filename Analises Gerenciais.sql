
-- Análises do DW - Luan e Victor


-- 01 - Análise de quantidade de empréstimos realizados agrupado por filme, categoria ao longo do tempo.
-- Por numero de alugueis agrupado por filme, categoria ao longo do tempo

--Índice adicionado para rental.rental_date pois é um campo não primário que recebe 
-- muitas buscas e joins , visto que muitas das análises são baseadas em intervalos de
-- tempo
CREATE INDEX idx_rental_date ON dbo.dbo.rental(rental_date);


CREATE OR ALTER PROCEDURE ETL_alugueisPorFilmeCategoriaTempo
AS BEGIN BEGIN TRANSACTION;
BEGIN TRY
INSERT INTO
    DW.dbo.emprestimo (quantidade_emprestimos, id_filme,id_categoria, id_tempo)
SELECT
    COUNT(r.rental_id) AS quantidade_emprestimos,
    f.film_id AS id_filme,
	c.category_id AS id_categoria,
    t.id AS id_tempo
FROM
    DW.dbo.tempo t
    LEFT JOIN dbo.dbo.rental r ON MONTH (r.rental_date) = t.mes
    AND YEAR (r.rental_date) = t.ano
    LEFT JOIN dbo.dbo.inventory i ON i.inventory_id = r.inventory_id
    LEFT JOIN dbo.dbo.film f ON i.film_id = f.film_id
	LEFT JOIN dbo.dbo.film_category fc ON fc.film_id = f.film_id
	LEFT JOIN dbo.dbo.category c ON c.category_id = fc.category_id
	WHERE c.category_id is not null and f.film_id is not null
GROUP BY
    f.film_id,
	c.category_id,
    t.id
	COMMIT TRANSACTION;
END TRY
BEGIN CATCH
	ROLLBACK TRANSACTION;
	THROW;
END CATCH
END;

exec ETL_alugueisPorFilmeCategoriaTempo;

select  f.nome, c.nome , e.quantidade_emprestimos, t.mes, t.ano from DW.dbo.emprestimo e
INNER JOIN DW.dbo.filme f on e.id_filme = f.id
INNER JOIN DW.dbo.tempo t on e.id_tempo = t.id
INNER JOIN DW.dbo.categoria c on e.id_categoria = c.id
where id_filme is not null and id_tempo is not null and id_categoria is not null
order by f.nome,c.nome, t.mes, t.ano;






-----------------------------------------------------------------------------------------------

-- 02 - Quantidade e valor total de empréstimos, possibilitando uma visão hierárquica ao longo do tempo
--total por tempo de alugueis e valor pago

----Índice adicionado para payment.amount pois é um campo não chave primária que recebe 
-- muitas buscas, baseando as análises em valores pagos/nao pagos 
CREATE INDEX idx_payment_amount ON dbo.dbo.payment(amount);


CREATE OR ALTER PROCEDURE ETL_alugueisValorPorTempo
AS BEGIN BEGIN TRANSACTION;
BEGIN TRY
INSERT INTO
    DW.dbo.emprestimo (quantidade_emprestimos,valor_total, id_tempo)
SELECT
    COUNT(r.rental_id) AS quantidade_emprestimos,
    COALESCE(SUM(p.amount), 0) AS valor_total,
    t.id AS id_tempo
FROM
    DW.dbo.tempo t
    LEFT JOIN dbo.dbo.rental r ON MONTH (r.rental_date) = t.mes
    AND YEAR (r.rental_date) = t.ano
	LEFT JOIN dbo.dbo.payment p ON p.rental_id = r.rental_id
group by t.id
	COMMIT TRANSACTION;
END TRY
BEGIN CATCH
	ROLLBACK TRANSACTION;
	THROW;
END CATCH
END;

exec ETL_alugueisValorPorTempo;

select t.mes, t.ano, e.quantidade_emprestimos, e.valor_total from DW.dbo.emprestimo e
INNER JOIN DW.dbo.tempo t on t.id = e.id_tempo
where e.quantidade_emprestimos is not null and e.valor_total is not null and e.id_tempo is not null
order by t.ano, t.mes;



-----------------------------------------------------------------------------------------------

-- 03 - Análise de atores que mais tiveram filmes seus emprestados e categoria do filme emprestado,
-- com quantidade acumulada, valor acumulado e média no período.
CREATE OR ALTER PROCEDURE ETL_alugueisFilmesAtorCategoriaTempo
AS BEGIN
BEGIN TRANSACTION;
BEGIN TRY
	INSERT INTO
	    DW.dbo.emprestimo (id_ator,id_categoria, id_tempo,quantidade_emprestimos, valor_total)
SELECT a.actor_id as id_ator, c.category_id as id_categoria, t.id, count( DISTINCT r.rental_id) as quantidade_emprestimos, SUM(p.amount) AS valor_total
FROM 	DW.dbo.tempo t
	LEFT JOIN dbo.dbo.rental r ON MONTH (r.rental_date) = t.mes
    AND YEAR (r.rental_date) = t.ano
	LEFT JOIN dbo.dbo.payment p ON r.rental_id = p.rental_id
	INNER JOIN dbo.dbo.inventory i ON i.inventory_id = r.inventory_id
    INNER JOIN dbo.dbo.film f ON i.film_id = f.film_id
	INNER JOIN dbo.dbo.film_category fc ON fc.film_id = f.film_id
	INNER JOIN dbo.dbo.category c ON c.category_id = fc.category_id
	INNER JOIN dbo.dbo.film_actor fa ON fa.film_id = f.film_id
	INNER JOIN dbo.dbo.actor a ON a.actor_id = fa.actor_id
GROUP BY
    a.actor_id,
	c.category_id,
	t.id
COMMIT TRANSACTION
END TRY
BEGIN CATCH
	ROLLBACK TRANSACTION;
	THROW;
END CATCH
END;
exec ETL_alugueisFilmesAtorCategoriaTempo;


select a.nome,c.nome, t.ano,t.mes, quantidade_emprestimos, COALESCE(valor_total, 0) as valor from DW.dbo.emprestimo e
INNER JOIN DW.dbo.ator a on a.id = e.id_ator
INNER JOIN DW.dbo.tempo t on t.id = e.id_tempo
INNER JOIN DW.dbo.categoria c on c.id = e.id_categoria
where id_ator is not null and id_categoria is not null
order by a.nome, t.ano, t.mes, valor desc;




-----------------------------------------------------------------------------------------------

-- 04 - Análise dos empréstimos realizados por filme ao longo do tempo.
-- Por numero de aluguéis, agrupado por filme ao longo do tempo

CREATE OR ALTER PROCEDURE ETL_alugueisPorFilmeTempo 

AS BEGIN BEGIN TRANSACTION;

BEGIN TRY
INSERT INTO
    DW.dbo.emprestimo (quantidade_emprestimos, id_filme, id_tempo)
SELECT
    COUNT(r.rental_id) AS quantidade_emprestimos,
    i.film_id as id_filme,
    t.id
FROM
    DW.dbo.tempo t
    LEFT JOIN dbo.dbo.rental r ON MONTH (r.rental_date) = t.mes
    AND YEAR (r.rental_date) = t.ano
    LEFT JOIN dbo.dbo.inventory i ON i.inventory_id = r.inventory_id
	WHERE i.film_id is not null
GROUP BY
    i.film_id,
    t.id
	COMMIT TRANSACTION;
END TRY 
BEGIN CATCH 
	ROLLBACK TRANSACTION;
	THROW;
END CATCH 
END;

exec ETL_alugueisPorFilmeTempo;

select  f.nome, e.quantidade_emprestimos, t.mes, t.ano from DW.dbo.emprestimo e
INNER JOIN DW.dbo.filme f on e.id_filme = f.id
INNER JOIN DW.dbo.tempo t on e.id_tempo = t.id
where id_filme is not null and id_tempo is not null and id_categoria is null
order by f.nome, t.mes, t.ano;







-----------------------------------------------------------------------------------------------

-- 05 - Análise de filmes e suas quantidades por filme e categoria em cada local de armazenamento.

-- Filmes e suas quantidades por filme e categoria em cada local armazenamento
-- (pedir sobre mudar a estrutura pra salvar ou se é o total de alugueis para cada local)
CREATE OR ALTER PROCEDURE ETL_alugueisPorFilmeCategoriaLocal
AS 
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.emprestimo (quantidade_emprestimos, id_filme, id_categoria, id_localizacao)
        SELECT 
            COUNT(r.rental_id) AS quantidade_emprestimos,
            f.film_id AS id_filme,
            c.category_id as id_categoria,
            s.staff_id AS id_localizacao
        FROM dbo.dbo.rental r 
        LEFT JOIN dbo.dbo.inventory i ON i.inventory_id = r.inventory_id
        LEFT JOIN dbo.dbo.film f ON i.film_id = f.film_id
        LEFT JOIN dbo.dbo.film_category fc ON fc.film_id = f.film_id
        LEFT JOIN dbo.dbo.category c ON c.category_id = fc.category_id
        LEFT JOIN dbo.dbo.staff s ON r.staff_id = s.staff_id
        GROUP BY 
		f.film_id, 
		c.category_id, 
		s.staff_id
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

exec ETL_alugueisPorFilmeCategoriaLocal;

select quantidade_emprestimos, f.nome as filme, c.nome as categoria, l.cidade  from DW.dbo.emprestimo e
INNER JOIN DW.dbo.filme f on e.id_filme = f.id
INNER JOIN DW.dbo.categoria c on e.id_categoria = c.id
INNER JOIN DW.dbo.localizacao l on e.id_localizacao = l.id
where id_localizacao is not null and id_filme is not null
order by f.nome, l.cidade;

-----------------------------------------------------------------------------------------------


-- 06 - Análise de volume de empréstimos por clientes, permitindo uma visão por localidade do cliente e tempo
-- Por cliente localidade tempo

CREATE OR ALTER PROCEDURE ETL_alugueisPorClienteLocalTempo
AS BEGIN
BEGIN TRANSACTION;
BEGIN TRY
	INSERT INTO
    DW.dbo.emprestimo (quantidade_emprestimos, id_cliente, id_tempo,id_localizacao)
SELECT
	COUNT(r.rental_id) AS quantidade_emprestimos,
	c.customer_id as id_cliente,
	t.id as id_tempo,
	ci.city_id as id_localizacao
from
	DW.dbo.tempo t
    LEFT JOIN dbo.dbo.rental r ON MONTH (r.rental_date) = t.mes
    AND YEAR (r.rental_date) = t.ano
	INNER JOIN dbo.dbo.customer c on r.customer_id = c.customer_id
	INNER JOIN dbo.dbo.address a on c.address_id = a.address_id
	INNER JOIN dbo.dbo.city ci on ci.city_id = a.city_id
GROUP BY
    c.customer_id,
	ci.city_id,
    t.id
COMMIT TRANSACTION
END TRY
BEGIN CATCH
	ROLLBACK TRANSACTION;
	THROW;
END CATCH
END;


exec ETL_alugueisPorClienteLocalTempo;

select c.nome, t.ano,t.mes, quantidade_emprestimos, l.cidade from DW.dbo.emprestimo e
INNER JOIN DW.dbo.cliente c on c.id = e.id_cliente
INNER JOIN DW.dbo.tempo t on t.id = e.id_tempo
INNER JOIN DW.dbo.localizacao l on l.id = e.id_localizacao
where id_cliente is not null and id_tempo is not null and e.id_localizacao is not null
order by c.nome, t.ano, t.mes;

-----------------------------------------------------------------------------------------------

-- 07 - Análise comparativa da produtividade dos funcionários em relação aos empréstimos.
-- Total Por funcionario

CREATE OR ALTER PROCEDURE ETL_alugueisPorFuncionario 
AS
BEGIN
    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO DW.dbo.emprestimo (quantidade_emprestimos, id_funcionario, valor_total)
        SELECT
            COUNT(r.rental_id) AS quantidade_emprestimos,
            s.staff_id AS id_funcionario,
            SUM(p.amount) AS valor_total
        FROM dbo.dbo.rental r
        INNER JOIN dbo.dbo.staff s ON r.staff_id = s.staff_id
        LEFT JOIN dbo.dbo.payment p ON r.rental_id = p.rental_id
        GROUP BY s.staff_id;
        
        COMMIT TRANSACTION;
    END TRY 
    BEGIN CATCH 
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH 
END;

exec ETL_alugueisPorFuncionario;



select f.nome, quantidade_emprestimos, valor_total from DW.dbo.emprestimo e
INNER JOIN DW.dbo.funcionario f on f.id = e.id_funcionario
where id_funcionario is not null and id_tempo is null;





-----------------------------------------------------------------------------------------------

-- 08 - Análise da empréstimos realizados e que não foram pagos e devolvidos, considerando lucro versus prejuízo
-- (lucro vem dos empréstimos pagos e prejuízo daqueles que não foram pagos).
-- Quantidade de emprestimos pagos vs nao pagos(pedir sobre valor do nao pago)

CREATE OR ALTER PROCEDURE ETL_lucroPrejuizo 
AS BEGIN
BEGIN TRANSACTION;
BEGIN TRY
	INSERT INTO
    DW.dbo.emprestimo (quantidade_emprestimos, valor_total, qtd_emprestimos_nao_pagos, valor_total_nao_pago)
SELECT     
	COUNT(r.rental_id) AS quantidade_emprestimos,
	SUM(p.amount) AS valor_total,
	(SELECT COUNT(*) from dbo.dbo.rental r 
	LEFT JOIN dbo.dbo.payment p ON r.rental_id = p.rental_id WHERE p.payment_id is null )  as nao_pagos,
	(SELECT SUM(f.rental_rate) from dbo.dbo.rental r 
	LEFT JOIN dbo.dbo.payment p ON r.rental_id = p.rental_id
	LEFT JOIN dbo.dbo.inventory i ON i.inventory_id = r.inventory_id
	LEFT JOIN dbo.dbo.film f ON	f.film_id = i.film_id
	WHERE p.payment_id is null )  as valor_total_nao_pago
from
	dbo.dbo.rental r 
	LEFT JOIN dbo.dbo.payment p ON p.rental_id = r.rental_id;
COMMIT TRANSACTION
END TRY 
BEGIN CATCH 
	ROLLBACK TRANSACTION;
	THROW;
END CATCH 
END;

 
exec ETL_lucroPrejuizo;


select (quantidade_emprestimos - qtd_emprestimos_nao_pagos) as emprestimos_pagos,
(valor_total - valor_total_nao_pago) as valor_pago,
quantidade_emprestimos, valor_total, qtd_emprestimos_nao_pagos , valor_total_nao_pago
from DW.dbo.emprestimo where qtd_emprestimos_nao_pagos is not null;





