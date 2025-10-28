SELECT TOP(10) * FROM world_population;

SELECT 
	FORMAT(SUM("2022 Population"),'#,0', 'pt-BR')  populacao_total
FROM world_population;