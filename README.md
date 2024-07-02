# karhub_project

O desafio consiste em desenvolver um ETL para processar arquivos que representam o orçamento do Estado de São Paulo de 2022 e armazená-los em um formato consistente para responder perguntas que ajudarão nosso time.
Os valores foram dolarizados com a cotação máxima do dolár do dia 22/06/2022.

## Build

    conda create -n karhub python=3.11 -y
    conda activate karhub
    pip install -r requirements.txt

## Run

    Adicionar na pasta das DAGs do seu Airflow e seja feliz !

# Respondendo as perguntas do projeto

![Tabela Final](resultados/fonte_table_final.png)



# Quais são as 5 fontes de recursos que mais arrecadaram?
![TOP 5 Que Mais Arrecadaram](resultados/top5_que_mais_arrecadacao.png)

# Quais são as 5 fontes de recursos que mais gastaram?
![TOP 5 Que Mais Gastaram](resultados/top5_que_mais_gastos.png)

# Quais são as 5 fontes de recursos com a melhor margem bruta?
![TOP 5 Com Melhor Margem Bruta](resultados/top5_que_melhor_margem_brutas.png)

# Quais são as 5 fontes de recursos que menos arrecadaram?
![TOP 5 Que Menos Arrecada](resultados/top5_menor_arrecadacao.png)

# Quais são as 5 fontes de recursos que menos gastaram?
![TOP 5 Que Menos Gasta](resultados/top5_menor_gastos.png)

# Quais são as 5 fontes de recursos com a pior margem bruta?
![TOP 5 Com Pior Margem Bruta](resultados/top5_pior_margem_bruta.png)

# Qual a média de arrecadação por fonte de recurso?
![Média de Arrecadação](resultados/media_arrecadacao.png)

# Qual a média de gastos por fonte de recurso?
![Média de Gastos](resultados/media_arrecadacao.png)
