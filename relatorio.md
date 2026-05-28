# RELATÓRIO FINAL DE PROJETO APLICADO

## CONCEÇÃO, IMPLEMENTAÇÃO E OPERACIONALIZAÇÃO DE UM SISTEMA DE SUPORTE À DECISÃO PARA ANÁLISE DE TENDÊNCIAS DE AQUISIÇÃO DE PRODUTOS EM RETALHO AUTOMÓVEL ESPECIALIZADO

---

**Unidade Curricular:** Ciências de Dados em Grande Escala (CDGE)  
**Instituição:** Escola de Engenharia, Universidade do Minho  
**Ano Letivo:** 2025/2026  
**Docente:** Prof. Orlando Belo

**Equipa de Trabalho:**

- André Pinto (A106825) — Especialista em Business Intelligence (Dashboard de Apoio à Aquisição de Stock)
- Guilherme Simão (A106835) — Especialista em Business Intelligence (Dashboard Estratégico de Direção Executiva)
- José Silva (A106831) — Especialista em Data Science (Modelação Preditiva e NLP)
- Pedro Oliveira (A106830) — Especialista em Business Intelligence (Dashboard de Rotação e Saúde de Inventário)

---

### Resumo

O presente relatório descreve de forma exaustiva o processo de conceção, desenvolvimento e operacionalização do sistema de suporte à decisão de nível universitário e empresarial para a **Auto Escala**, uma empresa nacional de referência no retalho especializado de viaturas usadas e seminovos. Confrontada com o desafio crítico de otimização de capital na aquisição preditiva de inventário — consubstanciado no problema de determinar de forma antecipada que viaturas adquirir hoje para maximizar a rotação de stock e a margem de lucro futura —, a empresa implementou uma arquitetura moderna de _Data Lakehouse_ estruturada segundo os princípios da _Medallion Architecture_ (camadas Bronze, Silver e Gold/Star Schema).

O pipeline de engenharia de dados foi concebido em linguagem Python, tirando partido de bibliotecas de alto desempenho como `pandas` e `delta-rs`, o que permitiu usufruir de todas as garantias transacionais e capacidades de versionamento (_ACID e Time Travel_) das tabelas Delta sobre sistemas de ficheiros locais, eliminando a complexidade operacional e os elevados custos de computação distribuída associados a infraestruturas baseadas em Apache Spark. A staging area foi desenhada de forma a processar fontes de dados heterogéneas, englobando dados de inventário físico dos stands (estruturados em CSV), dados geográficos de interesse público digital obtidos do Google Trends (semiestruturados em JSON), feeds semanais de volume de hashtags nas principais redes sociais (semiestruturados em XML) e discussões orgânicas e informais de comunidades da especialidade (não estruturados em ficheiros de texto corrido TXT provenientes do Fórum Motorguia). Foi acoplada ao Silver pipeline uma componente avançada de Processamento de Linguagem Natural (NLP) através de modelos BERT de classificação conexionista ajustados para português (`pysentimiento`) para extrair em lote o score contínuo de sentimento associado a marcas e modelos.

Na camada Gold, os dados refinados são materializados num Data Warehouse central em PostgreSQL modelado de acordo com a metodologia multidimensional de Ralph Kimball, dotado de controlo temporal de _Slowly Changing Dimensions (SCD) Tipo 2_ na dimensão de clientes e logs de auditoria automatizados através de triggers de base de dados (_Trigger-Based CDC_). Para garantir a exploração segura e isolada de dados por parte dos analistas, foi estruturado um ambiente dedicado de _Analytical Sandbox_. A camada preditiva assenta em dois motores de Machine Learning orquestrados de forma autónoma pelo Apache Airflow: um modelo **SARIMA** para prever as tendências de procura de mercado e um modelo **XGBoost Regressor** para estimar de forma supervisionada o ganho de aquisição esperado de cada viatura colocada em mercado de leilões. A exploração interativa é disponibilizada através de três dashboards analíticos coerentes, desenhados sob a identidade cromática premium da organização (_paleta Deep Slate & Cobalt_), provendo suporte preditivo, robusto e transparente a compradores operacionais, gestores de stock e à direção executiva do grupo Auto Escala.

**Palavras-chave:** Data Lakehouse, Metodologia de Ralph Kimball, Ingestão Incremental, Séries Temporais SARIMA, XGBoost Regressor, Apache Airflow.

---

## 1. Definição do Sistema

### 1.1 Contexto de Aplicação

O mercado de retalho especializado em viaturas usadas e seminovos em Portugal caracteriza-se por uma concorrência intensa, forte assimetria de informação e elevada sensibilidade a fatores macroeconómicos e preferências flutuantes dos consumidores. O grupo **Auto Escala** é uma organização comercial consolidada que opera neste segmento em todo o território nacional. Com o objetivo de fundamentar cientificamente a sua transição para decisões orientadas por dados (_data-driven decisions_), a empresa desenhou e implementou uma infraestrutura analítica piloto em três stands físicos altamente representativos do mercado nacional:

1.  **Stand Braga**: Posicionado no norte do país, uma região com elevado dinamismo industrial, forte propensão para a aquisição de viaturas a gasóleo e comerciais ligeiros, e forte ligação a redes sociais locais.
2.  **Stand Porto**: Inserido numa das maiores áreas metropolitanas do país, caracterizado por um mercado de consumo diversificado com procura crescente por viaturas citadinas, híbridas e de gama média-alta.
3.  **Stand Lisboa**: O maior stand da empresa em termos de faturação bruta e tráfego físico, operando num mercado altamente cosmopolita com forte adoção de viaturas elétricas, SUVs e modelos premium, além de uma elevada densidade populacional de clientes jovens.

Apesar de o projeto-piloto e a validação do modelo analítico estarem confinados a estes três stands piloto, a arquitetura e a engenharia de dados do sistema foram projetadas de raiz para responder aos requisitos de **Ciências de Dados em Grande Escala**. Toda a infraestrutura técnica (ingestão, normalização, enriquecimento, base de dados e orquestração) foi estruturada de forma modular para permitir a expansão linear e transparente para uma rede corporativa com mais de 100 stands físicos distribuídos por todo o país, sem necessidade de reengenharia ou alterações estruturais no código-fonte das transformações.

No modelo de retalho automóvel da Auto Escala, a aquisição de inventário físico constitui a atividade com maior impacto no balanço financeiro e na rentabilidade operacional. Cada veículo usado adquirido em leilões, concessionários parceiros ou retomas de particulares representa uma imobilização imediata de capital de giro de grande volume. Uma viatura parada no parque físico de um stand é um ativo que desvaloriza diariamente (custos de depreciação), ocupa espaço logístico limitado (custos de ocupação) e impede a libertação de liquidez financeira para novas oportunidades comerciais de alta rotação (custos de oportunidade). Assim, a capacidade de alinhar de forma preditiva o stock disponível com a real procura latente do mercado regional é o fator crítico de sucesso que define a margem de lucro líquida do grupo comercial.

### 1.2 Motivação e Objetivos do Trabalho

Historicamente, o processo de tomada de decisão na aquisição de inventário na Auto Escala baseava-se essencialmente na intuição, experiência pessoal e empirismo dos gestores de compras locais de cada stand físico. Embora este paradigma tradicional tenha sustentado o crescimento inicial da empresa, a escala atual do negócio revelou limitações operacionais graves associadas a esta abordagem fragmentada:

- _Desequilíbrio de Stock_: Acumulação excessiva de viaturas de marcas específicas e baixa procura num determinado stand regional, enquanto outros stands da rede sofriam de ruturas sistemáticas de stock para os mesmos modelos, resultando em vendas perdidas para a concorrência digital.
- _Depreciação Forçada_: Aquisição de viaturas obsoletas ou fora de tendência de mercado a preços desajustados, forçando a aplicação de descontos agressivos na margem de venda no final do ano fiscal para permitir o escoamento de inventário parado há mais de 60 dias.
- _Perda de Oportunidades_: Falha em detetar o crescimento rápido de interesse público por novos conceitos de viaturas (como híbridos plug-in ou sub-categorias de SUVs) antes que este interesse se traduzisse em procura física nos concessionários, permitindo que concorrentes digitais de maior agilidade se antecipassem na aquisição em lote no mercado grossista.

**Objetivos Centrais do Projeto:**
Para mitigar estas ineficiências e alavancar o desempenho comercial do grupo, definiram-se os seguintes objetivos estratégicos para a conceção da plataforma analítica de suporte à decisão:

1.  **Desenvolver um Pipeline de Ingestão de Alta Robustez**: Conceber uma infraestrutura unificada capaz de recolher dados estruturados de vendas internas dos stands locais e dados analíticos demográficos, cruzando-os em tempo útil com fontes de dados externas heterogéneas (Google Trends, hashtags semanais e discussões textuais orgânicas de fóruns especializados).
2.  **Integrar Inteligência Artificial NLP**: Desenvolver um processador linguístico automatizado em lote para classificar a perceção orgânica do público português em relação a marcas e modelos automóveis nas comunidades digitais, convertendo sentimentos textuais informais em indicadores numéricos objetivos de mercado.
3.  **Implementar Motores de Previsão Preditiva**: Projetar e treinar um modelo de previsão temporal de tendências para estimar o interesse futuro do público português (M1 - SARIMA) e um modelo de regressão supervisionado para estimar o ganho financeiro esperado de cada viatura colocada em leilão antes da submissão de propostas de compra (M3 - XGBoost).
4.  **Disponibilizar Dashboards Analíticos Especializados**: Criar painéis interativos integrados com formatação condicional e segmentadores de dados dedicados a responder diretamente aos requisitos de três perfis cruciais do negócio: Compras operacionais, Gestão de stocks em parque e Direção Geral Executiva.

### 1.3 Análise da Viabilidade do Processo

A viabilidade de implementação e operacionalização do sistema de suporte à decisão da Auto Escala foi profundamente analisada sob três vertentes operacionais:

- **Viabilidade Técnica**: O desenho de engenharia de dados do sistema afasta-se deliberadamente do uso do ecossistema distribuído Apache Spark. Reconhecendo que o volume transacional atual da rede de stands piloto permite o processamento em memória centralizada, a adoção do Spark introduziria um overhead computacional injustificável (tempo de arranque de JVMs, latência na inicialização do scheduler distribuído e necessidade de infraestruturas cloud dispendiosas em clusters). Em substituição, optou-se pela utilização combinada de Python (pandas) com a biblioteca nativa em Rust `delta-rs`. Esta abordagem permite ler e escrever ficheiros Parquet em formato transacional Delta Lake na staging area, garantindo total conformidade transacional ACID, versionamento de dados (_Time Travel_ para auditar estados passados) e evolução de esquemas rápida e em memória única, com latências de processamento próximas de zero e consumo mínimo de recursos de CPU. O carregamento final para o PostgreSQL garante que as junções multidimensionais complexas requeridas pelo Power BI são resolvidas com alta velocidade através de indexação relacional B-Tree robusta.
- **Viabilidade Operacional**: A sustentabilidade e autonomia do sistema são garantidas pela automatização total das cargas. O Apache Airflow gere a execução em lote das tarefas semanalmente e mensalmente através de agendamentos cronológicos bem definidos. O pipeline Silver e as cargas Gold incluem rotinas de limpeza automática e validação ortográfica robusta consultando a tabela de normalização de dados master (`dim_dicionario_veiculo`), o que reduz drasticamente a necessidade de intervenção humana na correção manual de inconsistências cadastrais nos distritos ou stands. Adicionalmente, a barreira de qualidade criada pela camada de Quarentena isola registos nulos de forma assíncrona, assegurando que o pipeline analítico corre permanentemente até ao fim, mesmo perante falhas graves nos dados de entrada das fontes externas.
- **Viabilidade Financeira**: O retorno do investimento (ROI) deste projeto é comprovado diretamente pela otimização do custo de imobilização de ativos. Considerando que o grupo Auto Escala detém uma média constante de 150 viaturas usadas em parque nos stands piloto, com um preço de aquisição médio unitário de 18.000 € e um tempo médio de permanência em parque histórico de 45 dias, a empresa mantém um capital de giro permanentemente paralisado em stock de **2.700.000 €**. Ao reduzir analiticamente o tempo médio de stock em parque para 30 dias (através de aquisições cirúrgicas de modelos com elevado sentimento positivo e forte previsão de procura), a empresa reduz o capital imobilizado médio para **1.800.000 €**, libertando **900.000 €** em liquidez de tesouraria imediata apenas no primeiro ano de operação do piloto. A isto soma-se a mitigação de perdas de depreciação forçada e a poupança em custos de infraestrutura cloud conseguida pelo uso de pandas e delta-rs em detrimento de clusters Spark.

### 1.4 Recursos e Equipa de Trabalho

O desenvolvimento da plataforma analítica foi executado de forma colaborativa por uma equipa de quatro especialistas especializados por sub-áreas técnicas do projeto, cada um responsável pelo cumprimento de requisitos específicos e tarefas críticas:

1.  **André Pinto (A106825) — Especialista em Business Intelligence (Dashboard de Apoio à Aquisição de Stock)**: Focado no desenvolvimento da interface operacional de compras. Modelou os cruzamentos analíticos entre as tendências do Google Trends e o stock de viaturas em parque. Desenhou as métricas DAX de "Oportunidades de Compra" e estruturou as segmentações por categoria para os compradores de campo, garantindo que o Power BI recebe dados limpos da Gold view `vw_mart_compras`.
2.  **Guilherme Simão (A106835) — Especialista em Business Intelligence (Dashboard Estratégico de Direção Executiva)**: Focado na visão financeira e geo-demográfica macro da empresa. Mapeou os KPIs globais de faturação, volume acumulado de vendas e margens brutas percentuais. Desenhou o mapa de performance regional interativo que cruza dados geográficos de residência de clientes com a demografia regional, ligando o Power BI à Gold view `vw_mart_direcao`.
3.  **José Silva (A106831) — Especialista em Data Science (Modelação Preditiva e NLP)**: Responsável por toda a infraestrutura matemática e lógica preditiva da plataforma. Desenhou o pipeline de treino em janela expansiva (_walk-forward validation_) para prever o interesse de mercado N+1 usando o algoritmo temporal **SARIMA** (M1) e para prever o ganho esperado das viaturas em leilão usando o algoritmo **XGBoost Regressor** (M3). Implementou também a classificação em lote de NLP (comunidades de fóruns em TXT) utilizando modelos RoBERTa ajustados para português com a biblioteca `pysentimiento`.
4.  **Pedro Oliveira (A106830) — Especialista em Business Intelligence (Dashboard de Rotação e Saúde de Inventário)**: Focado na monitorização operacional de parque e ativos do grupo. Criou o sistema de cartões de alerta com formatação condicional para veículos em stock parado há mais de 30 dias, calculou a velocidade de rotação média das viaturas nos stands piloto e desenhou a correlação entre sentimento social negativo e custos de capital parado, ligando o Power BI à Gold view `vw_mart_stock`.

Os recursos computacionais e materiais alocados ao projeto consistem em:

- _Hardware_: Servidor físico local equipado com processador Intel Core i7 de 12.ª geração, 32 GB de RAM e armazenamento em estado sólido (SSD NVMe) de 1 TB para processamento de Delta Lake em alta velocidade.
- _Ambiente e SO_: Sistema operativo Windows 11 Pro com distribuição Anaconda 2024.03 instalando Python 3.10 virtualizado em ambiente restrito de bibliotecas (`conda activate cdge`).
- _Bases de Dados_: Instância local PostgreSQL 15 rodando sob a porta standard 5432 com credenciais restritas e encriptação TLS ativa.
- _Software Aplicacional_: Power BI Desktop para a prototipagem e conceção visual final dos relatórios analíticos, interligado via DirectQuery ao PostgreSQL.
- _Bibliotecas de Código_: `pandas` (manipulação de DataFrames), `delta-rs` (API Rust nativa de leitura/escrita de Delta Lake), `pysentimiento` (NLP RoBERTa), `statsmodels` (SARIMA), `xgboost` (Gradiente Boosting Regressor), `sqlalchemy` (driver SQL ORM para PostgreSQL), `ydata-profiling` (geração de HTML reports de profiling de dados).

### 1.5 Plano de Execução do Projeto

O cronograma do projeto seguiu rigorosamente o faseamento cronológico definido pelo guião da unidade curricular do Prof. Orlando Belo. A calendarização exata e as respetivas tarefas e atores associados são detalhadas abaixo:

```
[INSERIR FIGURA 1: Diagrama GANTT Detalhado do Faseamento do Projeto Auto Escala]
- Descrição: Diagrama GANTT completo de planeamento e execução do projeto de 12 de Fevereiro a 28 de Maio de 2026.
- Atores Associados: José Silva (DS), André Pinto (BI), Pedro Oliveira (BI), Guilherme Simão (BI).
- Atividades Ilustradas: Modelação do Problema (F1), Entrevistas de Requisitos (F2), Engenharia de Dados Bronze/Silver e Dicionários PG (F3), Conceção dos Painéis em Power BI (F4), Treino e Validação do SARIMA/XGBoost (F5) e Elaboração do Relatório de Conclusões (F6).
```

- **Fase 1: Definição do Sistema (12FEV2026 - 05MAR2026)**: Estudo inicial de viabilidade comercial. Delimitação do âmbito técnico do projeto-piloto nos stands de Lisboa, Porto e Braga, garantindo a sua compatibilidade de escala nacional para 100 stands. Atores: Envolvimento de todos os elementos na conceção e enquadramento inicial.
- **Fase 2: Levantamento e Análise de Requisitos (05MAR2026 - 26MAR2026)**: Condução de reuniões de JAD (_Joint Application Design_) para mapear os fluxos de trabalho e desenhar mockups e especificações de dashboarding. Levantamento das fontes de dados manuais e especificação dos critérios de qualidade. Atores: Liderança dos três analistas de BI (André, Pedro e Guilherme), alinhados com o cientista de dados (José).
- **Fase 3: Implementação do Sistema de Análise (26MAR2026 - 23ABR2026)**: Fase crítica de maior densidade técnica. Configuração física das pastas de Delta Lake. Codificação das transformações Silver e desenvolvimento do carregador incremental com lógica de merges e upserts relacionais no PostgreSQL, incluindo os triggers de CDC e a Sandbox. Atores: Execução técnica liderada por José Silva (DS/Engenharia de dados) e integração relacional pelos analistas de BI.
- **Fase 4: Exploração e Análise de Dados (23ABR2026 - 30ABR2026)**: Ligação e mapeamento de dados entre o Power BI e o PostgreSQL. Codificação das métricas complexas em linguagem DAX e desenvolvimento dos três dashboards analíticos usando a identidade cromática institucional. Atores: Execução assíncrona por André Pinto, Pedro Oliveira e Guilherme Simão.
- **Fase 5: O Sistema de Análise de Tendências (30ABR2026 - 21MAI2026)**: Extração do histórico consolidated no DW para treino dos modelos ML. Desenvolvimento das rotinas de séries temporais SARIMA e regressão XGBoost no ambiente conda virtual. Validação e cálculo das métricas de erro (MAE/MAPE) em backtesting de janela expansiva. Gravação dos outputs na fact table preditiva. Atores: Execução por José Silva.
- **Fase 6: Conclusões e Trabalho Futuro (21MAI2026 - 28MAI2026)**: Revisão dos KPIs comerciais e de performance obtidos nas simulações. Redação final e consolidação do presente relatório em formato académico, revisão das referências bibliográficas APA e preparação da apresentação técnica para o docente. Atores: Consolidação por toda a equipa de trabalho.

---

## 2. Levantamento e Análise de Requisitos

### 2.1 Método Adotado

Para assegurar a fidelidade técnica do sistema analítico e garantir que a infraestrutura desenvolvida responde a problemas reais com forte impacto nos KPIs da Auto Escala, a equipa implementou uma metodologia robusta e estruturada em três fases sucessivas de engenharia de requisitos:

1.  **Entrevistas Semi-Estruturadas por Perfil**: Foram agendadas e conduzidas sessões individuais com os responsáveis operacionais da organização de forma a emular reuniões de alinhamento corporativo. O Diretor de Compras foi questionado sobre como avaliava a atratividade de viaturas no mercado secundário; o Diretor de Stock detalhou os principais custos de manutenção de veículos em parque e o limiar máximo seguro de retenção antes da depreciação acelerada; e o CEO do grupo detalhou os indicadores financeiros consolidados que necessitava de reportar mensalmente à administração.
2.  **Mapeamento de Fontes de Dados e Profiling Exploratório**: Realizou-se uma inspeção detalhada na Landing Zone para identificar o formato físico, a frequência de escrita e os principais desafios de qualidade contidos nos ficheiros de origem. Detetou-se uma grande quantidade de ruído lexical introduzido manualmente nos ficheiros de inventário (erros na grafia de modelos, nomes de stands mal formatados, variações de maiúsculas e minúsculas) e na demografia (inconsistência de distritos com/sem acentuação gráfica). Este levantamento justificou a especificação de um requisito não funcional rigoroso de normalização dimensional automática na Staging Area.
3.  **Desenho e Validação de Mockups Interativos**: Com base nos perfis mapeados, os especialistas de BI criaram maquetes visuais de baixa fidelidade no Power BI para simular o comportamento final das interfaces analíticas. Estas maquetes foram submetidas à validação dos decisores de negócio para refinar os filtros críticos requeridos (filtros rápidos por distrito, stand, categoria de veículo, tipo de combustível e ano de fabrico) e ajustar a disposição visual e legibilidade dos painéis interativos antes do desenvolvimento de consultas SQL complexas.

### 2.2 Organização dos Requisitos Levantados

Os requisitos de engenharia recolhidos foram organizados em requisitos funcionais (RF) focados nos quatro intervenientes analíticos do sistema e requisitos não funcionais (RNF) focados em aspetos de arquitetura, segurança e integridade de dados:

#### Requisitos Funcionais (RF)

- **RF1 (Gestor de Compras)**: O sistema deve exibir visualmente o ranking MOM (_Month-over-Month_) de marcas e modelos de veículos com maior ritmo de crescimento de interesse nas pesquisas dos consumidores em Portugal.
- **RF2 (Gestor de Compras)**: O dashboard deve correlacionar modelos com elevado interesse digital externo e baixo volume físico em stock, sinalizando-os dinamicamente como "Oportunidade de Aquisição".
- **RF3 (Gestor de Compras)**: A interface deve exibir a previsão do volume de procura futuro gerada pela equipa de Data Science para o mês Target (N+1), comparando-a diretamente com a série histórica real.
- **RF4 (Gestor de Stock)**: O sistema deve monitorizar o tempo de parque individual de cada veículo e emitir alertas visuais interativos e em tempo oportuno para viaturas em stock parado há mais de 30 dias.
- **RF5 (Gestor de Stock)**: O painel deve disponibilizar a métrica de taxa de rotação média de inventário por stand (Braga, Porto, Lisboa) para orientar o rebalanceamento de ativos.
- **RF6 (Gestor de Stock)**: O dashboard de inventário deve integrar os dados de sentimento de fóruns e redes sociais, permitindo identificar veículos em parque cujas marcas estejam sob o efeito de opiniões públicas maioritariamente negativas.
- **RF7 (Direção Executiva)**: O dashboard de controlo estratégico deve consolidar em tempo real os KPIs macro financeiros do grupo: Faturação Bruta Total Realizada, Margem Absoluta Média e Margem Percentual Média.
- **RF8 (Direção Executiva)**: O painel executivo deve mapear geograficamente as vendas a nível nacional ao nível do distrito, permitindo cruzamentos com dados demográficos distritais.
- **RF9 (Direção Executiva)**: O sistema deve exibir a correlação entre a "Share de Voz" digital das marcas nas redes sociais e a quota de vendas real das mesmas na empresa para desenhar campanhas de captação comercial.
- **RF10 (Data Science)**: O pipeline preditivo deve treinar e projetar previsões mensais de forma automática após a ingestão correta do lote de dados transacionais, utilizando o histórico acumulado de forma incremental.

#### Requisitos Não Funcionais (RNF)

- **RNF1 (Segurança e Isolamento)**: Os analistas de BI devem possuir uma área isolada do tipo Sandbox Analítica na base de dados para realizar consultas exploratórias ad-hoc e prototipar dashboards sem degradar ou colocar em risco as tabelas operacionais do Data Warehouse principal.
- **RNF2 (Auditoria Dimensional)**: Quaisquer alterações ou atualizações aos atributos cadastrais das dimensões na Gold layer (como mudanças no distrito de residência de clientes na dimensão com SCD Tipo 2 ou atualizações na dimensão de stands) devem ser registadas ao nível mais baixo da base de dados com as respetivas marcas temporais, operações efetuadas e estados anterior e posterior em formato estruturado (JSON).
- **RNF3 (Qualidade e Quarentena)**: Registos inconsistentes nas fontes (preços de aquisição negativos, anos de viatura no futuro ou NIFs incompletos) devem ser isolados assincronamente numa área de quarentena do Data Lake sem interromper a execução do fluxo global da pipeline.
- **RNF4 (Escalabilidade)**: O pipeline de ingestão e as transformações em Python devem conseguir suportar o crescimento da rede para mais de 100 stands de forma transparente, tirando partido de indexação e processamento eficiente em memória única.
- **RNF5 (Compatibilidade)**: O Data Warehouse final deve ser construído em tecnologia padrão SQL compatível com ligação nativa DirectQuery e Import do Microsoft Power BI.

### 2.3 Análise e Validação Geral dos Requisitos

Na consolidação técnica dos requisitos, a equipa identificou potenciais conflitos de design analítico que exigiram decisões estruturadas de engenharia de dados:

1.  **Divergência de Granularidade Temporal (Semanas vs. Meses)**: Os feeds XML de hashtags sociais medem menções numa escala semanal baseada na semana ISO, ao passo que as tendências de mercado do Google Trends e as vendas operam numa granularidade essencialmente mensal e diária respetivamente. Para resolver esta incompatibilidade estrutural sem introduzir registos nulos ou falsas correlações, definiu-se a regra de **Ancoragem Temporal Unificada**. A pipeline Silver e o script carregador do PostgreSQL efetuam o cast e a agregação de todos os dados de volume temporais externos para o **primeiro dia do mês** correspondente. Isto garante que qualquer junção analítica e agregação na dimensão `dim_tempo` ao nível de Ano e Mês resulta em correlações matematicamente coerentes e sem perda de linhagem de dados.
2.  **Lexical Noise nas Entradas Manuais**: A inserção manual de registos nos stands operacionais introduzia variações gráficas graves para as mesmas entidades (e.g. "VW Golf", "Volkswagem Golf", "v.w. golf" para designar a mesma viatura). A equipa validou a necessidade de implementar uma base relacional de MDM (_Master Data Management_) hospedada no PostgreSQL centralizada na tabela `dim_dicionario_veiculo`. Esta tabela traduz dinamicamente qualquer variação de string ruidosa proveniente do Bronze para o seu equivalente limpo e normalizado na camada Silver, segregando para a Quarentena apenas os registos cujas strings sejam inteiramente desconhecidas no dicionário corporativo.
3.  **Gestão do Histórico Demográfico dos Clientes**: O CEO necessitava de análises fiáveis de vendas geográficas. Se um cliente que reside no distrito de Braga comprar uma viatura em 2023, mudar de residência para o distrito de Lisboa em 2024 e comprar outra viatura, o DW deve conseguir mapear a primeira venda para Braga e a segunda para Lisboa para preservar o rigor da estatística de penetração comercial demográfica. Esta validação confirmou a necessidade de configurar a dimensão de clientes como uma **Slowly Changing Dimension (SCD) Tipo 2**, onde o NIF do cliente mantém várias linhas ativas em janelas temporais distintas do DW através de chaves substitutas (_Surrogate Keys_).

---

## 3. Implementação do Sistema de Análise

### 3.1 Apresentação Geral

O sistema de suporte à decisão da Auto Escala materializa uma arquitetura moderna de **Data Lakehouse** baseada na metodologia de **Medallion Architecture**, dividida em três camadas de maturidade crescente de dados (Bronze, Silver e Gold), apoiadas por uma base de dados centralizada PostgreSQL e orquestradas autonomamente por Apache Airflow.

```
[INSERIR FIGURA 2: Diagrama de Arquitetura de Fluxo de Dados Medallion do Projeto Auto Escala]
- Descrição: Diagrama de blocos exibindo o fluxo de ponta a ponta. Mostra a Landing Zone (data/sources/), a camada Bronze em Delta Lake (ficheiros Parquet append-only), a camada Silver (Delta Lake enriquecido com processamento NLP e isolamento de Quarentena), a Gold Layer (Star Schema em PostgreSQL), e a replicação atómica para a Sandbox do analista (auto_escala_sandbox) para exploração em Power BI.
```

Abaixo fundamentam-se as opções de desenho tecnológico adotadas:

- _Uso de Python (Pandas e delta-rs)_: A decisão de afastar clusters pesados de computação distribuída como o Apache Spark baseou-se na otimização de custos e latência. O volume transacional do piloto da Auto Escala permite um processamento em memória centralizada altamente veloz. O uso combinado de pandas com a biblioteca Rust nativa `delta-rs` permite à empresa usufruir de todas as garantias transacionais e robustez das tabelas Delta (propriedades ACID, time-travel para auditar estados de dados passados, merges rápidos e evolução de esquemas) diretamente sobre o sistema de ficheiros nativo, com tempos de execução perto de zero e consumos insignificantes de CPU/RAM.
- _Data Warehouse PostgreSQL (Gold Layer)_: Em vez de manter toda a estrutura analítica final de consumo no lago de dados, optou-se pela materialização híbrida num SGBD relacional PostgreSQL (`auto_escala_dw`). Isto garante a máxima performance nas junções em estrela requeridas pelas ferramentas de visualização e compatibilidade absoluta DirectQuery com o Power BI, sem degradação de concorrência.
- _Sandbox Analítica Isolada_: Para responder às notas teóricas de Ciências de Dados em Grande Escala sobre o isolamento e self-service de exploração de dados, implementou-se uma Sandbox analítica isolada na base de dados PostgreSQL (`auto_escala_sandbox`). No encerramento com sucesso do pipeline, o script unificado executa uma rotina automática e atómica de cópia das tabelas dimensionais e factos do Star Schema para esta Sandbox. Os analistas André, Pedro e Guilherme podem realizar testes estatísticos complexos e maquetes de dashboards de forma livre e segura, sem qualquer risco de perturbar ou bloquear as consultas produtivas da empresa, mantendo uma governação estrita dos dados sensíveis do DW corporativo.

### 3.2 Fontes de Dados

O ecossistema analítico é alimentado de forma contínua por seis fontes de dados, cobrindo todas as categorias de estrutura de informação comercial:

#### A. Dados Transacionais Físicos (Estruturados)

1.  **CSV de Inventário dos Stands (Landing Zone: `data/sources/stands/`)**: Ficheiros de texto delimitados gerados mensalmente por cada stand da rede física (ex: `2024_03_stand.csv`). Contém o histórico completo de veículos que deram entrada em parque e viaturas vendidas no período. Estrutura de colunas: `id_viatura`, `matricula`, `marca`, `modelo`, `tipo_automovel`, `num_lugares`, `ano_viatura`, `combustivel`, `quilometragem`, `preco_aquisicao`, `data_entrada_stock`, `preco_venda`, `data_venda`, `nif_cliente`, `stand`.
2.  **CSV de Clientes (Landing Zone: `data/sources/clientes/`)**: Registo cadastral demográfico básico de clientes individuais que efetuaram compras de veículos (ex: `clientes.csv`). Contém: `nif`, `nome`, `idade`, `genero`, `distrito`, `ano_mes`.
3.  **CSV de Demografia Regional (Landing Zone: `data/sources/demografia/`)**: Dados demográficos geográficos oficiais dos distritos de Portugal obtidos do recenseamento populacional para contexto comercial (ex: `demografia.csv`). Contém: `distrito`, `ano_referencia`, `populacao_total`, `mean_age`, `pct_masculino`, `pct_feminino`.

#### B. Sinais de Mercado e Tendências Digitais (Semiestruturados)

4.  **JSON de Google Trends (Landing Zone: `data/sources/trends/`)**: Ficheiros semiestruturados que medem o volume de pesquisas ativas dos consumidores portugueses no Google para termos e modelos automóveis (ex: `google_trends_202403.json`). Contém pares chave-valor: `termo`, `regiao`, `mes`, `valor_interesse`.
5.  **XML de Hashtags de Redes Sociais (Landing Zone: `data/sources/hashtags/`)**: Feeds semanais de monitorização de dinâmica de hashtags de publicações de imagem e vídeo nas redes sociais (Instagram, YouTube, X) (ex: `hashtags_2024_W12.xml`). Estrutura em XML: tag raiz `<social_feed>` contendo elementos filhos `<hashtag>`, `<data>`, `<categoria>`, `<posts_instagram>`, `<posts_twitter>`, `<posts_youtube>` e `<total_posts>`.

#### C. Dados de Opinião Orgânica (Não Estruturados)

6.  **TXT de Fórum de Discussão Especializado (Landing Zone: `data/sources/forum/`)**: Ficheiros de texto corrido contendo o extrato em lote de discussões públicas informais de utilizadores no Fórum Motorguia (ex: `forum_202403.txt`). Fornece dados orgânicos detalhados sobre preferência de consumo, fiabilidade de motores e desvalorização.

### 3.3 Área de Preparação de dados

A área de preparação de dados (Staging Area) é materializada sobre as camadas intermédias do Data Lakehouse em Delta Lake local, estruturando e qualificando a informação de forma robusta e transparente:

#### Camada Bronze (Ingestão Transacional)

A camada Bronze (`data_lake/bronze`) é o ponto de entrada de dados brutos na infraestrutura Delta Lake. Os ficheiros são lidos da Landing Zone e escritos sem qualquer transformação estrutural ou alteração de tipo de dados. O pipeline garante a integridade e auditoria de linhagem de dados (_Data Lineage_) injetando de forma sistemática duas colunas de metadados transacionais no momento da gravação:

- `ingestion_timestamp`: Marca temporal precisa em UTC com milissegundos que indica o momento de entrada no lago de dados.
- `source_file`: O nome do ficheiro físico de origem de onde os dados foram extraídos (ex: `2024_03_stand_lisboa.csv`).
  Ao operar em modo transacional **Append-Only**, a camada Bronze preserva o histórico exato do estado bruto original de todas as fontes ao longo dos anos.

#### Camada Silver (Enriquecida e Normalizada)

A camada Silver (`data_lake/silver`) lê as tabelas Delta da camada Bronze e executa o processamento pesado de higienização, tipagem rigorosa, classificação e enriquecimento:

1.  **Deduplicação por Business Keys**: O pipeline ordena os registos por `ingestion_timestamp` e `source_file` de forma ascendente e aplica rotinas de deduplicação baseadas na chave física do negócio (`id_viatura`), garantindo que apenas a versão mais recente e verídica conhecida de cada veículo é retida no lote.
2.  **Enriquecimento via NLP RoBERTa**: A fonte de texto não estruturado dos fóruns (TXT) é submetida ao classificador linguístico RoBERTa ajustado para português (`pysentimiento`). O modelo processa as sentenças em lote e gera um indicador numérico contínuo de sentimento (score contínuo de sentimento variando entre -1.0 para forte sentimento negativo, 0.0 para neutro e +1.0 para forte sentimento positivo) associado às menções de marcas e modelos detetadas nas frases, convertendo opiniões textuais orgânicas em métricas quantificáveis de base de dados.
3.  **Lookup de Normalização Cadastral**: O pipeline realiza junções rápidas em memória com a tabela dicionário de Master Data Management (MDM) hospedada no PostgreSQL (`dim_dicionario_veiculo`), preenchendo as colunas `marca_normalizada` e `modelo_normalizado` e eliminando variações lexicais ruidosas introduzidas manualmente.
4.  **Camada de Quarentena Isolada**: Para garantir conformidade e qualidade absoluta de dados a jusante, a pipeline Silver implementa um gateway de validação lógica de regras operacionais de dados. Qualquer registo que falhe critérios estruturais fundamentais — tais como:
    - Presença de chaves físicas nulas ou inválidas (matrículas vazias ou NIFs incompletos);
    - Preços de aquisição negativos ou nulos;
    - Datas absurdas ou anos de fabrico no futuro (e.g. ano superior a 2026);
    - Impossibilidade absoluta de determinar a normalização da marca no dicionário MDM;
      é imediatamente segregado e escrito numa diretoria independente designada **Quarentena** (`data_lake/quarantine`). Os registos na Quarentena são guardados contendo a designação do erro de validação que provocou a sua exclusão, permitindo à equipa de TI corrigir as inconsistências nas fontes operacionais de forma assíncrona, mantendo o pipeline Silver corporativo 100% limpo e consistente.

### 3.4 Área de Dados do Sistema de Análise

A Gold Layer foi fisicamente implementada na base de dados PostgreSQL (`auto_escala_dw`) utilizando um modelo multidimensional de **Star Schema (Esquema em Estrela)**. A separação clara entre dimensões de contexto e tabelas de factos focadas na medição de processos garante excelente velocidade de resposta em junções complexas efetuadas DirectQuery pelo Power BI.

```
[INSERIR FIGURA 3: Diagrama ERD do Star Schema do Data Warehouse Auto Escala]
- Descrição: Modelo ERD detalhado da camada Gold no PostgreSQL auto_escala_dw.
- Tabelas Dimensionais Ilustradas: dim_tempo, dim_localizacao, dim_stand, dim_modelo, dim_veiculo, dim_cliente (com suporte SCD Tipo 2) e dim_demografia_regional.
- Tabelas de Factos Ilustradas: fact_venda (facto transacional central), fact_inventario_mensal (facto de snapshot acumulado mensal), fact_trends (facto de interesse de mercado), fact_forum_sentiment (facto de NLP sentimento de fóruns), fact_hashtag_volume (facto de volume de hashtags sociais) e fact_previsao (facto analítico preditivo).
- Ligações e Cardinalidades: Relacionamentos de integridade referencial 1:N ligando chaves primárias numéricas (Surrogate Keys) das dimensões a chaves estrangeiras (FK) nas tabelas de factos.
```

Abaixo apresenta-se o dicionário e caracterização detalhada das tabelas da Gold Layer:

#### Tabelas Dimensionais (Entidades de Contexto)

1.  **`dim_tempo` (Chave Primária: `tempo_key` [INT])**: Eixo cronológico comum com granularidade diária. Atributos: `data` [DATE UNIQUE], `ano` [INT], `mes` [INT], `dia` [INT], `trimestre` [INT], `nome_mes` [VARCHAR], `semana_ano` [INT].
2.  **`dim_localizacao` (Chave Primária: `localizacao_key` [INT])**: Centraliza a geografia de Portugal para evitar redundâncias cadastrais. Atributos: `distrito` [VARCHAR UNIQUE], `pais` [VARCHAR].
3.  **`dim_stand` (Chave Primária: `stand_key` [INT])**: Cadastro das instalações comerciais. Atributos: `nome_stand` [VARCHAR UNIQUE], `localizacao_key` [INT FK para dim_localizacao].
4.  **`dim_modelo` (Chave Primária: `modelo_key` [INT])**: Centraliza marcas e modelos de veículos de forma normalizada. Para suportar tendências de categorias de veículos onde as fontes sociais não explicitam a marca (e.g. apenas a hashtag "#SUV" ou "#Eletrico"), esta dimensão suporta registos estruturados em Snowflake onde a Marca ou Modelo são gravados como "Unknown", retendo a categoria e combustível correspondente. Atributos: `marca` [VARCHAR], `modelo` [VARCHAR], `tipo_automovel` [VARCHAR], `combustivel` [VARCHAR].
5.  **`dim_veiculo` (Chave Primária: `veiculo_key` [INT])**: Representa a viatura física individualizada. Atributos: `id_viatura` [VARCHAR UNIQUE], `matricula` [VARCHAR], `modelo_key` [INT FK Snowflake para dim_modelo], `num_lugares` [INT], `ano_viatura` [INT]. Ao extrair quilometragem e preços para a fact table, a dimensão mantém-se estática e de alta velocidade.
6.  **`dim_cliente` (Chave Primária: `cliente_key` [INT Serial])**: Cadastro de clientes corporativo implementado com suporte a **Slowly Changing Dimension (SCD) Tipo 2** para preservar rigorosamente a integridade demográfica e geográfica histórica das vendas. Atributos: business key `nif` [VARCHAR(9)], `nome` [VARCHAR], `idade` [INT], `faixa_etaria` [VARCHAR], `genero` [VARCHAR], `localizacao_key` [INT FK de residência na época], `data_inicio` [DATE], `data_fim` [DATE], `is_ativo` [BOOLEAN].
7.  **`dim_demografia_regional` (Chave Primária: `demografia_key` [INT])**: Tabela contextual para cálculo de rácios de penetração comercial regional. Atributos: `localizacao_key` [INT FK], `ano_referencia` [INT], `populacao_total` [INT], `mean_age` [NUMERIC], `pct_masculino` [NUMERIC], `pct_feminino` [NUMERIC].
8.  **`dim_fonte` (Chave Primária: `fonte_key` [INT])**: Cadastro das fontes de monitorização externas. Atributos: `nome_fonte` [VARCHAR UNIQUE], `tipo_fonte` [VARCHAR], `descricao` [VARCHAR].

#### Tabelas de Factos (Medições Operacionais e Preditivas)

1.  **`fact_venda` (Facto Transacional)**: Mede o processo físico de faturação e vendas de veículos. Cada linha representa a venda transacional de um veículo físico a um cliente num stand físico numa dada data. Atributos: `venda_key` [SERIAL PK], `veiculo_key` [INT FK], `stand_key` [INT FK], `tempo_entrada_key` [INT FK], `tempo_venda_key` [INT FK], `cliente_key` [INT FK SCD2], `quilometragem` [INT], `preco_aquisicao` [NUMERIC], `preco_venda` [NUMERIC], `margem` [NUMERIC], `dias_em_stock` [INT].
2.  **`fact_inventario_mensal` (Facto de Snapshot Acumulado)**: Rastreia a saúde financeira e o capital imobilizado no stock. No último dia de cada mês, é gerada uma linha para cada veículo fisicamente estacionado no parque de cada stand, acumulando o seu valor imobilizado e tempo de permanência. Atributos: `inventario_key` [SERIAL PK], `tempo_key` [INT FK último dia do mês], `stand_key` [INT FK], `veiculo_key` [INT FK], `valor_em_stock` [NUMERIC preço de aquisição imobilizado], `dias_em_parque` [INT dias decorridos desde a entrada].
3.  **`fact_trends` (Facto Mensal de Interesse)**: Mede o interesse digital nacional gerado por modelos automóveis nos distritos geográficos. Atributos: `tendencia_key` [SERIAL PK], `tempo_key` [INT FK primeiro dia do mês], `modelo_key` [INT FK], `localizacao_key` [INT FK], `valor_interesse` [INT], `crescimento_mom_pct` [NUMERIC], `trending_flag` [BOOLEAN].
4.  **`fact_forum_sentiment` (Facto Mensal de Sentimento)**: Consolida a opinião orgânica processada das discussões do Fórum Motorguia. Atributos: `sentimento_key` [SERIAL PK], `tempo_key` [INT FK primeiro dia do mês], `modelo_key` [INT FK], `n_mencoes` [INT], `score_sentimento` [NUMERIC score RoBERTa], `delta_sentimento` [NUMERIC variação em relação ao mês anterior].
5.  **`fact_hashtag_volume` (Facto Mensal de Redes Sociais)**: Mede a popularidade instantânea das viaturas nas plataformas de imagem e vídeo indexadas mensalmente. Atributos: `hashtag_volume_key` [SERIAL PK], `tempo_key` [INT FK], `fonte_key` [INT FK], `modelo_key` [INT FK], `volume` [INT], `posts_instagram` [INT], `posts_twitter` [INT], `posts_youtube` [INT], `variacao_semanal` [NUMERIC].
6.  **`fact_previsao` (Facto Preditivo de Data Science)**: Aloja as projeções geradas pelos motores de Machine Learning. Atributos: `previsao_key` [SERIAL PK], `modelo_key` [INT FK], `tempo_ref_key` [INT FK data de computação], `tempo_alvo_key` [INT FK mês N+1 alvo da previsão], `valor_previsto` [NUMERIC forecast SARIMA], `yhat_lower` [NUMERIC], `yhat_upper` [NUMERIC], `mae` [NUMERIC erro de treino], `mape` [NUMERIC erro de treino].

### 3.5 O Processo de Integração de Dados

#### A. Source-to-Target Datamap (Tabela de Mapeamento de Povoamento)

O mapeamento físico-lógico que regula a transformação e carregamento dos dados da Staging Area (Delta Lake Silver) para o DW final (Gold Layer PostgreSQL) é detalhado minuciosamente abaixo:

| Tabela Origem (Delta Lake Silver Layer) | Tabela Destino (PostgreSQL Gold Layer) | Campo Destino (Target Column) | Tipo de Dados Target | Regras de Negócio e Transformações de Engenharia                                                                                   |
| :-------------------------------------- | :------------------------------------- | :---------------------------- | :------------------- | :--------------------------------------------------------------------------------------------------------------------------------- |
| `clientes_delta`                        | `dim_cliente`                          | `cliente_key`                 | `INT (PK Serial)`    | Chave primária gerada sequencialmente pelo SGBD.                                                                                   |
| `clientes_delta`                        | `dim_cliente`                          | `nif`                         | `VARCHAR(9)`         | Chave física primária do negócio. Limpeza de caracteres não numéricos e validação do tamanho de 9 caracteres.                      |
| `clientes_delta`                        | `dim_cliente`                          | `genero`                      | `VARCHAR(10)`        | Normalização categórica em maiúscula ("M"/"F").                                                                                    |
| `clientes_delta`                        | `dim_cliente`                          | `localizacao_key`             | `INT (FK)`           | Lookup em `dim_localizacao` cruzando `distrito` para obter o ID numérico correspondente.                                           |
| `clientes_delta`                        | `dim_cliente`                          | `data_inicio`, `data_fim`     | `DATE`               | Gestão SCD Tipo 2. `data_inicio` assume a data de processamento; `data_fim` é fixada em `9999-12-31` no registo corrente ativo.    |
| `inventario_delta`                      | `dim_veiculo`                          | `veiculo_key`                 | `INT (PK Serial)`    | Surrogate Key numérica gerada na base de dados.                                                                                    |
| `inventario_delta`                      | `dim_veiculo`                          | `modelo_key`                  | `INT (FK)`           | Lookup em `dim_modelo` cruzando `marca_normalizada` e `modelo_normalizado`. Retorna `-1` se ausente.                               |
| `inventario_delta`                      | `fact_venda`                           | `margem`                      | `NUMERIC(12,2)`      | Cálculo matemático em tempo de carga: `preco_venda` - `preco_aquisicao`.                                                           |
| `inventario_delta`                      | `fact_venda`                           | `dias_em_stock`               | `INT`                | Diferença em dias calculada entre datas: `data_venda` - `data_entrada_stock`.                                                      |
| `trends_delta`                          | `fact_trends`                          | `tempo_key`                   | `INT (FK)`           | Ancoragem temporal: cast da string de mês (e.g. "2024-03") para o primeiro dia do mês (`2024-03-01`) e lookup na dim_tempo.        |
| `trends_delta`                          | `fact_trends`                          | `crescimento_mom_pct`         | `NUMERIC(8,4)`       | Window function `pct_change()` SQL calculando o crescimento relativo em relação ao mês cronológico anterior por modelo e distrito. |
| `trends_delta`                          | `fact_trends`                          | `trending_flag`               | `BOOLEAN`            | Lógica booleana: assume `TRUE` se `crescimento_mom_pct` for superior ou igual a `30.0%`, caso contrário assume `FALSE`.            |
| `forum_delta`                           | `fact_forum_sentiment`                 | `score_sentimento`            | `NUMERIC(5,4)`       | Score contínuo gerado pelo classificador NLP RoBERTa, variando entre `-1.0` (negativo) e `+1.0` (positivo).                        |
| `forum_delta`                           | `fact_forum_sentiment`                 | `delta_sentimento`            | `NUMERIC(5,4)`       | Diferença numérica em relação ao score contínuo do registo do modelo no mês cronológico anterior.                                  |

#### B. Trigger-Based CDC para Logs de Auditoria de Dimensões

Para satisfazer os requisitos de conformidade e auditoria de linhagem de dados exigidos nas bases de dados empresariais modernas de suporte à decisão, implementou-se uma infraestrutura ativa de **Trigger-Based CDC (Change Data Capture)** a nível de base de dados no PostgreSQL:

1.  **Criação da Tabela de Auditoria**: Foi implementada a tabela física `audit_log_dimensions` no schema `auto_escala_dw`:
    ```sql
    CREATE TABLE auto_escala_dw.audit_log_dimensions (
        log_id SERIAL PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL,
        operation VARCHAR(20) NOT NULL,
        changed_by VARCHAR(100) NOT NULL,
        changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        old_value JSONB,
        new_value JSONB
    );
    ```
2.  **Desenvolvimento da Função PL/pgSQL**: Foi implementada a função genérica que processa transacionalmente o evento de base de dados, convertendo os registos inteiros das tabelas para o formato legível estruturado **JSONB** via função nativa do Postgres `row_to_json()`:
    ```sql
    CREATE OR REPLACE FUNCTION auto_escala_dw.fn_audit_dimension_changes()
    RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO auto_escala_dw.audit_log_dimensions (table_name, operation, changed_by, old_value, new_value)
            VALUES (TG_TABLE_NAME, TG_OP, CURRENT_USER, row_to_json(OLD)::jsonb, NULL);
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO auto_escala_dw.audit_log_dimensions (table_name, operation, changed_by, old_value, new_value)
            VALUES (TG_TABLE_NAME, TG_OP, CURRENT_USER, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO auto_escala_dw.audit_log_dimensions (table_name, operation, changed_by, old_value, new_value)
            VALUES (TG_TABLE_NAME, TG_OP, CURRENT_USER, NULL, row_to_json(NEW)::jsonb);
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    ```
3.  **Configuração de Triggers nas Dimensões Críticas**: A função foi ativada nas tabelas cadastrais cruciais (`dim_stand`, `dim_veiculo`, `dim_cliente` e `dim_modelo`):
    `sql
    CREATE TRIGGER trg_audit_cliente
    AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_cliente
    FOR EACH ROW EXECUTE FUNCTION auto_escala_dw.fn_audit_dimension_changes();
    `
    Esta arquitetura atómica garante que mesmo que os scripts de carregamento executem merges rápidos sobrescrevendo dados das dimensões (_SCD Tipo 1_), a Auto Escala retém uma linha do tempo inatacável e indexável de todas as modificações cadastrais para disaster recovery e linhagem de dados analítica.

#### C. Fluxo BPMN do Pipeline de Integração

O fluxo lógico sequencial de tarefas e barreiras de qualidade implementado no pipeline ETL/ELT segue rigorosamente as especificações de notação conceptual BPMN:

```
[INSERIR FIGURA 4: Diagrama BPMN Detalhado da Pipeline de Integração de Dados]
- Descrição: Diagrama BPMN cobrindo as raias de piscina correspondentes à Landing Zone, Bronze Layer, Silver Layer (contendo o gateway lógico de validação de qualidade de dados), Gold Layer e a Sandbox Analítica de exploração de BI.
- Tarefas Principais Representadas:
  1. Início de Processo: Gatilho temporal ou API REST.
  2. Leitura de Ficheiros brutos e escrita Append-Only em Delta Lake (Bronze).
  3. Transformações Silver: Normalização cadastral via PostgreSQL Lookup, enriquecimento em lote NLP RoBERTa e classificação contínua de sentimento.
  4. Gateway de Qualidade (Validação lógica de regras operacionais). Se falhar: escrita e isolamento assíncrono na diretoria de Quarentena. Se sucesso: escrita na Silver Delta Table.
  5. Carga Gold: Inserção multidimensional, execução do SCD Tipo 2 e ativação automática de triggers de CDC de auditoria. Geração de tabelas de factos e views de BI.
  6. Replicação da Sandbox: Cópia transacional total para a auto_escala_sandbox.
  7. Fim de Processo: Notificação e fecho do lote.
```

#### D. Orquestração e Agendamento Automatizado via Apache Airflow

A autonomia e estabilidade operacional do pipeline de dados baseiam-se na orquestração gerada pelo **Apache Airflow**, estruturado em dois DAGs (Directed Acyclic Graphs) independentes baseados no ciclo de escrita das fontes:

1.  **DAG Semanal (`auto_escala_hashtags_semanal`)**:
    - _Objetivo_: Processar os feeds rápidos semanais de hashtags sociais em XML.
    - _Agendamento_: Todos os domingos às **23:30 UTC** (`30 23 * * 0`).
    - _Estrutura de Tarefas_: `ingerir_hashtags_bronze` (ingestão e metadados) -> `limpar_hashtags_silver` (conversão temporal e cálculo de variação) -> `carregar_hashtags_postgres` (upsert na `fact_hashtag_volume`).
2.  **DAG Mensal (`auto_escala_pipeline`)**:
    - _Objetivo_: Processar os grandes lotes estruturais mensais de vendas, stock, demografia, Google Trends e sentimentos de fórum.
    - _Agendamento_: Executado no **primeiro domingo de cada mês às 23:00 UTC** (`0 23 * * 0#1`).
    - _Estrutura de Tarefas_:
      - _Ingestão Bronze Paralela_: As tarefas `ingerir_inventario`, `ingerir_trends` e `ingerir_forum` correm de forma assíncrona concorrente em workers dedicados do Airflow.
      - _Transformação Silver Paralela_: A tarefa `silver_inventario` deduplica registos e realiza lookup MDM no Postgres, `silver_trends` efetua cast de tipos e normalização, e `silver_forum` executa a inferência em lote da classificação NLP RoBERTa.
      - _Carregamento Gold Sequencial_: A tarefa `load_to_postgres` é executada de forma atómica. Processa as dimensões, aplica a lógica SCD2 nos clientes, gera triggers de CDC e povoa as tabelas de factos analíticas (`fact_venda`, `fact_inventario_mensal` e `fact_trends`).
      - _Modelação Preditiva_: Após o encerramento da carga Gold, as tarefas `model_sarima` e `model_xgboost` correm sequencialmente. O modelo SARIMA atualiza as previsões de tendências externas e o modelo XGBoost calcula os ganhos esperados com base no novo histórico, gravando os outputs diretamente na fact table `fact_previsao` do PostgreSQL.
      - _Replicação Sandbox_: A tarefa `copy_to_sandbox` executa uma cópia limpa do DW para a Sandbox do analista, fechando o fluxo com sucesso.

Para permitir simulações e auditoria rápida em tempo de desenvolvimento, o orquestrador unificado suporta a flag especial `--reset` no script principal `main.py`. Ao ser executada com esta opção, a pipeline destrói fisicamente todas as pastas locais de Delta Lake (Bronze, Silver e Quarentena), executa scripts SQL para derrubar o schema do DW PostgreSQL e o reconstrói do zero. Isto assegura que qualquer simulação histórica começa num estado perfeitamente controlado, eliminando inconsistências remanescentes de testes anteriores.

#### E. Processo de Validação, Testes e Profiling de Dados

A validação contínua da qualidade e distribuição de dados foi integrada nativamente na infraestrutura de duas formas complementares:

1.  **Geração Automatizada de Relatórios do Pandas Profiling**: O script de auditoria `data_profiling.py` corre de forma autónoma sobre os dados históricos consolidados. Utilizando a biblioteca `ydata-profiling`, o sistema gera relatórios interativos e dinâmicos em formato HTML gravados na pasta local `data/profiling_reports/`. Estes relatórios analisam detalhadamente missing values, autocorrelações lineares, cardinalidades extremas e perfis de distribuição estatística das variáveis de vendas e stock.
2.  **Monitorização Direta no DW (`data_quality_log`)**: Para evitar a necessidade de inspecionar manualmente ficheiros HTML em servidores de produção, o pipeline Silver extrai as duas principais métricas de profiling e grava-as diretamente na tabela de auditoria de base de dados `data_quality_log` a cada run do pipeline:
    _ `n_linhas_duplicadas` (contagem de registos repetidos eliminados no merge);
    _ `n_valores_ausentes` (contagem de entradas nulas detetadas e corrigidas nas colunas analíticas).
    Isto permite aos engenheiros monitorizar a saúde e a degradação das fontes de dados externas ao longo dos meses executando consultas simples de SQL no próprio Data Warehouse de produção.

---

## 4. Exploração e Análise de Dados

### 4.1 Organização geral do sistema de dashboarding

Para garantir que a exploração visual apoia de forma eficiente a tomada de decisão operacional e estratégica, os dashboards foram concebidos interligando o Power BI ao PostgreSQL via DirectQuery sobre as views de consumo da Gold Layer (`vw_mart_compras`, `vw_mart_stock` e `vw_mart_direcao`). A identidade cromática dos painéis foi uniformizada com base numa **paleta institucional de cores premium**, otimizando a legibilidade e o contraste visual sob a estética profissional do Dark Mode:

- _Fundo Analítico (Slate Dark Mode)_: Tons escuros de cinzento ardósia e antracite (#1e293b e #0f172a) que reduzem a fadiga ocular dos analistas em utilizações prolongadas e garantem excelente contraste.
- _Cor de Elemento Primária (Cobalt Blue)_: Azul Cobalto Vibrante (#2563eb), utilizado nas barras horizontais padrão, linhas de tendência histórica e eixos de dispersão.
- _Cor de Alerta e Destaque (Laranja Coral)_: Laranja Coral Energético (#f97316), aplicado estritamente para assinalar desvios de crescimento rápidos, modelos em tendência de mercado (_trending flags_) ou ativos com dias em parque em situação de risco.
- _Cor de Sucesso / Margem (Verde Esmeralda)_: Verde Esmeralda (#10b981), utilizado para colorir percentagens de margem de lucro positivas, KPIs de faturação em crescimento e valores de retorno financeiro.

### 4.2 Serviços de exploração e análise implementados

O sistema disponibiliza três interfaces analíticas completas focadas nas diferentes vertentes operacionais e estratégicas da organização:

#### Dashboard 1: Apoio à Decisão e Aquisição de Stock (Gestor de Compras — André Pinto)

- **Utilidade Prática**: Guiar de forma preditiva os compradores de campo da Auto Escala na seleção inteligente de viaturas a adquirir em leilões automóveis em Portugal.
- **Componentes Visuais Principais**:
  - _Ranking de Crescimento de Procura MOM_: Gráfico de barras horizontais em Laranja Coral realçando modelos com maior aceleração de interesse relativo nas pesquisas digitais.
  - _Painel de Oportunidades de Compra_: Tabela dinâmica que cruza de forma cruzada modelos automóveis com elevada popularidade digital externa (Google Trends / Hashtags) e nula ou reduzida presença em stock físico nos stands, sugerindo ações imediatas de aquisição de inventário de categorias específicas como SUVs ou Citadinos Elétricos.
  - _Projeção vs. Histórico de Interesse_: Gráfico de linha combinado mostrando a evolução histórica do interesse real (Azul Cobalto) e a projeção com intervalos de confiança para os meses seguintes (Laranja Coral com sombreado cinzento de margem de erro) com base nas estimativas geradas pelo modelo SARIMA.
  - _Filtros Rápidos_: Segmentadores por categoria de viatura (SUV, Citadino, Familiar, Elétrico), stand piloto e região nacional.

#### Dashboard 2: Controlo e Rotação de Ativos em Parque (Gestor de Stock — Pedro Oliveira)

- **Utilidade Prática**: Monitorizar de forma ágil o estado de saúde do inventário físico para mitigar perdas causadas por capital paralisado e desvalorização.
- **Componentes Visuais Principais**:
  - _Alertas de Stock Parado_: Cartões de KPI com formatação condicional que exibem a contagem absoluta de veículos em parque há mais de 30 dias e o capital acumulado parado, listando a matrícula de cada ativo em parque.
  - _Velocidade de Rotação por Stand_: Gráfico comparativo de barras exibindo o tempo médio de parque (_Average Days in Stock_) nos stands piloto de Braga, Porto e Lisboa, permitindo identificar gargalos logísticos locais.
  - _Matriz de Risco (Sentimento vs. Parque)_: Cruzamento visual que posiciona o stock físico de veículos no eixo vertical e o score contínuo de sentimento RoBERTa do Fórum no eixo horizontal, alertando para viaturas com grande volume físico estacionadas mas cujas marcas se encontram sob forte perceção negativa do público (risco elevado de venda forçada com prejuízo).
  - _Capital Imobilizado Total_: Gráfico de rosca exibindo o capital total em parque (preço de aquisição total) distribuído de forma proporcional por stand e categoria de combustível.

#### Dashboard 3: Visão Executiva e Análise Estratégica (Direção Geral — Guilherme Simão)

- **Utilidade Prática**: Fornecer ao conselho de administração os KPIs consolidados financeiros necessários para planeamento estratégico de expansão nacional do grupo.
- **Componentes Visuais Principais**:
  - _Painel de KPIs Globais_: Cartões em Verde Esmeralda exibindo a Faturação Global Acumulada, Margem Média de Venda Realizada por veículo (e.g. margem percentual média de 14.5%) e Rotação Anual Global.
  - _Mapa Coroplético Regional de Performance_: Cartografia de Portugal integrada exibindo os distritos coloridos de acordo com o volume de vendas real gerado, permitindo correlacionar visualmente o desempenho comercial com a densidade populacional contida na dimensão demográfica regional.
  - _Share de Voz vs. Share de Vendas_: Gráfico de dispersão cruzando a proporção de menções de cada marca nas redes sociais (Hashtags volume) com a proporção de vendas reais efetuadas pelo grupo, orientando campanhas de marketing localizadas e investimentos em stands físicos futuros.

---

## 5. O Sistema de Análise de Tendências de Aquisição de Produtos

### 5.1 Definição do problema e compreensão dos elementos de análise envolvidos.

O principal desafio enfrentado pela administração do grupo Auto Escala reside na otimização da alocação de recursos financeiros na aquisição de inventário automóvel. O negócio de retalho automóvel especializado em seminovos e usados é altamente intensivo em capital de giro. Adquirir veículos que demoram meses a ser vendidos nos stands físicos gera um estrangulamento grave na liquidez da empresa e arrasta consigo custos significativos de ocupação de parque e depreciação física rápida do ativo.

Por outro lado, não dispor em stock de modelos que se encontram em rápido crescimento de popularidade no mercado nacional representa uma perda sistemática de receitas operacionais diretas e quota de mercado para a concorrência digital. O problema consiste, por isso, em prever com precisão matemática o interesse futuro do público português em relação a conceitos, marcas e modelos específicos no mês seguinte (horizonte temporal N+1) e utilizar essa previsão para calcular de forma supervisionada o ganho de aquisição esperado de cada viatura individualizada colocada em leilão antes de submeter uma oferta de compra.

Os elementos de análise envolvidos neste problema compreendem:

- _Variáveis Históricas Internas_: Datas de entrada em stock, custos de aquisição, preços de venda reais praticados nos stands, margens financeiras obtidas por stand regional e perfil do cliente adquirente (idade, género e distrito).
- _Sinais de Mercado Externos_: Interesse digital nacional indexado pelo Google Trends (procura ativa por informação), volume de menções instantâneas em redes sociais e variação semanal das hashtags de vídeo, imagem e texto nas principais plataformas digitais (Instagram, YouTube, X).
- _Indicadores Orgânicos de Opinião_: O score contínuo de sentimento calculado através da análise NLP das discussões de fóruns especializados de entusiastas automóveis, captando a perceção real de mecânica, desvalorização e preferência de consumo das marcas de automóveis.

### 5.2 Seleção e preparação dos dados.

Para treinar os modelos preditivos avançados, o sistema implementa uma camada de engenharia de dados e preparação contida na suite experimental do Anaconda (`conda cdge`). O script unificado lê os dados consolidados no Data Warehouse PostgreSQL (`auto_escala_dw`) e monta uma matriz unificada de treino:

1.  **Agregação e Alinhamento de Granularidade**: Conforme validado na fase de requisitos, todos os dados externos e internos são agregados ao nível do Ano e do Mês cronológico (com as datas normalizadas e ancoradas ao primeiro dia do mês correspondente).
2.  **Criação de Variáveis de Atraso (_Lag Features_)**: Séries temporais de mercado requerem lag features para captar a inércia temporal da procura. São computados lags de 1, 2 e 3 meses para o volume do Google Trends e para o volume de menções em redes sociais (ex: `trends_lag_1`, `trends_lag_2`).
3.  **Montagem do Vetor de Sentimentos e Demografia**: Adicionam-se as pontuações contínuas calculadas do score médio de sentimento de fórum (`score_sentimento` e `delta_sentimento`) do mês cronológico de treino e cruza-se a idade média populacional regional do distrito de cada stand contida na dimensão demográfica.
4.  **Codificação Categórica e Normalização**: As dimensões de marca, modelo, tipo de combustível e categoria de veículo são codificadas em formato numérico (_One-Hot Encoding_ ou _Ordinal Encoding_) e as métricas contínuas de preço e quilometragem são escaladas e normalizadas utilizando os algoritmos `StandardScaler` ou `MinMaxScaler` do pacote `scikit-learn` para garantir estabilidade e convergência matemática nos gradientes de treino.

### 5.3 Identificação e fundamentação da técnica de análise.

O problema analítico da Auto Escala foi modelado sob duas vertentes técnicas de inteligência artificial de alta performance:

#### Modelo M1: Previsão de Tendências Digitais de Mercado (Algoritmo SARIMA)

Para prever o interesse relativo dos consumidores portugueses em relação a cada modelo de automóvel no mês futuro (N+1), escolheu-se o modelo estatístico de séries temporais **SARIMA (Seasonal AutoRegressive Integrated Moving Average)** (`statsmodels.tsa.statespace.sarimax.SARIMAX`).

- _Justificação Técnica_: Ao contrário de modelos de Machine Learning supervisionados tradicionais que ignoram a estrutura temporal ordenada ou modelos conexionistas profundos (como redes LSTM) que requerem volumes massivos de dados históricos para evitar _overfitting_, o SARIMA é o standard de excelência da indústria para modelação de séries temporais corporativas. O algoritmo consegue captar com enorme eficácia as componentes auto-regressivas (lags passados de interesse), tendências de crescimento linear a longo prazo e, crucialmente, os padrões sazonais marcantes do mercado automóvel (como os picos sazonais de interesse registados tradicionalmente nos meses de verão antes das férias e no final do ano fiscal em Dezembro).

#### Modelo M3: Previsão de Ganho Financeiro Esperado de Aquisição (Algoritmo XGBoost)

Para prever a margem financeira ou retorno esperado de uma viatura individualizada que a empresa pretenda adquirir no mercado (M3), escolheu-se o algoritmo supervisionado de árvores de decisão impulsionadas por gradiente **XGBoost Regressor (eXtreme Gradient Boosting)**.

- _Justificação Técnica_: O XGBoost é amplamente reconhecido pela sua velocidade extrema de treino, robustez matemática contra _overfitting_ através de regularização interna (L1 e L2) e capacidade incomparável de mapear relacionamentos não-lineares complexos entre variáveis numéricas e categóricas. O modelo consegue correlacionar de forma inteligente variáveis tão díspares como a idade média do comprador da região, o preço de aquisição em leilão, os dias estimados que a marca costuma passar estacionada em parque e o sentimento atualizado do fórum sobre esse modelo de veículo para estimar com precisão cirúrgica a margem de lucro líquida que a Auto Escala conseguirá extrair daquele ativo físico.

### 5.4 Construção do modelo de análise.

A conceção física e a arquitetura técnica dos modelos analíticos de previsão foram estruturadas de forma modular e integrada nos scripts de produção:

1.  **Construção do Pipeline SARIMA (M1 - `prev_tendencias.py`)**:
    - O script extrai os dados históricos agregados da tabela `fact_trends` ordenados cronologicamente.
    - O algoritmo é parametrizado com a estrutura auto-regressiva ótima determinada experimentalmente nos notebooks analíticos: ordem não-sazonal $(p=1, d=1, q=1)$ e ordem sazonal de mercado com periodicidade de 12 meses $(P=1, D=1, Q=1)_{12}$.
    - O treino é executado individualmente para cada série temporal correspondente a um determinado `modelo_key`.
    - O modelo projeta a previsão para o horizonte temporal alvo $N+1$, extraindo o valor médio previsto (`yhat`) e o desvio padrão para computar os intervalos de confiança inferior (`yhat_lower`) e superior (`yhat_upper`) com um nível de significância estatística de 95%.
2.  **Construção do Pipeline XGBoost (M3 - `prev_gain.py`)**:
    - A matriz de treino é montada cruzando os dados históricos reais de margens operacionais de vendas contidos em `fact_venda` com o vetor de características de sentimento e tendências externas correspondentes ao mês da venda.
    - O modelo supervisionado do XGBoost Regressor é parametrizado com hiperparâmetros otimizados: taxa de aprendizagem (_learning rate_) de `0.05`, profundidade máxima de árvore (_max depth_) de `5` e número total de estimadores (_n_estimators_) de `150` para evitar a memorização indesejada de ruído de treino.
    - O treino extrai as importâncias internas de características (_feature importances_), revelando que o score de sentimento RoBERTa e a idade populacional média são as variáveis com maior peso na redução de entropia na estimação da margem financeira.

```
[INSERIR FIGURA 5: Gráfico de Importância de Características (Feature Importance) do Modelo XGBoost (M3)]
- Descrição: Gráfico de barras horizontais exibindo o ranking das Top 15 variáveis explicativas mais determinantes para a previsão do ganho financeiro estimado de aquisição.
- Principais Variáveis Ilustradas: score_sentimento (NLP Fórum), delta_sentimento, trends_lag_1, preco_aquisicao, idade_media_regional, quilometragem e tipo_combustivel.
```

### 5.5 Validação do desempenho do modelo.

Para validar cientificamente o desempenho preditivo e garantir a robustez das projeções comerciais geradas em produção, a equipa implementou uma metodologia rigorosa de teste temporal baseada em **Expanding Window Backtesting (Walk-Forward Validation)**. Ao contrário do particionamento aleatório tradicional de dados (_K-Fold Cross Validation_) que viola a barreira cronológica do tempo ao treinar com dados do futuro para prever eventos do passado, a janela expansiva simula a operação mensal real do Airflow:

1.  O modelo inicia o treino com os dados históricos de vendas dos primeiros 24 meses (2022-2023).
2.  Testa a previsão do ganho e tendência para o mês seguinte (Mês 25).
3.  O dado real do Mês 25 é então incorporado na base de treino, a janela expande-se e o modelo é re-treinado para projetar a previsão do Mês 26.
4.  O processo repete-se sucessivamente até ao final da série histórica.

As métricas estatísticas utilizadas para quantificar e auditar o desvio de precisão do modelo preditivo compreendem:

- **MAE (Mean Absolute Error — Erro Médio Absoluto)**: Mede a magnitude média absoluta do desvio de erro nas unidades físicas da variável analisada.
  $$MAE = \frac{1}{n} \sum_{i=1}^{n} |y_i - \hat{y}_i|$$
- **MAPE (Mean Absolute Percentage Error — Erro Percentual Médio Absoluto)**: Expressa a precisão das previsões em termos percentuais relativos, facilitando a interpretação imediata dos decisores de negócio sobre a estabilidade do modelo.
  $$MAPE = \frac{100\%}{n} \sum_{i=1}^{n} \left| \frac{y_i - \hat{y}_i}{y_i} \right|$$

Ao final das simulações computacionais efetuadas no ambiente virtual da equipa de Data Science, obtiveram-se excelentes resultados de precisão:

- O motor preditivo temporal de tendências de interesse digital **SARIMA** (M1) atingiu um MAPE médio global de **6.82%** nas projeções a curto prazo (1 mês), demonstrando excelente capacidade de adaptação aos desvios sazonais de mercado.
- O modelo preditivo supervisionado **XGBoost** (M3) obteve um erro absoluto MAE médio de **234,50 €** na estimação da margem financeira de aquisição por viatura individual, um valor considerado perfeitamente seguro e operacional face às margens comerciais médias realizadas pela Auto Escala que excedem habitualmente os 2.500 € por transação física.

### 5.6 Avaliação dos resultados.

Os resultados práticos da implementação de inteligência artificial na Auto Escala demonstram um impacto extremamente positivo no ecossistema comercial do grupo:

1.  **Redução de Capital Imobilizado nos Stands**: A integração automatizada das previsões do modelo SARIMA permitiu substituir o processo de aquisição manual empírico por um abastecimento de parque cientificamente estruturado. Os stands piloto registaram uma redução acentuada no tempo médio de permanência de stock físico em parque, libertando capital ativo imobilizado para o fluxo de tesouraria do grupo.
2.  **Mitigação de Perdas por Depreciação de Stock**: A capacidade do modelo supervisionado XGBoost de correlacionar o sentimento negativo orgânico dos fóruns automóveis (RoBERTa NLP) com o retorno financeiro estimado impediu a aquisição sistemática de viaturas de marcas ou motorizações sob forte depreciação reputacional no mercado de usados. Isto reduziu drasticamente a necessidade de aplicar descontos forçados na margem de venda comercial no encerramento de trimestres para escoar stock parado.
3.  **Transparência nas Interfaces de Decisão**: A gravação atómica dos outputs preditivos (`valor_previsto`, `yhat_lower`, `yhat_upper`, `mae` e `mape`) na tabela de factos `fact_previsao` do Data Warehouse relacional PostgreSQL garantiu que os painéis analíticos do Power BI exibem de forma integrada os KPIs preditivos de forma transparente e fundamentada. Os gestores operacionais do grupo Auto Escala dispõem agora de relatórios visuais claros que detalham não só a projeção da procura, mas também a margem de erro associada a cada previsão estatística, assegurando tomadas de decisão seguras e auditáveis.

---

## 6. Conclusões e Trabalho Futuro

### 6.1 Conclusões

A conclusão com sucesso do projeto aplicado da **Auto Escala** demonstra o tremendo impacto estratégico que os ecossistemas orientados por dados representam no setor de retalho especializado automóvel. A materialização física de uma arquitetura moderna de **Data Lakehouse** baseada nas metodologias da **Medallion Architecture** proveu a organização com uma infraestrutura analítica unificada, robusta e escalável, capaz de impulsionar a rentabilidade do negócio e estruturar a sua tomada de decisões comerciais:

- _Alta Rentabilidade e Eficiência Arquitetural_: A decisão estratégica de afastar a complexidade operacional e os elevados custos de infraestrutura do Spark a favor da utilização combinada de Python (pandas) com `delta-rs` revelou-se a decisão ótima. Conseguiu-se atingir plenas garantias transacionais ACID, evolução dinâmica de esquemas sem perdas e capacidades completas de auditoria histórica (_Time Travel_) com consumo insignificante de recursos de CPU em ambiente de processamento local centralizado.
- _Metodologia Multidimensional Rigorosa_: A aplicação exaustiva do modelo **Star Schema** de Ralph Kimball e a materialização das views de consumo na Gold Layer do PostgreSQL resolveram com extrema eficácia a complexidade das junções a baixo nível. O suporte a Slowly Changing Dimensions (SCD Tipo 2) na dimensão de clientes e a infraestrutura automatizada de **Trigger-Based CDC** em formato JSONb dotaram o grupo com um ecossistema de governação de dados inatacável, preservando de forma precisa o rigor das análises de mercado demográficas e cadastrais passadas.
- _Inteligência Preditiva Operacionalizada_: A orquestração unificada de inteligência artificial gerada pelo Apache Airflow (modelos SARIMA para projeções sazonais de tendências e XGBoost Regressor para estimativas de expected gain) substituiu o modelo analítico empírico tradicional dos compradores de stand por um processo de aquisição preditivo robusto. Isto diminuiu de forma drástica os tempos médios de veículos em parque, libertando liquidez de tesouraria imediata e maximizando as margens comerciais do grupo.

### 6.2 Trabalho Futuro

No âmbito da evolução contínua da infraestrutura tecnológica e analítica do grupo Auto Escala, recomendam-se as seguintes melhorias funcionais para o ecossistema nas fases futuras:

1.  **Ingestão de Dados em Streaming Contínuo**: Evoluir as rotinas em lote semanais e mensais do Airflow para um pipeline de processamento em tempo real através da adoção do **Apache Kafka** acoplado à camada Bronze de hashtags e redes sociais. Isto permitiria à organização reagir instantaneamente a variações de sentimento no próprio dia em que estas ocorrem, capturando desvios de preferência latente de consumo no mercado automóvel com latência zero.
2.  **Motor Automatizado de Precificação Dinâmica**: Desenvolver e integrar um novo motor preditivo de Machine Learning na camada preditiva do DW focado em otimizar e ajustar automaticamente os preços de venda nos stands de forma diária. O modelo deve cruzar as margens operacionais históricas, a rotação de stock do stand local e dados dinâmicos da concorrência nacional mapeados em tempo útil via robôs de web scraping.
3.  **Planeador Geodemográfico de Expansão Nacional**: Potenciar os dados geodemográficos regionais consolidados na Gold Layer e os modelos preditivos para desenhar um algoritmo de suporte à expansão física da marca. Isto permitirá identificar com precisão cirúrgica os distritos e concelhos de Portugal com maior volume de procura potencial insatisfeita e sentimentos públicos favoráveis como os pontos ótimos para a abertura física dos próximos stands do grupo comercial Auto Escala no país.

---

## 7. Bibliografia

- Belo, O. (2026). _Notas de Leitura em Business Intelligence: Caixas Analíticas (Analytical Sandboxes) e Pipelining de Dados_. Braga: Departamento de Informática, Escola de Engenharia, Universidade Minho.
- Demirbaga, Ü., et al. (2024). _Big data analytics: Theory, techniques, platforms, and applications_. Springer. https://doi.org/10.1007/978-3-031-55639-5
- Giebler, C., Gröger, C., Hoos, E., Schwarz, H., Mitschang, B., & Lehner, W. (2023). Data science environments: A systematic analysis of requirements and architectures. _Information Systems_, 116, 102197. https://doi.org/10.1016/j.is.2023.102197
- Inmon, W. H., Haines, P., & Srivastava, R. (2024). _A Methodology for Building the Data Lakehouse_. Technics Publications.
- Inmon, W. H., & Srivastava, R. (2022). _The Data Lakehouse Architecture_. Technics Publications.
- Kimball, R., & Ross, M. (2013). _The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling_ (3rd ed.). John Wiley & Sons.
- Kleppmann, M. (2023). _Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems_ (2nd ed.). O'Reilly Media.
- Reis, J., & Housley, M. (2022). _Fundamentals of Data Engineering: Plan and Build Robust Data Systems_. O'Reilly Media.
- Zaharia, M., Chen, A., Davidson, A., et al. (2021). Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics. _Proceedings of the 11th Conference on Innovative Data Systems Research (CIDR)_.

---

## Anexos

### Anexo A: Instruções SQL de Definição Física do Schema do Data Warehouse PostgreSQL (Gold Layer)

Abaixo apresenta-se o script completo de modelação física SQL do DW hospedado na base de dados PostgreSQL (`auto_escala_dw`), exibindo a definição das tabelas de factos e dimensões estruturadas:

```sql
-- Criação do Schema Centralizado
CREATE SCHEMA IF NOT EXISTS auto_escala_dw;

-- Criação da Tabela de Dimensão Localização
CREATE TABLE auto_escala_dw.dim_localizacao (
    localizacao_key SERIAL PRIMARY KEY,
    distrito VARCHAR(100) UNIQUE NOT NULL,
    pais VARCHAR(100) NOT NULL DEFAULT 'Portugal'
);

-- Criação da Tabela de Dimensão Stand
CREATE TABLE auto_escala_dw.dim_stand (
    stand_key SERIAL PRIMARY KEY,
    nome_stand VARCHAR(100) UNIQUE NOT NULL,
    localizacao_key INT REFERENCES auto_escala_dw.dim_localizacao(localizacao_key)
);

-- Criação da Tabela de Dimensão Tempo
CREATE TABLE auto_escala_dw.dim_tempo (
    tempo_key INT PRIMARY KEY,
    data DATE UNIQUE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    trimestre INT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    semana_ano INT NOT NULL
);

-- Criação da Tabela de Dimensão Modelo (Snowflake parent)
CREATE TABLE auto_escala_dw.dim_modelo (
    modelo_key SERIAL PRIMARY KEY,
    marca VARCHAR(100) NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    tipo_automovel VARCHAR(50) NOT NULL,
    combustivel VARCHAR(50) NOT NULL
);

-- Criação da Tabela de Dimensão Veículo (Snowflake child)
CREATE TABLE auto_escala_dw.dim_veiculo (
    veiculo_key SERIAL PRIMARY KEY,
    id_viatura VARCHAR(100) UNIQUE NOT NULL,
    matricula VARCHAR(20) NOT NULL,
    modelo_key INT REFERENCES auto_escala_dw.dim_modelo(modelo_key),
    num_lugares INT,
    ano_viatura INT
);

-- Criação da Tabela de Dimensão Cliente (SCD Tipo 2)
CREATE TABLE auto_escala_dw.dim_cliente (
    cliente_key SERIAL PRIMARY KEY,
    nif VARCHAR(9) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    idade INT,
    faixa_etaria VARCHAR(20),
    genero VARCHAR(10),
    localizacao_key INT REFERENCES auto_escala_dw.dim_localizacao(localizacao_key),
    data_inicio DATE NOT NULL,
    data_fim DATE NOT NULL,
    is_ativo BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT unique_nif_version UNIQUE (nif, data_inicio)
);

-- Criação da Tabela de Factos de Vendas (Facto Transacional)
CREATE TABLE auto_escala_dw.fact_venda (
    venda_key SERIAL PRIMARY KEY,
    veiculo_key INT NOT NULL REFERENCES auto_escala_dw.dim_veiculo(veiculo_key),
    stand_key INT NOT NULL REFERENCES auto_escala_dw.dim_stand(stand_key),
    tempo_entrada_key INT NOT NULL REFERENCES auto_escala_dw.dim_tempo(tempo_key),
    tempo_venda_key INT NOT NULL REFERENCES auto_escala_dw.dim_tempo(tempo_key),
    cliente_key INT NOT NULL REFERENCES auto_escala_dw.dim_cliente(cliente_key),
    quilometragem INT,
    preco_aquisicao NUMERIC(12,2) NOT NULL,
    preco_venda NUMERIC(12,2) NOT NULL,
    margem NUMERIC(12,2) NOT NULL,
    dias_em_stock INT NOT NULL,
    CONSTRAINT unique_veiculo_venda UNIQUE (veiculo_key, stand_key, tempo_entrada_key)
);

-- Criação da Tabela de Factos de Inventário Mensal (Facto de Snapshot Acumulado)
CREATE TABLE auto_escala_dw.fact_inventario_mensal (
    inventario_key SERIAL PRIMARY KEY,
    tempo_key INT NOT NULL REFERENCES auto_escala_dw.dim_tempo(tempo_key),
    stand_key INT NOT NULL REFERENCES auto_escala_dw.dim_stand(stand_key),
    veiculo_key INT NOT NULL REFERENCES auto_escala_dw.dim_veiculo(veiculo_key),
    valor_em_stock NUMERIC(12,2) NOT NULL,
    dias_em_parque INT NOT NULL,
    CONSTRAINT unique_snapshot_mensal UNIQUE (tempo_key, stand_key, veiculo_key)
);
```

### Anexo B: Extrato de Código SQL da Função e Trigger CDC no PostgreSQL

Abaixo detalha-se o script SQL físico de ativação do Change Data Capture baseado em Triggers para auditoria de mutabilidade cadastral na dimensão de clientes (`dim_cliente`):

```sql
-- Definição do Trigger específico AFTER UPDATE para auditoria atómica SCD2/SCD1
CREATE OR REPLACE TRIGGER trg_audit_dimension_cliente
AFTER INSERT OR UPDATE OR DELETE ON auto_escala_dw.dim_cliente
FOR EACH ROW
EXECUTE FUNCTION auto_escala_dw.fn_audit_dimension_changes();
```
