# 🤖 Robô de Faturamento PBH/UNIMED
> Automação completa do processo de faturamento mensal entre PBH e UNIMED — extração de PDFs, casamento de notas fiscais e geração de relatório executivo de auditoria.

![Python](https://img.shields.io/badge/Python-3.10+-8b5cf6?style=flat-square&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-8b5cf6?style=flat-square&logo=pandas&logoColor=white)
![pdfplumber](https://img.shields.io/badge/pdfplumber-0.11-8b5cf6?style=flat-square&logoColor=white)
![OpenPyXL](https://img.shields.io/badge/OpenPyXL-3.x-8b5cf6?style=flat-square&logoColor=white)
![Status](https://img.shields.io/badge/Status-Em%20Produção-22c55e?style=flat-square)
![Versão](https://img.shields.io/badge/Versão-r5.1.3-8b5cf620?style=flat-square)

---

## 📌 Sobre o Projeto

Sistema de automação de faturamento desenvolvido para processar o ciclo mensal PBH/UNIMED — um dos processos mais complexos do setor de saúde pública.

O robô lê PDFs de notas fiscais, extrai os valores por componente (Subsídio, MCF, Créditos, MNC), casa cada NF com o layout de faturamento por contrato e valor, detecta divergências e gera um relatório executivo completo de auditoria por entidade.

**Problema resolvido:** o processo manual consumia dias de trabalho de analistas, com alto risco de erro em casamentos manuais entre centenas de NFs e contratos. O robô executa tudo em minutos com rastreabilidade completa.

---

## 🚀 Funcionalidades

- ✅ **Extração automática de PDFs** — lê NFs e captura CNPJ, valores brutos, ISS, IRRF e número do título
- ✅ **Motor de casamento inteligente** — casa NFs com layout por contrato + tipo + valor (tolerância configurável)
- ✅ **Fallback de múltiplos níveis** — se o match principal falha, tenta por contrato+valor antes de declarar divergência
- ✅ **Auditoria por NF e por Contrato** — relatório separado por entidade (HOB, SAMU, URBEL, etc.)
- ✅ **Relatório Excel executivo** — cabeçalho em negrito, bordas, formatação R$, auto-ajuste de colunas
- ✅ **Cópia segura de abas** — recriação de estilos via openpyxl sem `TypeError: unhashable type`
- ✅ **Log completo no terminal** — rastreabilidade de cada decisão do algoritmo

---

## 🛠️ Stack

| Tecnologia | Uso |
|---|---|
| Python 3.10+ | Lógica principal e automação |
| Pandas + NumPy | Manipulação e análise de dados |
| pdfplumber | Extração de texto e valores de PDFs |
| OpenPyXL | Leitura e escrita de Excel com estilos |
| xlwings | Cópia de abas com fórmulas preservadas |
| Decimal | Precisão financeira nas comparações |

---

## 📁 Estrutura

```
robo-faturamento-unimed/
├── robo_faturamento_UNIMED.py   # Script principal (r5.1.3)
├── requirements.txt
├── .gitignore                   # Ignora pastas de dados e PDFs
└── README.md
```

**Pastas de dados esperadas (não versionadas):**
```
Automacao_Faturamento/
├── 01.2026/    # Mês anterior — layout + PDFs
└── 02.2026/    # Mês atual — layout + PDFs
```

---

## ⚙️ Como Executar

```bash
pip install -r requirements.txt
python robo_faturamento_UNIMED.py
```

Configure os caminhos no topo do script:
```python
PASTA_MES_ANTERIOR = r"caminho\para\mes_anterior"
PASTA_MES_ATUAL    = r"caminho\para\mes_atual"
```

---

## 📈 Exemplo de Output no Terminal

```
[UNIMED] Lendo layout de faturamento...
[UNIMED] 247 NFs encontradas nos PDFs
[MATCH] HOB — 89/91 NFs casadas (97.8%)
[MATCH] SAMU — 43/43 NFs casadas (100%)
[WARN] URBEL — 2 NFs sem match → fallback por contrato+valor
[OK] Auditoria exportada: Auditoria_02.2026.xlsx
[OK] Concluído em 4m 32s
```

---

## 👤 Autor

**Gustavo** — Dev & Founder · Inside.co

[![LinkedIn](https://img.shields.io/badge/LinkedIn-8b5cf6?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/gustavo-henriquesp/)
[![Portfolio](https://img.shields.io/badge/Portfolio-8b5cf6?style=flat-square&logo=netlify&logoColor=white)](https://seusite.netlify.app)
[![Email](https://img.shields.io/badge/Email-8b5cf6?style=flat-square&logo=gmail&logoColor=white)](mailto:ghspdm@gmail.com)

---
> *"Construo soluções que outros apenas descrevem em planilhas."*
