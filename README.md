# dashboard_sei

Base para automacao Selenium (SEI) + dashboard Streamlit.

## Setup rapido

1. Criar ambiente virtual e instalar dependencias:
```bash
pip install -r requirements.txt
```
2. Copiar `.env.example` para `.env` e preencher as credenciais.

## Execucao backend

```bash
python backend/main.py
```

Flags uteis:
- `--debug`: forca log em DEBUG.
- `--manual-login`: espera login manual.
- `--auto-login`: tenta login por credencial.
- `--max-internos N`
- `--max-processos N`

Exemplo:
```bash
python backend/main.py --debug --manual-login --max-internos 2 --max-processos 3
```

## Execucao dashboard

```bash
streamlit run dashboard_streamlit.py
```

## VS Code debug

Use a configuracao `Backend SEI (Debug)` em `.vscode/launch.json`.
Para parar em excecoes, no painel Run and Debug habilite `Raised Exceptions` e `Uncaught Exceptions`.
