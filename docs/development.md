# Desenvolvimento

## Pre-requisitos
- Python 3.11+
- Google Chrome

## Instalacao
```bash
pip install -r requirements.txt
```

## Configuracao
Crie `.env` com base no `.env.example`.

Campos principais:
- `SEI_URL`
- `SEI_USERNAME`
- `SEI_PASSWORD`
- `HEADLESS`
- `MANUAL_LOGIN`
- `DEBUG`
- `LOG_LEVEL`

## Rodar backend
```bash
python backend/main.py
```

Modo debug:
```bash
python backend/main.py --debug --manual-login
```

Sem login manual:
```bash
python backend/main.py --debug --auto-login
```

## Rodar dashboard
```bash
streamlit run dashboard_streamlit.py
```
