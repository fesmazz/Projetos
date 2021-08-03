# Analisando dados de espécies em perigo de extinção
# Este script é um experimento para importar e tratar dados armazenados em uma planilha do 
# Microsoft Excel que contém dados sobre as espécies em extinção nas principiais áreas de preservação e parques nacionais brasileiros

import numpy as np
import pandas as pd
import sqlite3 as db

src = input("Insira o nome da planilha, ou tecle Enter para usar o nome padrão: ")
if len(src) < 1:
    src = "fauna_fed.xlsx"
data = pd.read_excel(src)
print("Dados carregados como: " + str(type(data)))
print("Convertendo para base de dados SQLite...")

conn = db.connect("fauna_db") #criando conexão com DB
data.to_sql("Fauna", conn, if_exists='replace')
cur = conn.cursor()
cur.execute("SELECT * FROM Fauna")
tmp = cur.fetchone()
print(tmp[4])
conn.commit()




