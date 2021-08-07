# Calculando a probabilidade de vitória no Poker

Projeto final do curso de especialização "Introductory C Programming" oferecido pela Duke University na plataforma Coursera. Trata-se de um programa desenvolvido em C que, 
dadas duas mãos (com cartas conhecidas, desconhecidas, ou um misto), calcula a probabilidade de vitória de cada uma, considerando as regras do Texas Hold'em Poker . 
O programa lê um arquivo .txt que contém informações sobre as mãos a serem analisadas, roda um número de simulações especificado pelo usuário (padrão = 10.000) e 
retorna o número de vitórias, derrotas e empates de cada uma.

## Uso
Compile o programa a partir do makefile fornecido (por padrão, será buscado o GCC). O programa, "Poker", aceita dois argumentos:
* __input.txt__: Um arquivo txt que contém as mãos que o usuário deseja comparar. Devem ser inseridas da seguinte maneira:          
  \[Número ou Letra\]\[Naipe\], \[Número ou Letra\]\[Naipe\] .....
  Cada mão aceita até cinco cartas conhecidas e, no mínimo, duas desconhecidas. 
  
* __NumTrials__ (opcional): Um número inteiro que definirá quantas simulações serão rodadas. O número padrão de simulações é 10.000
