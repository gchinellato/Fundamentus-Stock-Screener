#!/usr/bin/env python3

from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from bs4 import BeautifulSoup
import json
import urllib.request
import pandas as pd

# Filter
MARKET_CAP = 200000000.00
VOLUME = 200000.00

# http://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/acoes/consultas/classificacao-setorial/
FINANCIAL_STOCKS = [
					'ABCB','RPAD','BRIV','BAZA','BPAN','BGIP','BEES','BOAC','BPAR','BRSR','BBDC','BBAS','BSLI','BPAC','CTGP','GSGI','IDVL','BIDI','ITSA','ITUB','JPMC','BMEB','BMIN','BNBR','PRBC','BPAT','PINE','SANB','UBSG','USBC','WFCO', #bancos
					'CRIV','FNCN','MERC', #Soc. Crédito e Financiamento
					'BDLS','BVLS','DBEN', #Soc. Arrendamento Mercantil
					'APCS','BZRS','BSCS','BRCS','WTVR','CBSC','ECOA','GAFL','GAIA','OCTS','PDGS','PLSC','RBRA','RBCS','VERT','WTPI', #Securitizadoras de Recebíveis
					'BLAK','BNDP','BONY','BFRE','GPIV','IDNT','PPLA','TRPN', #Gestão de Recursos e Investimentos
					'AXPB','B3SA','CIEL','MSCD','MSBR','SCHW','VISA','WUNI', #Serviços Financeiros Diversos
					'AIGB','BRGE','BBSE','IRBR','METB','PSSA','CSAB','SULA','TRVC', #Seguradoras
					'APER','WIZS' #Corretoras de Seguros
					]

def get_screening():
	url = 'http://www.fundamentus.com.br/resultado.php'
	headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
		'Accepts': 'text/html,application/xhtml+xml,application/xml',
	}

	try:
		req = urllib.request.Request(url, headers=headers)
		url = urllib.request.urlopen(req)
		response = url.read().decode('ISO-8859-1')
		soup = BeautifulSoup(response, 'html.parser')
		table = soup.find('table', id="resultado")
		df = pd.read_html(str(table), decimal=',', thousands='.')[0]

		cols = ['Div.Yield', 'Mrg Ebit', 'Mrg. Líq.', 'ROIC', 'ROE', 'Cresc. Rec.5a']

		for col in cols:
			df[col] = df[col].str.replace('%', '').str.replace('.','').str.replace(',','.').astype(float) / 100.0

		# Filter: Volume
		return df[(df['Liq.2meses'] >= VOLUME) & (df['Mrg Ebit'] > 0.0)]
	except (ConnectionError, Timeout, TooManyRedirects) as e:
		print(e)
		return None

def get_stock_info(ticker):
	url = 'http://www.fundamentus.com.br/detalhes.php?papel=' + str(ticker)
	headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
		'Accepts': 'text/html,application/xhtml+xml,application/xml',
	}
	
	try: 
		req = urllib.request.Request(url, headers=headers)
		url = urllib.request.urlopen(req)
		response = url.read().decode('ISO-8859-1')
		soup = BeautifulSoup(response, 'html.parser')
		tables = soup.findAll('table', class_="w728")

		# Informacoes
		'''
			0 			1 				2 				3
		0 	?Papel 		PETR4 			?Cotação 		30.54
		1 	?Tipo 		PN 				?Data últ cot 	18/12/2019
		2 	?Empresa 	PETROBRAS PN 	?Min 52 sem 	20.56
		3 	?Setor 		Petróleo 		?Max 52 sem 	30.97
		4 	?Subsetor 	Exploração 		?Vol $ méd (2m) 1638030000
		'''
		df0 = pd.read_html(str(tables), decimal=',', thousands='.')[0]
		df_temp = df0.iloc[:,2:]
		df_temp.columns = [0, 1]
		df0 = df0.append(df_temp, ignore_index=True).drop([2, 3], 1).fillna(0)
		cols = df0.iloc[:,0]
		df0 = df0.pivot(columns=0).stack().groupby(level=0, sort=False).first().transpose()
		df0.columns = cols.str.replace('?','')

		# Valor de mercado
		'''
			0 					1 				2 							3
		0 	?Valor de mercado 	398379000000 	?Últ balanço processado 	30/09/2019
		1 	?Valor da firma 	712451000000 	?Nro. Ações 				13044500000
		'''
		df1 = pd.read_html(str(tables), decimal=',', thousands='.')[1]
		df_temp = df1.iloc[:,2:]
		df_temp.columns = [0, 1]
		df1 = df1.append(df_temp, ignore_index=True).drop([2, 3], 1).fillna(0)
		cols = df1.iloc[:,0]
		df1 = df1.pivot(columns=0).stack().groupby(level=0, sort=False).first().transpose()
		df1.columns = cols.str.replace('?','')

		# Oscilacoes e Indicadores fundamentalistas
		#df2 = pd.read_html(str(tables), decimal=',', thousands='.')[2]
		# Dados Balanço Patrimonial
		#df3 = pd.read_html(str(tables), decimal=',', thousands='.')[3]
		# Dados demonstrativos de resultados
		#df4 = pd.read_html(str(tables), decimal=',', thousands='.')[4]
						
		return df0.join(df1)
	except (ConnectionError, Timeout, TooManyRedirects) as e:
		print(e)
		return None

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

if __name__ == '__main__':
	df = get_screening()
	l = df.shape[0]

	stocks_info = pd.DataFrame()
	printProgressBar(0, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
	for index, row in enumerate(df.iterrows()):
		printProgressBar(index + 1, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
		stocks_info = stocks_info.append(get_stock_info(row[1]['Papel']), ignore_index=True)

	out = pd.merge(df, stocks_info, how ='outer', on ='Papel') 

	# Filter: Market Cap
	out = out[out['Valor de mercado'] >= MARKET_CAP]

	out = out[['Papel', 'Empresa', 'Setor', 'Subsetor', 'Tipo', 'Cotação_x',
	   'P/L', 'P/VP', 'Div.Yield', 'EV/EBIT', 'EV/EBITDA',
       'Mrg Ebit', 'Mrg. Líq.', 'ROIC', 'ROE', 'Liq.2meses',
       'Dív.Brut/ Patrim.', 'Valor de mercado',
       'Data últ cot', 'Últ balanço processado']]

	# Removing Financial Stocks based on B3 categorization
	out = out[~out['Papel'].str.contains('|'.join(FINANCIAL_STOCKS))]	

	# Keep stock with higher volume, in case of duplicated ticker (i.e.: PETR4, PETR3)
	out['pre_ticker'] = out['Papel'].str[:4]
	out.sort_values(by=['pre_ticker', 'Liq.2meses'], ascending=[True, False], inplace=True)
	out.drop_duplicates(subset=['pre_ticker'], keep='first', inplace=True)
	out.drop('pre_ticker', axis=1, inplace=True)

	# Acquirer's Multiple Ranking
	out.sort_values(by=['EV/EBIT'], ascending=[True], inplace=True)
	out.reset_index(inplace=True)

	out.to_json('fundamentus.json', orient='index')
