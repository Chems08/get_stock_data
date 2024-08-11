import pandas as pd
import finnhub
import yfinance as yf
import datetime
import time 
from pathlib import Path
import asyncio
import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError
import random
import requests
#import requests_cache
#import json


start = time.perf_counter()

# Remplacez 'YOUR_API_KEY' par votre clé API Finnhub
api_key = 'YOUR_API_KEY 1'
api_key2 = 'YOUR_API_KEY 2'
api_key3 = 'YOUR_API_KEY 3'
api_keys = [api_key, api_key2, api_key3]

def change_api_key(api_keys, index):
    #alterner les 3 clés API pour éviter les blocages
    return api_keys[index % 3]

finnhub_client = finnhub.Client(change_api_key(api_keys, random.randint(0,2)))

# Créer un cache pour stocker les réponses des requêtes (utile dans le cas où vous exécutez le script après avoir ajouté de nouveaux symboles à votre liste)
#requests_cache.install_cache('finnhub_cache', expire_after=3600)

# Liste des symboles boursiers

async def fetch_symbols(url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=180) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return [item['symbol'] for item in data]
        except asyncio.TimeoutError:
            await asyncio.sleep(0)
            return []
        except ServerDisconnectedError:
            await asyncio.sleep(0)
            return []
        

async def retrieve_symbols(api_key, api_key2):
    NASDAQ_URL = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&mic=XNAS&token={api_key}"
    NYSE_URL = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&mic=XNYS&token={api_key2}"


    nasdaq_symbols = await fetch_symbols(NASDAQ_URL) 
    nyse_symbols = await fetch_symbols(NYSE_URL) 
    symbols = nasdaq_symbols + nyse_symbols

    file_symbol_retrieved = Path("symbols_files/symbols_retrieved_optimized.txt")

    if not file_symbol_retrieved.exists():
        with open(file_symbol_retrieved, 'w') as fichier:
            for valeur in symbols:
                fichier.write(f"{valeur}\n")
        print("Le fichier a été créée.")
    else :
        #read le fichier symbols_retrieved.txt
        with open(file_symbol_retrieved, 'r') as fichier:
            symbols_retrieved = fichier.readlines()
        symbols_retrieved = [symbol.strip() for symbol in symbols_retrieved]

        #ajoute le symbole s'il n'est pas présent dans le fichier
        tmp = []
        for symbol in symbols:
            if symbol not in symbols_retrieved:
                tmp.append(symbol)

        #écrit les symboles dans le fichier
        with open(file_symbol_retrieved, 'a') as fichier:
            for valeur in tmp:
                fichier.write(f"{valeur}\n")

        print("-"*50)
        print(f"Les valeurs ont été ajoutées dans le fichier {fichier}")
        print("-"*50,'\n')

    return symbols

def read_symbols(file_path):
    if Path(file_path).exists():
        with open(file_path, 'r') as fichier:
            return [symbol.strip() for symbol in fichier.readlines()]
    else:
        if not Path(file_path).exists():
            with open(file_path, 'w') as fichier:
                fichier.write("")
                return []

async def check_symbol_yahoo(symbol):
    try:
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(None, yf.Ticker, symbol)
        # Vérifiez si le champ 'longName' ou 'shortName' est présent dans l'objet info
        try:
            info = ticker.info
            return 'longName' in info or 'shortName' in info
        except:
            return False
    except requests.exceptions.ReadTimeout:
        await asyncio.sleep(0)
        return False


async def check_symbol2(symbol, api_keys, index):
    try:
        key = change_api_key(api_keys, index)
        url = f'https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={key}'
        async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=180) as response:
                    response.raise_for_status()
                    data = await response.json()
    
        symbol_fin = data.get('symbol', 'N/A')
        return symbol_fin
    except asyncio.TimeoutError:
        await asyncio.sleep(0)
        return 'N/A'
    except ServerDisconnectedError:
        await asyncio.sleep(0)
        return 'N/A'

async def process_symbol(symbol, symbols_checked, api_keys, index, semaphore, failed_list, added_symbols):
    async with semaphore:
        if symbol in symbols_checked:
            return symbol
        
        if index != 0 and index%60 == 0:
            print(50*"-", "\nPause de 60 secondes pour éviter le blocage de l'API...", f'\n{50*"-"}')
            time.sleep(60)

        print(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}] - {index} out of {len(added_symbols)}')

        is_present = await check_symbol_yahoo(symbol)
        symbol_fin = await check_symbol2(symbol, api_keys, index)
        # await asyncio.sleep(1)

        if not is_present or symbol != symbol_fin:
            failed_list.append(symbol)
            print(f'Symbol {symbol} is going out of the list')
            return 'Bad symbol'

        return symbol

async def checked_data(symbols, api_keys):

    file_symbol_checked = "symbols_files/symbols_checked_optimized.txt"
    file_symbol_failed = "symbols_files/symbols_failed_optimized.txt"

    symbols_checked = read_symbols(file_symbol_checked)
    symbols_failed = read_symbols(file_symbol_failed)


    # Supprime les symboles fail 
    symbols = [symbol for symbol in symbols if symbol not in symbols_failed]
    added_symbols = [symbol for symbol in symbols if symbol not in symbols_checked and symbol not in symbols_failed]

    failed_list = []
    semaphore = asyncio.Semaphore(60)  # Limite à 60 tâches en simultané

    tasks = []
    for index, symbol in enumerate(symbols, start=1):
        tasks.append(process_symbol(symbol, symbols_checked, api_keys, index, semaphore, failed_list, added_symbols))

    results = await asyncio.gather(*tasks)
    symbols = [result for result in results if result != 'Bad symbol' and result not in failed_list]

    # Mise à jour des fichiers
    with open(file_symbol_checked, 'w') as fichier:
        for valeur in symbols:
            fichier.write(f"{valeur}\n")

    # Ajouter les symboles fail
    tmp = [symbol for symbol in failed_list if symbol not in symbols_failed]
    with open(file_symbol_failed, 'a') as fichier:
        for valeur in tmp:
            fichier.write(f"{valeur}\n")

    return symbols


async def take_it(periode, stock):
    try:
        # Calcule la date il y a 52 semaines
        date_52_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=52)

        historical_data = stock.history(periode)

        historical_data = historical_data.reset_index()

        historical_data['Date'] = pd.to_datetime(historical_data['Date'], errors='coerce')

        historical_data['Date'] = historical_data['Date'].dt.date
        date_52_weeks_ago = date_52_weeks_ago.date()

        filtered_data = historical_data.loc[historical_data['Date'] <= date_52_weeks_ago]

        if not filtered_data.empty:
            price_52weeks_ago = filtered_data.iloc[-1]['Close']
        else:
            price_52weeks_ago = 'N/A'

        data_last_year = stock.history(periode)

        volume_last_year = data_last_year['Volume']
        volume_year = volume_last_year.mean()

        return price_52weeks_ago, volume_year

    except requests.exceptions.ReadTimeout:
        await asyncio.sleep(0)
        return 'N/A', 'N/A'
    

async def get_yahoo_data(symbol):
    # Fetch the historical data for the stock
    try:
        loop = asyncio.get_event_loop()
        stock = await loop.run_in_executor(None, yf.Ticker, symbol)

        if stock.history(period="1y").empty is False or 'Date' in stock.history(period="1y").columns:
            price_52weeks_ago, volume_year = await take_it("1y", stock)
        elif stock.history(period="ytd").empty is False or 'Date' in stock.history(period="ytd").columns:
            price_52weeks_ago, volume_year = await take_it("ytd", stock)
        else:
            price_52weeks_ago, volume_year =  'N/A', 'N/A' 

        
        # Récupére les données pour le mois dernier
        data_last_month = stock.history(period='1mo')

        # Extrait le volume pour le mois dernier
        volume_last_month = data_last_month['Volume']
        volume_month = volume_last_month.mean()

            
        #---------------------------------------
        
        sector = stock.info.get('sector', 'N/A')
        industry = stock.info.get('industry', 'N/A')
        
        ebitda = stock.info.get('ebitda', 'N/A')
        net_income = stock.info.get('netIncomeToCommon', 'N/A')
        revenue = stock.info.get('totalRevenue', 'N/A')
        total_assets = stock.info.get('sharesOutstanding')
        
        #---------------------------------------


        return sector, industry, price_52weeks_ago, volume_month, volume_year, ebitda, net_income, revenue, total_assets
    
    except requests.exceptions.ReadTimeout:
        await asyncio.sleep(0)
        return 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'

#---------------------------------

async def get_stock_metric(symbol, index, api_keys):
    try:
        key = change_api_key(api_keys, index)
        url = f'https://finnhub.io/api/v1//stock/metric?symbol={symbol}&metric=all&token={key}'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=180) as response:
                response.raise_for_status()
                if response.status == 200:
                    data = await response.json()
                    if data.get('metric', {}): 
                        return data.get('metric', {})
                    else:
                        return 'N/A'
    except asyncio.TimeoutError:
        await asyncio.sleep(0)
        return 'N/A'
    except ServerDisconnectedError:
        await asyncio.sleep(0)
        return 'N/A'


# Fonction pour récupérer les données de l'API Finnhub
async def get_stock_data(symbol, index, api_keys):
    try:
        key = change_api_key(api_keys, index)
        url = f'https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={key}'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=180) as response:
                response.raise_for_status()
                if response.status == 200:
                    data = await response.json()
                    if data: 
                        return data
                    else:
                        return None
    except asyncio.TimeoutError:
        await asyncio.sleep(0)
        return None
    except ServerDisconnectedError:
        await asyncio.sleep(0)
        return None

# Fonction pour récupérer le prix de l'action en temps réel
async def get_stock_price(symbol, index, api_keys):
    try:
        key = change_api_key(api_keys, index)
        url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={key}'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=180) as response:
                response.raise_for_status()  # Vérifie que la requête a réussi
                data = await response.json()  # Attend la réponse JSON
                return data
    except asyncio.TimeoutError:
        await asyncio.sleep(0)
        return None
    except ServerDisconnectedError:
        await asyncio.sleep(0)
        return None


async def get_current_price(symbol, index, api_keys):
    try:
        key = change_api_key(api_keys, index)
        url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={key}'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=180) as response:
                response.raise_for_status()
                if response.status == 200:
                    data = await response.json()
                    if data['c']: 
                        return data['c']
                    else:
                        return 'N/A'
    except asyncio.TimeoutError:
        await asyncio.sleep(0)
        return 'N/A'
    except ServerDisconnectedError:
        await asyncio.sleep(0)
        return 'N/A'
    except Exception:
        await asyncio.sleep(0)
        return 'N/A'



def write_csv(data):
    csv_file_path = "symbols_files/updated_assets.csv"

    # Création du DataFrame à partir des données nouvelles
    columns = [
        "Symbol", "Company Name", "Price", "Market Cap (in M)", "P/E Ratio", "Beta", "Volume 52 weeks", "Volume 1 month",
        "52 Weeks High", "52 Weeks Low", "Exchange", "Performance (52 weeks)", "Country", "Chiffre d'affaires",
        "Résultat net", "Sector", "Industry", "Price 52 Weeks Ago", "Currency",
        "Total assets", "EPS Annual", "Dividend Per Share Annual", "EBITDA CAGR (5y)", "EBITDA", "ROI Annual", "Ratio Debt/Equity (Annual)", 
        "Dividend Yield Indicated Annual",
    ]
    new_data_df = pd.DataFrame(data, columns=columns)

    # Lire le fichier CSV existant (s'il existe)
    try:
        existing_df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        # Si le fichier n'existe pas, créer un DataFrame vide avec les mêmes colonnes
        existing_df = pd.DataFrame(columns=columns)

    symbols_to_add = new_data_df[~new_data_df['Symbol'].isin(existing_df['Symbol'])]

    # Ajouter les nouveaux symboles au DataFrame existant
    updated_df = pd.concat([existing_df, symbols_to_add])

    # Enregistrer le DataFrame mis à jour dans le fichier CSV
    updated_df.to_csv(csv_file_path, index=False)

    print(f"Fichier CSV mis à jour : {csv_file_path}\n")



async def data_attribution(data, symbols, symbol, semaphore, index):
    async with semaphore: 
        if index%100==0:
            write_csv(data)
        
        if index != 0 and index%20 == 0:
            print(50*"-", "\nPause de 60 secondes pour éviter le blocage de l'API...", f'\n{50*"-"}')
            time.sleep(60)

        print(f"Récupération des données pour le symbole {symbol}...")
        print(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}] - {index} out of {len(symbols)}')

        #------------------------------------
        # Récupération des données

        price_info = await get_stock_price(symbol, index, api_keys)

        metric = await get_stock_metric(symbol, index, api_keys)

        sector, industry, price_52weeks_ago, volume_month, volume_year, ebitda, net_income, revenue, total_assets = await get_yahoo_data(symbol)

        # total_assets = await get_asset_finnhub(symbol, index, api_keys)

        info = await get_stock_data(symbol, index, api_keys)
        await asyncio.sleep(0)
        #------------------------------------

        #Check de la présence des données
        if price_info is not None and metric != 'N/A' and metric != {} and info is not None:

            week52_low = metric.get('52WeekLow', 'N/A')
            week52_high = metric.get('52WeekHigh', 'N/A')
            beta = metric.get('beta', 'N/A')  # Beta
            annual_eps = metric.get('epsAnnual', 'N/A')
            dividendYieldIndicatedAnnual = metric.get('dividendYieldIndicatedAnnual', 'N/A') # /// NO : "currentDividendYieldTTM" // the sum of a company's dividends paid out to shareholders over the past twelve months and is dynamically updated
            dividendPerShareAnnual = metric.get('dividendPerShareAnnual', 'N/A')
            ebitdaCagr5Y = metric.get('ebitdaCagr5Y', 'N/A') # // EBITDA CAGR (5y) measures the five-year compound annual growth rate in EBITDA.
            peAnnual = metric.get('peAnnual', 'N/A')
            roiAnnual = metric.get('roiAnnual', 'N/A')
            ratio_debt_equity = metric.get('totalDebt/totalEquityAnnual', 'N/A')

            company_name = info.get('name', 'N/A')
            price = await get_current_price(symbol, index, api_keys)
            market_cap = info.get('marketCapitalization', 'N/A')

            if info.get('exchange', 'N/A') == 'NEW YORK STOCK EXCHANGE, INC.':
                exchange = 'NYSE'
            elif info.get('exchange', 'N/A') == 'NASDAQ NMS - GLOBAL MARKET':
                exchange = 'NASDAQ'
            else:
                exchange = 'N/A'

            currency = info.get('currency', 'N/A')
            country = info.get('country', 'N/A')


            #Calcul de la performance sur 52 semaines --> Performance annuelle = (Valeur jour J/Valeur jour 0)^(365/J) – 1
            if price != 'N/A' and price_52weeks_ago != 'N/A':
                annual_performance = ((float(price)/ float(price_52weeks_ago))**(365/364) - 1)
            else:
                annual_performance = 'N/A'



            data.append([
                symbol, company_name, price, market_cap, peAnnual, beta, volume_year, volume_month,
                week52_high, week52_low, exchange, annual_performance, country, revenue, net_income,
                sector, industry, price_52weeks_ago, currency, total_assets, annual_eps, dividendPerShareAnnual, ebitdaCagr5Y, ebitda, 
                roiAnnual, ratio_debt_equity, dividendYieldIndicatedAnnual
            ])
        else:
            file_symbol_failed = "symbols_files/symbols_failed_optimized.txt"
            with open(file_symbol_failed, 'a') as fichier:
                fichier.write(f"{symbol}\n")
            #supprimer le symbole présent dans checked_symbol
            file_symbol_checked = "symbols_files/symbols_checked_optimized.txt"
            with open(file_symbol_checked, 'r') as file:
                lines = file.readlines()

            # Filtrer les lignes pour enlever celle avec la valeur à supprimer
            lines = [line for line in lines if line.strip() != symbol]

            # Ouvrir le fichier en mode écriture pour écraser le contenu avec les lignes filtrées
            with open(file_symbol_checked, 'w') as file:
                file.writelines(lines)
            



def convert_time(seconds):
    hours = seconds // 3600
    seconds_remaining = seconds % 3600
    minutes = seconds_remaining // 60
    seconds_remaining = seconds_remaining % 60

    print(f'Durée du script : {int(hours)}h {int(minutes)}m {int(seconds_remaining)}s')


async def main():
    print("Etape 1 : Retrieve symbols...\n")
    symbols = await retrieve_symbols(api_key, api_key2)
    #symbols = ["BN", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "BABA", "TM"]

    # elements_to_remove = ['FUFUW','HCOW', 'SFLO']
    # symbols = [element for element in symbols if element not in elements_to_remove]
    
    print("Etape 2 : Checking symbols...\n")
    symbols = await checked_data(symbols, api_keys)

    #Evite de récupérer les données déjà présentes dans le fichier csv
    csv_file_path = 'symbols_files/updated_assets.csv'
    df = pd.read_csv(csv_file_path)
    symbol_list = df['Symbol'].tolist()
    symbols = [symbol for symbol in symbols if symbol not in symbol_list]

    # Initialisation d'une liste pour stocker les données
    data = []

    # Récupération des données pour chaque symbole
    semaphore = asyncio.Semaphore(20)  # Limite à 20 appels API (tâches) en simultané

    tasks = []
    print("Etape 3 : Getting data from API...\n")
    for index, symbol in enumerate(symbols, start=1):
        tasks.append(data_attribution(data, symbols, symbol, semaphore, index))

    await asyncio.gather(*tasks)


    write_csv(data)


    end = time.perf_counter()
    duree = end - start

    convert_time(duree)



if __name__ == "__main__":
    asyncio.run(main())





