import pandas as pd
import requests
from lxml import html
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
from datetime import datetime


def get_income_stmt(ticker):
    try:
        print(f"{ticker}: Fetching income statement......")
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/?p=quarterly"
        )
        dt = pd.read_html(page.text)
        df = dt[0]
        print(f"{ticker}: Succesfully get income statement")
        return df
    except Exception as e:
        print(f"Error: Get income statement failed:\n {e}")


def get_balance_sheet(ticker):
    try:
        print(f"{ticker}: Fetching balance sheet......")
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/balance-sheet/?p=quarterly"
        )
        dt = pd.read_html(page.text)
        pattern = r"^Q[1-4] \d{4}$"
        df = dt[0]
        print(f"{ticker}: Succesfully get balance sheet")
        return df
    except Exception as e:
        print(f"Error: Get balance sheet failed:\n {e}")


def get_ratios(ticker):
    try:
        print(f"{ticker}: Fetching ratios page")
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/ratios/?p=quarterly"
        )
        dt = pd.read_html(page.text)
        pattern = r"^Q[1-4] \d{4}$"
        df = dt[0]
        print(f"{ticker}: Successfully get ratios page")
        return df
    except Exception as e:
        print(f"Error: Get balance sheet failed:\n {e}")


def date_format(date_string):
    pattern = r"(?:\b[A-Za-z]{3} '\d{2}\b )?(?P<full_date>[A-Za-z]{3} \d{2}, \d{4})"  # Define regex for get date like 'Jun 30, 2020'
    match = re.search(pattern, date_string)
    if match:
        full_date = match.group("full_date")
        date_obj = datetime.strptime(full_date, "%b %d, %Y")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        return formatted_date
    else:
        return pd.NA


def get_price_at_time(ticker, time):

    # Parse the end date into a datetime object
    end_date_obj = datetime.strptime(time, "%Y-%m-%d")

    # Calculate the start date (first day of the same month)
    start_date_obj = end_date_obj.replace(day=1)

    # Format the start and end dates as strings
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = end_date_obj.strftime("%Y-%m-%d")
    data = yf.download(ticker, start=start_date, end=end_date)
    if not data.empty:
        last_date = data.index[-1]  # Get the last date from the index
    return data.loc[last_date, "Close"][0]


def get_data_cell(source, row_name, column_name):
    try:
        cell = [
            source.loc[
                source[("Fiscal Quarter", "Period Ending")] == row_name, column_name
            ].values[0]
        ]
        return cell
    except Exception as e:
        print(f"Error: Cannot get row {row_name}\n {e}")


def get_company_ratio(ticker):
    try:
        print(f"System is getting company: {ticker}")
        income_stmt = get_income_stmt(ticker)
        balance_sheet = get_balance_sheet(ticker)
        ratio = get_ratios(ticker)
        pattern = r"^Q[1-4] \d{4}$"
        matching_columns = [
            col
            for col in income_stmt.columns
            if pd.Series(col).str.match(pattern, na=False).any()
        ]
        for time in matching_columns:
            date = date_format(time[1])
            eps = get_data_cell(income_stmt, "EPS (Basic)", time)
            total_assets = get_data_cell(balance_sheet, "Total Assets", time)
            total_debt = get_data_cell(balance_sheet, "Total Liabilities", time)
            bvps = get_data_cell(balance_sheet, "Book Value Per Share", time)
            roa = get_data_cell(ratio, "Return on Assets (ROA)", time)
            roe = get_data_cell(ratio, "Return on Equity (ROE)", time)
            dy = get_data_cell(ratio, "Dividend Yield", time)
            div = get_data_cell(income_stmt, "Dividend Per Share", time)
            market_cap = get_data_cell(ratio, "Market Capitalization", time)
            price = get_price_at_time(f"{ticker}.SS", date)
            data = pd.DataFrame(
                {
                    "Company": ticker,
                    "Date": date,
                    "Total Assets": total_assets,
                    "Total Debt": total_debt,
                    "EPS": eps,
                    "BVPS": bvps,
                    "ROA": roa,
                    "ROE": roe,
                    "DY": dy,
                    "Price": price,
                    "DIV": div,
                    "Market Cap": market_cap,
                }
            )
            print(data)
            data.to_csv("shanghai.csv", mode="a", header=False, index=False)
        print(f"Successfully Write {ticker} to CSV")
    except Exception as e:
        print(f"Indicator error: {e}")


if __name__ == "__main__":
    for comp_code in range(900000, 900999):
        get_company_ratio(comp_code)
