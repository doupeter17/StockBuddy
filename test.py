import pandas as pd
import requests
from lxml import html
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_eps_from_income_stmt(ticker):
    try:
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/?p=quarterly"
        )
        content = html.fromstring(page.content)
        table = content.xpath('//*[@id="main-table"]')
        dt = pd.read_html(page.text)
        pattern = r"^Q[1-4] \d{4}$"
        matching_columns = [
            col
            for col in dt[0].columns
            if pd.Series(col).str.match(pattern, na=False).any()
        ]
        matches = [item[0] for item in matching_columns if re.match(pattern, item[0])]
        df = dt[0]
        arr_eps = (
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "EPS (Basic)",
                time,
            ].values[0]
            for time in matching_columns
        )
        income_stm = pd.DataFrame({"DateTime": matches, "EPS": arr_eps})
        return income_stm
    except:
        print("Error: Get incm stmt")


def get_balance_sheet(ticker):
    try:
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/balance-sheet/?p=quarterly"
        )
        content = html.fromstring(page.content)
        table = content.xpath('//*[@id="main-table"]')
        dt = pd.read_html(page.text)
        pattern = r"^Q[1-4] \d{4}$"
        matching_columns = [
            col
            for col in dt[0].columns
            if pd.Series(col).str.match(pattern, na=False).any()
        ]
        matches = [item[0] for item in matching_columns if re.match(pattern, item[0])]
        # print(dt[0])
        df = dt[0]
        total_debt = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Total Liabilities",
                time,
            ].values[0]
            for time in matching_columns
        ]  # Create list for total_debt
        total_asset = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Total Assets",
                time,
            ].values[0]
            for time in matching_columns
        ]  # Create list for total_asset
        bvps = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Book Value Per Share",
                time,
            ].values[0]
            for time in matching_columns
        ]  # Create list for bvps
        # print(total_debt)
        balanced_sheet = pd.DataFrame(
            {
                "DateTime": matches,
                "Total Liability": total_debt,
                "Total Asset": total_asset,
                "BVPS": bvps,
            }
        )
        return balanced_sheet
    except:
        print("Error: Get balanced sheet")


def get_ratio(ticker):
    try:
        page = requests.get(
            f"https://stockanalysis.com/quote/sha/{ticker}/financials/ratios/?p=quarterly"
        )
        content = html.fromstring(page.content)
        table = content.xpath('//*[@id="main-table"]')
        dt = pd.read_html(page.text)
        pattern = r"^Q[1-4] \d{4}$"
        matching_columns = [
            col
            for col in dt[0].columns
            if pd.Series(col).str.match(pattern, na=False).any()
        ]
        matches = [item[0] for item in matching_columns if re.match(pattern, item[0])]

        df = dt[0]
        # print(df.columns)
        roa = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Return on Assets (ROA)",
                time,
            ].values[0]
            for time in matching_columns
        ]  # Create list for roa
        roe = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Return on Equity (ROE)",
                time,
            ].values[0]
            for time in matching_columns
        ]
        div = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Dividend Yield", time
            ].values[0]
            for time in matching_columns
        ]  # Create list for div
        size = [
            df.loc[
                df[("Fiscal Quarter", "Period Ending")] == "Market Capitalization", time
            ].values[0]
            for time in matching_columns
        ]
        ratio = pd.DataFrame(
            {
                "DateTime": matches,
                "ROA": roa,
                "ROE": roe,
                "div": div,
                "size": size,
            }
        )
        # print(ratio)
        return ratio

    except:
        print("Error: Get ratio")


def get_company_ratio(ticker):
    try:
        income_stmt = get_eps_from_income_stmt(ticker)
        balance_sheet = get_balance_sheet(ticker)
        ratio = get_ratio(ticker)
        merged_df1 = pd.merge(income_stmt, balance_sheet, on="DateTime", how="inner")
        merged_df2 = pd.merge(merged_df1, ratio, on="DateTime", how="inner")
        merged_df2.insert(0, "Company Code", ticker)
        print(income_stmt)
        print(balance_sheet)
        print(ratio)
        merged_df2.to_csv("shanghai.csv", mode="a", header=False, index=False)
        print(f"Successfully Write {ticker} to CSV")
    except Exception as e:
        print(f"Indicator error: {e}")


if __name__ == "__main__":
    for comp_code in range(600000, 604000):
        get_company_ratio(comp_code)
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_company_ratio, comp_code)
