from vnstock3 import Vnstock
from vnstock3.explorer.vci import Finance
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

isFetch = False


def fetch_indicator(comp_code, start_from):
    try:
        if comp_code == start_from:
            global isFetch
            isFetch = True
        if isFetch:
            stock = Vnstock().stock(symbol=comp_code, source="VCI")
            company = Vnstock().stock(symbol=comp_code, source="TCBS").company
            icm_stm = stock.finance.income_statement(period="year", lang="en")
            balanced_sheet = stock.finance.balance_sheet(period="year")
            finance = stock.finance.ratio(period="year", lang="en")
            years = finance["Meta", "yearReport"]
            for year in years:
                if year > 2019:
                    EPS = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu định giá", "EPS (VND)"),
                    ].values[0]
                    BVPS = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu định giá", "BVPS (VND)"),
                    ].values[0]
                    ROA = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu khả năng sinh lợi", "ROA (%)"),
                    ].values[0]
                    ROE = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu khả năng sinh lợi", "ROE (%)"),
                    ].values[0]
                    DY = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu khả năng sinh lợi", "Dividend yield (%)"),
                    ].values[0]
                    PE = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu định giá", "P/E"),
                    ].values[0]
                    Market_Cap = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu định giá", "Market Capital (Bn. VND)"),
                    ].values[0]
                    Total_Asset = balanced_sheet.loc[
                        balanced_sheet["yearReport"] == year, "TOTAL ASSETS (Bn. VND)"
                    ].values[0]
                    PB = finance.loc[
                        finance["Meta", "yearReport"] == year,
                        ("Chỉ tiêu định giá", "P/B"),
                    ].values[0]
                    Total_Debt = balanced_sheet.loc[
                        balanced_sheet["yearReport"] == year, "LIABILITIES (Bn. VND)"
                    ].values[0]
                    DAR = Total_Debt / Total_Asset

                    year_end_prices = (
                        stock.quote.history(
                            start=f"{year}-01-01", end=f"{year}-12-31", interval="1M"
                        )
                        .tail(1)["close"]
                        .values[0]
                    )

                    comp_name = company.overview()["short_name"].values[0]
                    industry = company.overview()["industry"].values[0]

                    print(f"\nYear: {year}")
                    print(f"Company Name: {comp_name}")
                    print(f"Industry: {industry}")
                    print(f"EPS: {EPS}")
                    print(f"BVPS: {BVPS}")
                    print(f"ROA: {ROA}")
                    print(f"ROE: {ROE}")
                    print(f"P/E Ratio {PE}")
                    print(f"Closed price: {year_end_prices}")
                    print(f"DAR: {DAR}")
                    print(f"MB: {PB}")
                    print(f"Market Cap: {Market_Cap}")
                    print(f"DY: {DY}")

                    fin_data = {
                        "Company code": comp_code,
                        "Company Name": comp_name,
                        "Industry": industry,
                        "Year": year,
                        "EPS": f"{EPS}",
                        "BVPS": f"{BVPS}",
                        "ROA": f"{ROA}",
                        "ROE": f"{ROE}",
                        "P/E Ratio": f"{PE}",
                        "DAR": f"{DAR}",
                        "MB": f"{PB}",
                        "DY": f"{DY}",
                        "Market Cap": f"{Market_Cap}",
                        "Total Assets": f"{Total_Asset}",
                        "Stock Price": f"{year_end_prices}",
                    }
                    write_to_csv(fin_data)
    except Exception as e:
        print(f"Idicator error: {e}")


isWriteHeader = False


def write_to_csv(fin_data):
    output_file = f"vietnam_data_{fin_data["Year"]}.csv"
    with open(output_file, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fin_data.keys())
        if isWriteHeader:
            writer.writeheader()  # Write the header row (column names)
        writer.writerow(fin_data)

    print(f"Data for {fin_data["Year"]} successfully written to {output_file}")


if __name__ == "__main__":
    isFetch = True
    stock = Vnstock().stock(source="VCI")
    stock_list = stock.listing.symbols_by_group("UPCOM")
    for stock in stock_list.values:
        fetch_indicator(stock, "SHA")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_indicator, stock)
