from vnstock import Vnstock
import pandas as pd

stock = Vnstock(source="VCI")
stock.listing.symbols_by_group("HOSE")
